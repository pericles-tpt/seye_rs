use std::{collections::HashMap, fs::{exists, File}, io::{BufWriter, Error}, time::SystemTime};
use rayon::{slice::ParallelSliceMut};

use crate::{diff::{add_diffs_to_items, get_entry_from_dir_diff, ignore_dir_entry, merge_dir_diff_to_entry, CDirEntryDiff, DiffEntry, DiffFile}, save::get_hash_from_root_path, utility::collect_from_root};
use crate::{save::{add_dir_diffs, diff_saves, read_diff_file, read_save_file}, walk::CDirEntry};

pub fn scan(target_path: std::path::PathBuf, output_path: std::path::PathBuf, min_diff_bytes: usize, num_threads: usize, thread_add_dir_limit: usize, cache_merged_diffs: bool) -> Result<(usize, usize), Error> {
    let root_path_hash = get_hash_from_root_path(&target_path);
    let mut path_to_initial = output_path.clone();
    path_to_initial.push(format!("{}_initial", root_path_hash));
    let mut path_to_diff = output_path.clone();
    path_to_diff.push(format!("{}_diffs", root_path_hash));
    
    let maybe_curr_scan = collect_from_root(target_path, num_threads, thread_add_dir_limit);
    if maybe_curr_scan.is_err() {
        return Err(std::io::Error::new(std::io::ErrorKind::Other, format!("Failed to do MT walk: {:?}", maybe_curr_scan.err())))
    }
    let mut curr_scan = maybe_curr_scan.unwrap();
    curr_scan.par_sort_by(|a, b| {
        return a.p.cmp(&b.p);
    });
    
    // Populate parent map
    let mut parent_map: HashMap<std::path::PathBuf, usize> = HashMap::new();
    for ci in 0..curr_scan.len() {
        let p = &curr_scan[ci].p;
        parent_map.insert(p.clone(), ci);
    }

    // Traverse scan in reverse to "bubble up" properties
    bubble_up_props(&mut curr_scan, &mut parent_map);
    
    let initial_scan_exists = exists(&path_to_initial)?;
    if !initial_scan_exists {
        let f  = File::create(path_to_initial)?;
        let writer: BufWriter<File> = BufWriter::new(f);
        bincode::serialize_into(writer, &curr_scan).expect("failed to seralise");
    
        return Ok((curr_scan[0].files_here + curr_scan[0].files_below, curr_scan[0].dirs_here + curr_scan[0].dirs_below + 1))
    }

    // Open file
    let maybe_last_scan = read_save_file(path_to_initial);
    if maybe_last_scan.is_err() {
        return Err(std::io::Error::new(std::io::ErrorKind::Other, format!("Failed to read entries from file: {:?}", maybe_last_scan.err())))
    }
    let mut initial_scan: Vec<CDirEntry> = maybe_last_scan.unwrap();

    let mut diff_file: DiffFile = DiffFile { has_merged_diff: true, timestamps: vec![], entries: vec![] };
    let mut combined_diffs: DiffEntry = DiffEntry { diffs: Default::default(), move_to_paths: HashMap::new() };
    let diff_exists = exists(&path_to_diff)?;
    if diff_exists {
        diff_file = read_diff_file(&path_to_diff)?;
        
        let res: Result<DiffEntry, Error> = add_combined_diffs(&diff_file, &initial_scan, None, None);
        if res.is_err() {
            return Err(std::io::Error::new(std::io::ErrorKind::Other, format!("failed to add combined diffs to scan: {:?}", res.err())))
        }
        combined_diffs = res.unwrap();
    }

    // Apply "moves" before `add_diffs_to_items`
    if combined_diffs.move_to_paths.len() > 0 {
        for i in 0..initial_scan.len() {
            let maybe_to_path = combined_diffs.move_to_paths.get(&initial_scan[i].p);
            if maybe_to_path.is_some() {
                initial_scan[i].p = maybe_to_path.unwrap().to_path_buf();
            }
        }
    }

    // TODO: This is VERY dumb, there should be a faster way to do this
    let res = add_diffs_to_items::<CDirEntry, CDirEntryDiff>(&mut initial_scan, &mut combined_diffs.diffs, |a, b| {
        return a.p.cmp(&b.p);
    }, |it, d| {
        return it.p == d.p;
    }, ignore_dir_entry, get_entry_from_dir_diff, merge_dir_diff_to_entry);
    if res.is_err() {
        return Err(std::io::Error::new(std::io::ErrorKind::Other, format!("failed to add diffs to scan: {:?}", res.err())))
    }

    // Step above probably screwed up the order...
    initial_scan.par_sort_by(|a, b| {
        return a.p.cmp(&b.p);
    });

    let num_scan_files = curr_scan[0].files_here + curr_scan[0].files_below;
    let num_scan_dirs = curr_scan[0].dirs_here + curr_scan[0].dirs_below + 1;

    let entries_before = diff_file.entries.len();
    diff_file = diff_saves(diff_file, initial_scan, curr_scan, combined_diffs, min_diff_bytes, cache_merged_diffs);
    let new_entry_added = diff_file.entries.len() > entries_before;
    if new_entry_added {
        let f  = File::create(path_to_diff)?;
        let writer = BufWriter::new(f);
        bincode::serialize_into(writer, &diff_file).expect("failed to seralise");
    }

    Ok((num_scan_files, num_scan_dirs))
}

pub fn add_combined_diffs(diff_file: &DiffFile, full_scan_entries: &Vec<CDirEntry>, maybe_start_diff_time: Option<SystemTime>, maybe_end_diff_time: Option<SystemTime>) -> std::io::Result<DiffEntry> {
    let mut combined_diffs = DiffEntry { diffs: Default::default(), move_to_paths: HashMap::new() };
    if diff_file.entries.len() == 0 {
        return Ok(combined_diffs);
    }
    
    if diff_file.entries.len() != diff_file.timestamps.len() {
        return Err(Error::new(std::io::ErrorKind::Other, "invalid diff file, entries.len() != timestamps.len()"));
    }

    let is_diff_range_restricted = maybe_start_diff_time.is_some() || maybe_end_diff_time.is_some();
    let mut start_diff_idx = 0;
    if diff_file.has_merged_diff {
        start_diff_idx = 1;
        if !is_diff_range_restricted {
            return Ok(diff_file.entries[0].clone());
        }
    }
    
    // Combine diffs
    let mut start_idx: i32 = -1;
    let mut end_idx: i32 = -1;
    if is_diff_range_restricted {
        for i in start_diff_idx..diff_file.timestamps.len() {
            let modified_at = diff_file.timestamps[i];
            if start_idx < 0 && modified_at >= maybe_start_diff_time.unwrap() {
                start_idx = i as i32;
            } else if end_idx < 0 && modified_at > maybe_end_diff_time.unwrap() {
                end_idx = (i - 1) as i32;
                break;
            }
        }
    }
    if start_idx < 0 {
        start_idx = start_diff_idx as i32;
    }
    if end_idx < 0 {
        end_idx = 0;
        if diff_file.entries.len() > 0 {
            end_idx = (diff_file.entries.len() - 1) as i32;
        }
    }
    combined_diffs = add_dir_diffs(&diff_file, &full_scan_entries, start_idx as usize, end_idx as usize);

    return Ok(combined_diffs);
}

pub fn bubble_up_props(scan: &mut Vec<CDirEntry>, pm: &mut HashMap<std::path::PathBuf, usize>) {
    // Traverse scan in reverse to "bubble up" properties
    if scan.len() > 0 {
        for i in 0..scan.len() {
            // Calculate memory usage for self
            let curr_idx = scan.len() - 1 - i;
            if let Some(parent) = scan[curr_idx].p.parent() {
                if let Some(maybe_ent) = &pm.get(parent) {
                    let idx = *maybe_ent;
    
                    scan[*idx].dirs_here += 1;
                    scan[*idx].dirs_below += scan[curr_idx].dirs_here + scan[curr_idx].dirs_below;
                    scan[*idx].files_below += scan[curr_idx].files_here + scan[curr_idx].files_below;
                    scan[*idx].size_below += scan[curr_idx].size_here + scan[curr_idx].size_below;
                }
            }
        }
    }
}