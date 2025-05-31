use std::{collections::{HashMap, HashSet}, ffi::OsStr, fs::File, io::{BufWriter, Error}, os::unix::fs::MetadataExt, time::SystemTime};
use rayon::{slice::ParallelSliceMut};

use crate::{diff::{add_diffs_to_items, get_entry_from_dir_diff, merge_dir_diff_to_entry, CDirEntryDiff, DiffFile, DiffType}, utility::collect_from_root};
use crate::{save::{add_dir_diffs, diff_saves, get_hash_iteration_count_from_file_names, read_diff_file, read_save_file}, walk::CDirEntry};

pub fn scan(target_path: std::path::PathBuf, output_path: std::path::PathBuf, min_diff_bytes: usize, num_threads: usize, thread_add_dir_limit: usize) -> Result<(usize, usize), Error> {
    let save_file_data = get_hash_iteration_count_from_file_names(&target_path, output_path.to_path_buf());
    let path_hash = save_file_data.0;
    let mut path_to_initial = output_path.clone();
    path_to_initial.push(format!("{}_initial", path_hash));

    let skip_set: HashSet<std::path::PathBuf> = HashSet::from_iter(vec![output_path.clone()]);
    let mut curr_scan;
    let mut pm: HashMap<std::path::PathBuf, usize> = HashMap::new();

    let maybe_curr_scan = collect_from_root(target_path, skip_set, num_threads, thread_add_dir_limit);
    if maybe_curr_scan.is_err() {
        return Err(std::io::Error::new(std::io::ErrorKind::Other, format!("Failed to do MT walk: {:?}", maybe_curr_scan.err())))
    }
    curr_scan = maybe_curr_scan.unwrap();
    curr_scan.par_sort_by(|a, b| {
        return a.p.cmp(&b.p);
    });

    // Populate parent map
    for ci in 0..curr_scan.len() {
        let p = &curr_scan[ci].p;
        pm.insert(p.clone(), ci);
    }

    // Traverse scan in reverse to "bubble up" properties
    bubble_up_props(&mut curr_scan, &mut pm);
    
    let iteration_count = save_file_data.1;
    let curr_is_initial_scan = iteration_count < 0;
    if curr_is_initial_scan {
        let f  = File::create(path_to_initial)?;
        let writer: BufWriter<File> = BufWriter::new(f);
        bincode::serialize_into(writer, &curr_scan).expect("failed to seralise");
    
        return Ok((curr_scan[0].files_here + curr_scan[0].files_below, curr_scan[0].dirs_here + curr_scan[0].dirs_below + 1))
    }

    // Open file
    let f = File::open(&path_to_initial)?;
    let mut f_sz= 0;
    if let Ok(md) = f.metadata() {
        f_sz = md.size();
    }
    if f_sz == 0 {
        return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, format!("Failed to get size for provided file path")))
    }

    // Get ENTIRE file, chunk-by-chunk
    // TODO: In future should fetch and process one chunk at a time instead of stitching them all together here
    let mut initial_scan: Vec<CDirEntry>;
    let maybe_last_scan = read_save_file(path_to_initial);
    match maybe_last_scan {
        Ok(entries) => {initial_scan = entries}
        Err(e) => {return Err(std::io::Error::new(std::io::ErrorKind::Other, format!("Failed to read entries from file: {}", e)))}
    }

    let mut combined_diffs: DiffFile = DiffFile { diffs: vec![], move_to_paths: HashMap::new() };
    if iteration_count > -1 {
        let mut diff_prefix = output_path.clone();
        diff_prefix.push("tmp");
        diff_prefix.set_file_name(format!("{}_diff", path_hash));
        let res: Result<DiffFile, Error> = add_combined_diffs(&diff_prefix, iteration_count as u16, None, None);
        match res {
            Ok(ds) => {
                combined_diffs = ds;
            }
            Err(e) => {return Err(std::io::Error::new(std::io::ErrorKind::Other, format!("failed to add combined diffs to scan: {}", e)))}
        }
    }

    // TODO: This is VERY dumb, there should be a faster way to do this
    let mut not_move_diffs: Vec<CDirEntryDiff> = combined_diffs.diffs.clone().into_iter().filter(|el| {
        return el.diff_type != DiffType::MoveDir
    }).collect();
    let move_diffs: Vec<CDirEntryDiff> = combined_diffs.diffs.into_iter().filter(|el| {
        return el.diff_type == DiffType::MoveDir
    }).collect();

    let res = add_diffs_to_items::<CDirEntry, CDirEntryDiff>(&mut initial_scan, &mut not_move_diffs, |a, b| {
        return a.p.cmp(&b.p);
    }, |it, d| {
        return it.p == d.p;
    }, |a| {a.diff_type == DiffType::Add}, |a| {a.diff_type == DiffType::Remove}, get_entry_from_dir_diff, merge_dir_diff_to_entry);
    if res.is_err() {
        return Err(std::io::Error::new(std::io::ErrorKind::Other, format!("failed to add diffs to scan: {:?}", res.err())))
    }

    if move_diffs.len() > 0 {
        let mut mvdi = 0;
        for i in 0..initial_scan.len() {
            if initial_scan[i].p == move_diffs[mvdi].p {
                let maybe_to_path = combined_diffs.move_to_paths.get(&initial_scan[i].p);
                if maybe_to_path.is_some() {
                    initial_scan[i].p = maybe_to_path.unwrap().to_path_buf();
                }
                mvdi += 1;

                if mvdi >= move_diffs.len() {
                    break;
                }
            }
        }
    }

    // Step above probably screwed up the order...
    initial_scan.par_sort_by(|a, b| {
        return a.p.cmp(&b.p);
    });

    let num_scan_files = curr_scan[0].files_here + curr_scan[0].files_below;
    let num_scan_dirs = curr_scan[0].dirs_here + curr_scan[0].dirs_below + 1;

    let existing_moved_paths = combined_diffs.move_to_paths.into_keys().collect();
    let diff: DiffFile = diff_saves(initial_scan, curr_scan, existing_moved_paths, min_diff_bytes);
    if diff.diffs.len() > 0 {
        let mut path_to_subsequent = output_path.clone();
        path_to_subsequent.push(format!("{}_diff_{}", path_hash, iteration_count + 1));
        let f  = File::create(path_to_subsequent)?;
        let writer = BufWriter::new(f);
        bincode::serialize_into(writer, &diff).expect("failed to seralise");
    }

    Ok((num_scan_files, num_scan_dirs))
}

pub fn add_combined_diffs(diff_path: &std::path::PathBuf, diff_count: u16, maybe_start_diff_time: Option<SystemTime>, maybe_end_diff_time: Option<SystemTime>) -> std::io::Result<DiffFile> {
    let combined_diffs = DiffFile { diffs: vec![], move_to_paths: HashMap::new() };
    if diff_count == 0 {
        return Ok(combined_diffs);
    }
    
    // Read initial diff
    let maybe_base_file_name = diff_path.file_name();
    let base_file_name: &OsStr;
    match maybe_base_file_name {
        Some(mfn) => {
            base_file_name = mfn;
        }
        None => {
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "failed to get diff base name from `diff_path`"))
        }
    }
    
    // Combine diffs
    let mut combined_diffs = DiffFile { diffs: vec![], move_to_paths: HashMap::new() };
    let is_diff_range_restricted = maybe_start_diff_time.is_some() || maybe_end_diff_time.is_some();
    for i in 1..(diff_count + 1) {
        let mut curr_diff_path = diff_path.clone();
        curr_diff_path.set_file_name(format!("{}_{}", base_file_name.to_str().unwrap(), i));
        
        if is_diff_range_restricted {
            let md = std::fs::metadata(&curr_diff_path)?;
            let modified_at = md.modified()?;
            let diff_in_time_range = (maybe_start_diff_time.is_none() || modified_at >= maybe_start_diff_time.unwrap()) && (maybe_end_diff_time.is_none() || modified_at <= maybe_end_diff_time.unwrap());
            if !diff_in_time_range {
                continue;
            }
        }
        
        let next_diff = read_diff_file(curr_diff_path)?;
        combined_diffs = add_dir_diffs(combined_diffs, next_diff);
    }

    return Ok(combined_diffs);
}

pub fn bubble_up_props(scan: &mut Vec<CDirEntry>, pm: &mut HashMap<std::path::PathBuf, usize>) {
    // Traverse scan in reverse to "bubble up" properties
    if scan.len() > 0 {
        // let mut_dt = &mut curr_scan;
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