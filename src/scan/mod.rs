use std::{collections::HashSet, ffi::OsStr, fs::File, io::{BufWriter, Error}, os::unix::fs::MetadataExt, time::SystemTime};
use crate::diff::{add_diffs_to_items, get_entry_from_dir_diff, merge_dir_diff_to_entry, CDirEntryDiff, DiffType};
use crate::{save::{add_dir_diffs, diff_saves, get_hash_iteration_count_from_file_names, read_diff_file, read_save_file}, walk::{walk_iter, CDirEntry}};

pub fn scan(target_path: std::path::PathBuf, output_path: std::path::PathBuf, min_diff_bytes: i64) -> Result<(usize, usize), Error> {
    let save_file_data = get_hash_iteration_count_from_file_names(&target_path, output_path.to_path_buf());
    let path_hash = save_file_data.0;
    let mut path_to_initial = output_path.clone();
    path_to_initial.push(format!("{}_initial", path_hash));

    let mut skip_set: HashSet<std::path::PathBuf> = HashSet::from_iter(vec![output_path.clone()]);

    let curr_scan = walk_iter(target_path, None, &mut skip_set);

    let iteration_count = save_file_data.1;
    let curr_is_initial_scan = iteration_count < 0;
    if curr_is_initial_scan {

        let f  = File::create(path_to_initial)?;
        let writer = BufWriter::new(f);
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

    let mut combined_diffs: Vec<CDirEntryDiff> = Vec::new();
    if iteration_count > -1 {
        let mut diff_prefix = output_path.clone();
        diff_prefix.push("tmp");
        diff_prefix.set_file_name(format!("{}_diff", path_hash));
        let res: Result<Vec<CDirEntryDiff>, Error> = add_combined_diffs(&diff_prefix, iteration_count as u16);
        match res {
            Ok(ds) => {
                combined_diffs = ds;
            }
            Err(e) => {return Err(std::io::Error::new(std::io::ErrorKind::Other, format!("failed to add combined diffs to scan: {}", e)))}
        }
    }

    let res = add_diffs_to_items::<CDirEntry, CDirEntryDiff>(&mut initial_scan, &mut combined_diffs, |a, b| {
        return a.p.cmp(&b.p);
    }, |it, d| {
        return it.p == d.p;
    }, |a| {a.diff_type == DiffType::Add}, |a| {a.diff_type == DiffType::Remove}, get_entry_from_dir_diff, merge_dir_diff_to_entry);
    if res.is_err() {
        return Err(std::io::Error::new(std::io::ErrorKind::Other, format!("failed to add diffs to scan: {:?}", res.err())))
    }

    // TODO: Get newest modified from `initial_scan`
    let mut newest_initial_entry_time: Option<SystemTime> = None;
    for ent in &initial_scan {
        if !ent.md.is_none() {
            if newest_initial_entry_time.is_none() || ent.md.unwrap() > newest_initial_entry_time.unwrap() {
                newest_initial_entry_time = ent.md;
            }
        }
    }
    if newest_initial_entry_time.is_none() {
        return Err(std::io::Error::new(std::io::ErrorKind::Other, "failed to find valid 'modified' time from initial_scan"))
    }

    let num_scan_files = curr_scan[0].files_here + curr_scan[0].files_below;
    let num_scan_dirs = curr_scan[0].dirs_here + curr_scan[0].dirs_below + 1;

    let diffs: Vec<CDirEntryDiff> = diff_saves(initial_scan, curr_scan, newest_initial_entry_time.unwrap(), iteration_count as u16 + 1, min_diff_bytes);
    let mut path_to_subsequent = output_path.clone();
    path_to_subsequent.push(format!("{}_diff_{}", path_hash, iteration_count + 1));
    let f  = File::create(path_to_subsequent)?;
    let writer = BufWriter::new(f);
    bincode::serialize_into(writer, &diffs).expect("failed to seralise");

    Ok((num_scan_files, num_scan_dirs))
}

pub fn add_combined_diffs(diff_path: &std::path::PathBuf, diff_count: u16) -> std::io::Result<Vec<CDirEntryDiff>> {
    let combined_diffs = Vec::new();
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
    let mut first_diff_path = diff_path.clone();
    first_diff_path.set_file_name(format!("{}_{}", base_file_name.to_str().unwrap(), 1));
    let mut combined_diffs= read_diff_file(first_diff_path.clone())?;
    
    // Combine diffs
    for i in 2..(diff_count + 1) {
        let mut curr_diff_path = diff_path.clone();
        curr_diff_path.set_file_name(format!("{}_{}", base_file_name.to_str().unwrap(), i));
        let next_diff = read_diff_file(curr_diff_path)?;

        add_dir_diffs(&mut combined_diffs, &next_diff);
    }

    return Ok(combined_diffs);
}