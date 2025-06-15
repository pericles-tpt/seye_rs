use std::{cmp::Ordering, fs::exists, io::Error, path::PathBuf};
use crate::{diff::{get_diff_type_shorthand, DiffEntry, ADD_DT_IDX, MOD_DT_IDX, REM_DT_IDX}, save::{self, read_diff_file, read_save_file}, scan::add_combined_diffs, utility, walk::CDirEntry, Config};

pub fn report_changes(target_path: PathBuf, output_path: PathBuf, cfg: Config) -> std::io::Result<()> {
    let root_path_hash = save::get_hash_from_root_path(&target_path);
    let mut path_to_initial = output_path.clone();
    path_to_initial.push(format!("{}_initial", root_path_hash));

    let mut path_to_diff = output_path.clone();
    path_to_diff.push(format!("{}_diffs", root_path_hash));
    let diff_exists = exists(&path_to_diff)?;
    if !diff_exists {
        return Err(std::io::Error::new(std::io::ErrorKind::Other, "No diffs found, run a scan first"))
    }

    let full_scan_entries: Vec<CDirEntry>;
    let maybe_last_scan = read_save_file(path_to_initial);
    match maybe_last_scan {
        Ok(entries) => {full_scan_entries = entries}
        Err(e) => {return Err(std::io::Error::new(std::io::ErrorKind::Other, format!("Failed to read entries from file: {}", e)))}
    }

    let combined_diffs: DiffEntry;
    let diff_file = read_diff_file(&path_to_diff)?;
    let res: Result<DiffEntry, Error> = add_combined_diffs(&diff_file, &full_scan_entries, cfg.maybe_start_report_time, cfg.maybe_end_report_time);
    match res {
        Ok(ds) => {
            combined_diffs = ds;
        }
        Err(e) => {return Err(std::io::Error::new(std::io::ErrorKind::Other, format!("failed to add combined diffs to scan: {}", e)))}
    }

    if combined_diffs.diffs.len() == 0 {
        println!("No diffs found in the provided time range");
        return Ok(());
    }

    let last_add_idx: i32 = combined_diffs.diffs[ADD_DT_IDX].len() as i32 - 1;
    let last_rem_idx: i32 = last_add_idx + combined_diffs.diffs[REM_DT_IDX].len() as i32;
    let mut all_diffs: Vec<_> = combined_diffs.diffs.concat().into_iter().enumerate().collect();
    all_diffs.sort_by(|a, b| {
        if (a.1.size_here + a.1.size_below) <= (b.1.size_here + b.1.size_below) {
            return Ordering::Greater
        }
        return Ordering::Less
    });
    // Replace original index in each entry pair with the ADD, REM or MOD identifier
    for i in 0..all_diffs.len() {
        let d = &all_diffs[i];
        if last_add_idx > -1 && d.0 <= last_add_idx as usize {
            all_diffs[i].0 = ADD_DT_IDX;
        } else if last_rem_idx > -1 && d.0 <= last_rem_idx as usize {
            all_diffs[i].0 = REM_DT_IDX;
        } else {
            all_diffs[i].0 = MOD_DT_IDX;
        }
    }

    let limit = all_diffs.len();
    let mut total: i64 = 0;
    let mut found_first_negative_diff = false;
    for i in 0..limit {
        let t = get_diff_type_shorthand(all_diffs[i].0);
        if cfg.show_moved_files && !found_first_negative_diff && all_diffs[i].1.size_here + all_diffs[i].1.size_below < 0 {
            found_first_negative_diff = true;
            for kv in &combined_diffs.move_to_paths {
                let from = kv.0;
                let to = kv.1;
                println!("MOV: {:?} -> {:?} ({})", from, to, utility::get_shorthand_file_size(0));
            }
        }
        if (all_diffs[i].1.size_here + all_diffs[i].1.size_below) == 0 {
            continue;
        }
        println!("{}: {:?} ({})", t, all_diffs[i].1.p, utility::get_shorthand_file_size(all_diffs[i].1.size_here + all_diffs[i].1.size_below));
        total += all_diffs[i].1.size_here + all_diffs[i].1.size_below;
    }
    println!("Total change is: {}", utility::get_shorthand_file_size(total));

    return Ok(());
}