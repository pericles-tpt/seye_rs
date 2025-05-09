use std::{cmp::Ordering, io::Error, path::PathBuf};

use crate::{diff::CDirEntryDiff, save::get_hash_iteration_count_from_file_names, scan::add_combined_diffs, utility::get_shorthand_memory_limit};

pub fn report_changes(target_path: PathBuf, output_path: PathBuf, merge_nesting_diff: i32) -> std::io::Result<()> {
    let save_file_data = get_hash_iteration_count_from_file_names(&target_path, output_path.to_path_buf());
    let path_hash = save_file_data.0;
    let mut path_to_initial = output_path.clone();
    path_to_initial.push(format!("{}_initial", path_hash));

    let iteration_count = save_file_data.1;
    let curr_is_initial_scan = iteration_count < 0;
    if curr_is_initial_scan {
        return Err(std::io::Error::new(std::io::ErrorKind::Other, "No diffs to report on yet, run a scan first"))
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

    // Sort by ABSOLUTE size, if any INCREASES exactly match DECREASES, then it's probably moved
    combined_diffs.sort_by(|a, b| {
        if (a.size_here + a.size_below).abs() <= (b.size_here + b.size_below).abs() {
            return Ordering::Greater
        }
        return Ordering::Less
    });
    let mut combined_diff_sl = Vec::new();
    let mut i = 0;
    while i < combined_diffs.len() {
        // If two diffs have the same: name, hash and size remove them
        if i + 1 < combined_diffs.len() {
            if (combined_diffs[i].size_below + combined_diffs[i].size_here + combined_diffs[i + 1].size_here + combined_diffs[i + 1].size_below) == 0 && (combined_diffs[i].p.file_name() == combined_diffs[i+1].p.file_name()) {
                i += 2;
                continue;
            }
        }
        combined_diff_sl.push(combined_diffs[i].clone());
        i += 1;
    }
    combined_diffs = combined_diff_sl;

    combined_diffs.sort_by(|a, b| {
        if (a.size_here + a.size_below) <= (b.size_here + b.size_below) {
            return Ordering::Greater
        }
        return Ordering::Less
    });

    let limit = combined_diffs.len();
    let mut total: i64 = 0;
    for i in 0..limit {
        if (combined_diffs[i].size_here + combined_diffs[i].size_below) == 0 {
            continue;
        }
        let mut t = format!("{:?}",combined_diffs[i].diff_type).to_ascii_uppercase();
        let _ = t.split_off(3);
        println!("{}: {:?} ({})", t, combined_diffs[i].p, get_shorthand_memory_limit(combined_diffs[i].size_here + combined_diffs[i].size_below));
        total += combined_diffs[i].size_here + combined_diffs[i].size_below;
    }
    println!("Total change is: {}", get_shorthand_memory_limit(total));

    return Ok(());
}