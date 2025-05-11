use std::{cmp::Ordering, io::Error, path::PathBuf};

use crate::{diff::{CDirEntryDiff, DiffType}, save::get_hash_iteration_count_from_file_names, scan::add_combined_diffs, utility::get_shorthand_file_size};

pub fn report_changes(target_path: PathBuf, output_path: PathBuf, merge_nesting_diff: i32, show_moved_files: bool) -> std::io::Result<()> {
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
    let mut moved_to_paths = Vec::new();
    if merge_nesting_diff > -1 {
        moved_to_paths = Vec::with_capacity(combined_diffs.len() / 2);
        let mut combined_diff_sl = Vec::new();
        let mut i = 0;
        let merge_path_diff = merge_nesting_diff as usize;
        while i < combined_diffs.len() {
            // If two diffs have the same: size and t_diff they may be a move...
            if i + 1 < combined_diffs.len() {
                // TODO: Review this requirement for a MOVE candidate pair, a bit too lose. Size alone doesn't prove it. Maybe factor in files? Dirs inside? A hash?
                if combined_diffs[i].size_below + combined_diffs[i].size_here + combined_diffs[i + 1].size_here + combined_diffs[i + 1].size_below == 0 {
                    // ONLY consider if a move IF their nesting diff < merge_diff
                    let ap = &combined_diffs[i].p;
                    let bp = &combined_diffs[i + 1].p;
                    let a_parts: Vec<_> = ap.iter().collect();
                    let b_parts: Vec<_> = bp.iter().collect();
                    let mut j = 0;
                    let mut max_num_conseq_diffs = 0;
                    let mut num_conseq_diffs = 0;
                    // TODO: Fix `merge_nesting_diff` == 0 condition, should show MOVE with same parent directory
                    while j < a_parts.len() && j < b_parts.len() {
                        if a_parts[j] == b_parts[j] {
                            if num_conseq_diffs > max_num_conseq_diffs {
                                max_num_conseq_diffs = num_conseq_diffs;
                            }
                            num_conseq_diffs = 0;
                        } else {
                            num_conseq_diffs += 1;
                        }
                        j += 1;
                    }
                    if j < a_parts.len() && max_num_conseq_diffs < (a_parts.len() - j) {
                        max_num_conseq_diffs = a_parts.len() - j;
                    }
                    if j < b_parts.len() && max_num_conseq_diffs < (b_parts.len() - j) {
                        max_num_conseq_diffs = b_parts.len() - j;
                    }
                    let mark_as_merge = merge_nesting_diff > 0 && max_num_conseq_diffs <= merge_path_diff;
                    let mut rem = combined_diffs[i].clone();
                    let mut add = combined_diffs[i + 1].clone();
                    let mut dn = rem.diff_no;
                    if combined_diffs[i].diff_type == DiffType::Add {
                        rem = combined_diffs[i + 1].clone();
                        add = combined_diffs[i].clone();
                    }
                    if add.diff_no > rem.diff_no {
                        dn = add.diff_no;
                    }
                    if mark_as_merge {
                        combined_diff_sl.push(CDirEntryDiff{
                            p: rem.p,
                            t_diff: add.t_diff,
                            diff_no: dn,
                        
                            files_here: add.files_here,
                            files_below: add.files_below,
                            dirs_here: add.dirs_here,
                            dirs_below: add.dirs_below,
                            size_here: 0,
                            size_below: 0,
                            memory_usage_here: add.memory_usage_here,
                            memory_usage_below: add.memory_usage_below,
                            
                            diff_type: DiffType::Move,
                            files: add.files,
                        });
                        moved_to_paths.push(add.p);
                    }
                    i += 2;
                    continue;
                }
            }
            combined_diff_sl.push(combined_diffs[i].clone());
            i += 1;
        }
        combined_diffs = combined_diff_sl;
    }

    combined_diffs.sort_by(|a, b| {
        if (a.size_here + a.size_below) <= (b.size_here + b.size_below) {
            return Ordering::Greater
        }
        return Ordering::Less
    });

    let limit = combined_diffs.len();
    let mut total: i64 = 0;
    let mut moved_to_idx = 0;
    for i in 0..limit {
        let mut t = format!("{:?}",combined_diffs[i].diff_type).to_ascii_uppercase();
        let _ = t.split_off(3);
        if show_moved_files && combined_diffs[i].diff_type == DiffType::Move {
            println!("{}: {:?} -> {:?} ({})", t, combined_diffs[i].p, moved_to_paths[moved_to_idx], get_shorthand_file_size(combined_diffs[i].size_here + combined_diffs[i].size_below));
            moved_to_idx += 1;
            continue;
        }
        if (combined_diffs[i].size_here + combined_diffs[i].size_below) == 0 {
            continue;
        }
        println!("{}: {:?} ({})", t, combined_diffs[i].p, get_shorthand_file_size(combined_diffs[i].size_here + combined_diffs[i].size_below));
        total += combined_diffs[i].size_here + combined_diffs[i].size_below;
    }
    println!("Total change is: {}", get_shorthand_file_size(total));

    return Ok(());
}