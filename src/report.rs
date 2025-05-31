use std::{cmp::Ordering, collections::HashMap, io::Error, path::PathBuf};
use crate::{diff::{self, DiffFile}, save, scan, utility, Config};

pub fn report_changes(target_path: PathBuf, output_path: PathBuf, cfg: Config) -> std::io::Result<()> {
    let save_file_data = save::get_hash_iteration_count_from_file_names(&target_path, output_path.to_path_buf());
    let path_hash = save_file_data.0;
    let mut path_to_initial = output_path.clone();
    path_to_initial.push(format!("{}_initial", path_hash));

    let iteration_count = save_file_data.1;
    let curr_is_initial_scan = iteration_count < 0;
    if curr_is_initial_scan {
        return Err(std::io::Error::new(std::io::ErrorKind::Other, "No diffs found, run a scan first"))
    }

    let mut combined_diffs: DiffFile = DiffFile { diffs: vec![], move_to_paths: HashMap::new() };
    if iteration_count > -1 {
        let mut diff_prefix = output_path;
        diff_prefix.push("tmp");
        diff_prefix.set_file_name(format!("{}_diff", path_hash));
        let res: Result<DiffFile, Error> = scan::add_combined_diffs(&diff_prefix, iteration_count as u16, cfg.maybe_start_report_time, cfg.maybe_end_report_time);
        match res {
            Ok(ds) => {
                combined_diffs = ds;
            }
            Err(e) => {return Err(std::io::Error::new(std::io::ErrorKind::Other, format!("failed to add combined diffs to scan: {}", e)))}
        }
    }

    if combined_diffs.diffs.len() == 0 {
        println!("No diffs found in the provided time range");
        return Ok(());
    }

    combined_diffs.diffs.sort_by(|a, b| {
        if (a.size_here + a.size_below) <= (b.size_here + b.size_below) {
            return Ordering::Greater
        }
        return Ordering::Less
    });

    let limit = combined_diffs.diffs.len();
    let mut total: i64 = 0;
    for i in 0..limit {
        let mut t = format!("{:?}",combined_diffs.diffs[i].diff_type).to_ascii_uppercase();
        let _ = t.split_off(3);
        if cfg.show_moved_files && combined_diffs.diffs[i].diff_type == diff::DiffType::MoveDir {
            let maybe_to_path = combined_diffs.move_to_paths.get(&combined_diffs.diffs[i].p);
            if maybe_to_path.is_some() {
                println!("{}: {:?} -> {:?} ({})", t, combined_diffs.diffs[i].p, maybe_to_path.unwrap(), utility::get_shorthand_file_size(combined_diffs.diffs[i].size_here + combined_diffs.diffs[i].size_below));
            } else {
                println!("{}: {:?} -> ? ({})", t, combined_diffs.diffs[i].p, utility::get_shorthand_file_size(combined_diffs.diffs[i].size_here + combined_diffs.diffs[i].size_below));
            }
            continue;
        }
        if (combined_diffs.diffs[i].size_here + combined_diffs.diffs[i].size_below) == 0 {
            continue;
        }
        println!("{}: {:?} ({})", t, combined_diffs.diffs[i].p, utility::get_shorthand_file_size(combined_diffs.diffs[i].size_here + combined_diffs.diffs[i].size_below));
        total += combined_diffs.diffs[i].size_here + combined_diffs.diffs[i].size_below;
    }
    println!("Total change is: {}", utility::get_shorthand_file_size(total));

    return Ok(());
}