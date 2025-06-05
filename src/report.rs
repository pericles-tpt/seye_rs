use std::{cmp::Ordering, fs::exists, io::Error, path::PathBuf};
use crate::{diff::{self, DiffEntry}, save::{self, read_diff_file, read_save_file}, scan::add_combined_diffs, utility, walk::CDirEntry, Config};

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

    let mut combined_diffs: DiffEntry;
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