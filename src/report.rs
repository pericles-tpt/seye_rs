use std::{cmp::Ordering, collections::HashMap, fs::read_dir, io::Error, path::PathBuf};

use crate::save;
use bincode::de;
use chksum_md5 as md5;

use crate::{diff::{CDirEntryDiff, DiffType}, save::get_hash_iteration_count_from_file_names, scan::add_combined_diffs, utility::get_shorthand_file_size, walk::DiffScan};

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

    // Read initial scan, need hashes for "MOVE" checking
    let initial_scan = save::read_save_file(path_to_initial)?;

    let mut combined_diffs: DiffScan = DiffScan { entries: vec![], hashes: vec![] };
    if iteration_count > -1 {
        let mut diff_prefix = output_path.clone();
        diff_prefix.push("tmp");
        diff_prefix.set_file_name(format!("{}_diff", path_hash));
        let res: Result<DiffScan, Error> = add_combined_diffs(&diff_prefix, iteration_count as u16);
        match res {
            Ok(ds) => {
                combined_diffs = ds;
            }
            Err(e) => {return Err(std::io::Error::new(std::io::ErrorKind::Other, format!("failed to add combined diffs to scan: {}", e)))}
        }
    }
    let combined_diff_entries_len = combined_diffs.entries.len();
    let combined_diff_hashes_len = combined_diffs.hashes.len();

    // Initialise HashMap to include the indexes of each hashpair in `combined_diffs.hashes`
    let mut hashes_idx_mapping: HashMap<usize, Option<usize>> = HashMap::with_capacity(combined_diffs.hashes.len());
    for hp in &combined_diffs.hashes {
        hashes_idx_mapping.insert(hp.0, None);
    }
    // Sort the keys by their size AND update `hashes_idx_mapping` so that the hash indexes remain valid
    let mut idx_keys: Vec<(usize, CDirEntryDiff)> = combined_diffs.entries.clone().into_iter().enumerate().collect();
    idx_keys.sort_by(|a, b| {
        let mut cmp = Ordering::Equal;
        if (a.1.size_here + a.1.size_below).abs() > (b.1.size_here + b.1.size_below).abs() {
            cmp = Ordering::Greater;
            // How do indexes need to be updated for each `Ordering` type
            if hashes_idx_mapping.contains_key(&a.0) {
                hashes_idx_mapping.insert(a.0, Some(b.0));
            }
            if hashes_idx_mapping.contains_key(&b.0) {
                hashes_idx_mapping.insert(b.0, Some(a.0));
            }
        } else if (a.1.size_here + a.1.size_below).abs() < (b.1.size_here + b.1.size_below).abs() {
            cmp = Ordering::Less;
        }
        return cmp;
    });
    // After the sort, update the hash indexes from the mapping
    let mut i = 0;
    while i < combined_diff_hashes_len {
        let hp = &combined_diffs.hashes[i];
        let maybe_mapping = hashes_idx_mapping.get(&hp.0);
        if maybe_mapping.is_some() {
            combined_diffs.hashes[i].0 = maybe_mapping.unwrap().unwrap();
        }
        i += 1;
    }
    // Move entries
    combined_diffs.entries = idx_keys.into_iter().map(|p| {
        p.1
    }).collect();

    // let mut moved_candidates: HashMap<usize, (Vec<usize>, Vec<usize>)> = HashMap::new();
    let mut moved_indexes = vec![];
    if merge_nesting_diff > -1 {
        let mut moved_candidates = HashMap::with_capacity(combined_diff_entries_len / 2);
        let mut i = 0;
        while i < combined_diff_entries_len {
            // If two diffs have the same: size and t_diff they may be a move...
            let abs_size_a = (combined_diffs.entries[i].size_below + combined_diffs.entries[i].size_here).abs();
            let mut abs_size_b = None;
            let a_is_add_or_rem = combined_diffs.entries[i].diff_type == DiffType::Add || combined_diffs.entries[i].diff_type == DiffType::Remove;
            let mut b_is_add_or_rem = false;
            if i + 1 < combined_diffs.entries.len() {
                abs_size_b = Some((combined_diffs.entries[i + 1].size_below + combined_diffs.entries[i + 1].size_here).abs());
                b_is_add_or_rem = combined_diffs.entries[i + 1].diff_type == DiffType::Add || combined_diffs.entries[i + 1].diff_type == DiffType::Remove;
            }

            // Collect all items that have +- this size
            let mut rems = Vec::new();
            let mut adds = Vec::new();                
            let mut abs_size_match = abs_size_b.is_some() && abs_size_a == abs_size_b.unwrap() && a_is_add_or_rem && b_is_add_or_rem;
            let mut j = i;
            while abs_size_match {
                let curr_size = combined_diffs.entries[j].size_here + combined_diffs.entries[j].size_below;
                let is_rem = combined_diffs.entries[j].size_here < 0;
                if is_rem {
                    rems.push(j);
                } else {
                    adds.push(j);
                }

                if j + 1 >= combined_diffs.entries.len() {
                    break;
                }
                let next_size = combined_diffs.entries[j + 1].size_here + combined_diffs.entries[j + 1].size_below;
                let next_is_add_or_rem = combined_diffs.entries[j + 1].diff_type == DiffType::Add || combined_diffs.entries[j + 1].diff_type == DiffType::Remove;
                abs_size_match = curr_size.abs() == next_size.abs() && next_is_add_or_rem;
                j += 1;
            }
            if j > i {
                j -= 1;
            }
            i = j + 1;

            let no_moves = rems.len() == 0 || adds.len() == 0;
            if no_moves {
                continue;
            }

            // Map: usize -> (Vec, Vec)
            moved_candidates.insert(abs_size_a as usize, (adds, rems));
        }

        println!("mcs: {:?}", moved_candidates);

        moved_indexes = Vec::with_capacity(moved_candidates.len());
        for ent in moved_candidates {
            let mut add_hashes = Vec::with_capacity(ent.1.0.len());
            for ah in ent.1.0 {
                // Is it in `combined_diffs`?
                let maybe_match = combined_diffs.hashes.iter().find(|h| {
                    return h.0 == ah;
                });
                if maybe_match.is_some() {
                    add_hashes.push(maybe_match.unwrap().1[0].clone());
                    continue;
                }
                println!("Generating hash for add!");
                let ent_p = combined_diffs.entries[ah].p.clone();
                add_hashes.push(generate_hash_for_dir(ent_p));
            }

            let mut rem_hashes = Vec::with_capacity(ent.1.1.len());
            for rh in ent.1.1 {
                // Is it in `combined_diffs`?
                let maybe_match = combined_diffs.hashes.iter().find(|h| {
                    return h.0 == rh;
                });
                if maybe_match.is_some() {
                    rem_hashes.push(maybe_match.unwrap().1[0].clone());
                    continue;
                }
                // Is it in original hashes? Maybe...
                let ent_p = combined_diffs.entries[rh].p.clone();
                let mut i = 0;
                let mut idx: i128 = -1;
                while i < initial_scan.entries.len() {
                    let ent = initial_scan.entries[i].clone();
                    if ent.p == ent_p {
                        idx = i as i128;
                        break;
                    }
                    i += 1;
                }
                if idx > -1 {
                    rem_hashes.push(initial_scan.hashes[idx as usize][0].clone());
                    continue;
                }
                println!("Generating hash for rem!");
                rem_hashes.push(generate_hash_for_dir(ent_p));
            }

            println!("ahs: {:?}", add_hashes);
            println!("rhs: {:?}", rem_hashes);

            // TODO: Now I've got hashes that correspond with each entry... need to determine if any `rems` matchs `adds`
            // TODO: Moved should be added to a new vector and the add/removes (might) be skipped in favour of remove entries
            let mut i = 0;
            let mut j = 0;
            while i < add_hashes.len() {
                while j < rem_hashes.len() {
                    if add_hashes[i] == rem_hashes[j] {
                        moved_indexes.push((i, j, ent.0));
                        break;
                    }
                    j += 1;
                }
                i += 1;
            }

            // ARCHIVE: Old stuff for: checking nesting for `-mvd` flag and CDirEntryDiff for Move
            //             let ap = &combined_diffs.entries[i].p;
            //             let bp = &combined_diffs.entries[i + 1].p;
            //             let a_parts: Vec<_> = ap.iter().rev().collect();
            //             let b_parts: Vec<_> = bp.iter().rev().collect();
            //             let mut j = 0;
            //             while j < a_parts.len() && j < b_parts.len() {
            //                 if a_parts[j] != b_parts[j] {
            //                     break;
            //                 }
            //                 j += 1;
            //             }
            //             let mark_as_merge = j <= merge_path_diff;
            //             if mark_as_merge {
            //                 combined_diff_sl.push(CDirEntryDiff{
            //                     p: rem.p,
            //                     t_diff: add.t_diff,
                            
            //                     files_here: add.files_here,
            //                     files_below: add.files_below,
            //                     dirs_here: add.dirs_here,
            //                     dirs_below: add.dirs_below,
            //                     size_here: 0,
            //                     size_below: 0,
                                
            //                     diff_type: DiffType::Move,
            //                     files: add.files,
            //                     symlinks: add.symlinks,
            //                 });
            //                 moved_to_paths.push(add.p);
            //             }
        }
    }

    let limit = combined_diffs.entries.len();
    let mut total: i64 = 0;
    for i in 0..limit {
        let mut t = format!("{:?}",combined_diffs.entries[i].diff_type).to_ascii_uppercase();
        let _ = t.split_off(3);
        if (combined_diffs.entries[i].size_here + combined_diffs.entries[i].size_below) == 0 {
            continue;
        }
        println!("{}: {:?} ({})", t, combined_diffs.entries[i].p, get_shorthand_file_size(combined_diffs.entries[i].size_here + combined_diffs.entries[i].size_below));
        total += combined_diffs.entries[i].size_here + combined_diffs.entries[i].size_below;
    }
    if show_moved_files {
        println!("Moved Items");
        for i in 0.. moved_indexes.len() {
            let from_path = &combined_diffs.entries[moved_indexes[i].0].p;
            let to_path = &combined_diffs.entries[moved_indexes[i].1].p;
            let size_moved = get_shorthand_file_size(moved_indexes[i].2 as i64);
            println!("MOV: {:?} -> {:?} ({})", from_path, to_path, size_moved);
        }
    }
    println!("Total change is: {}", get_shorthand_file_size(total));

    return Ok(());
}

// TODO: Currently just generates a random MD5
fn generate_hash_for_dir(_p: PathBuf) -> String {
    // Traverse directories, collect file paths, do md5's, bubble up
    let mut m: Vec<(PathBuf, Vec<String>)> = vec![];
    let mut i = 0;
    let mut q: Vec<PathBuf> = vec![_p.clone()];
    while i < q.len() {
        let mut all_items = vec![];
        let mut file_idxs = vec![];
        let mut file_items = vec![];
        m.push((q[i].clone(), vec![]));

        let dents = std::fs::read_dir(&q[i]);
        if dents.is_err() {
            // TODO: Something...
        } else {
            let mut j = 0;
            for d in dents.unwrap() {
                if let Ok(de) = d {
                    if let Ok(ft) = de.file_type() {
                        if ft.is_dir() {
                            q.push(de.path());
                            all_items.push(de.path().into_os_string().into_string().unwrap());
                        } else if ft.is_file() {
                            all_items.push(String::new());
                            file_idxs.push(j);
                            file_items.push(de.path());
                        }
                    }
                };
                j += 1;
            }
        }

        let file_hashes: Vec<String> = file_items.iter().map(|pb| {
                let mut hash_string: String = String::from("");
                let file = std::fs::File::open(pb);
                if file.is_ok() {
                    let digest = md5::chksum(file.unwrap());
                    if digest.is_ok() {
                        hash_string = digest.unwrap().to_hex_lowercase();
                    }
                }
                return hash_string;
        }).collect();

        for j in 0..file_hashes.len() {
            all_items[file_idxs[j]] = file_hashes[j].clone();
        }

        println!("hs: {:?}", file_hashes);
        
        i += 1;
    }


    // Go through everything again to re-assign hashes from dirs
    let mut root_idx = 0;
    for i in 0..m.len() - 1 {
        let mut item = m[m.len() - 1 - i].clone();
        
        if item.0 == _p {
            root_idx = m.len() - 1 - i;
        }
        
        for j in 0..item.1.len() {
            if item.1[j].starts_with("/") {
                let all_child_hashes = item.1.join("");
    
                let digest = md5::chksum(all_child_hashes);
                item.1[j] = digest.unwrap().to_hex_lowercase();
                m[i] = item.clone();
            }
        }
    }

    let all_child_hashes = m[root_idx].1.join("");
    println!("afh: {:?}", all_child_hashes);
    let digest = md5::chksum(all_child_hashes);
    return digest.unwrap().to_hex_lowercase();
}