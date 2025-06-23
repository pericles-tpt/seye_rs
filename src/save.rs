use std::{cmp::Ordering, collections::{HashMap, HashSet}, ffi::OsString, fs::File, hash::{DefaultHasher, Hasher}, io::BufReader, os::unix::ffi::OsStrExt, path::PathBuf, time::{SystemTime, UNIX_EPOCH}, usize};
use std::io;
use crate::{diff::{get_entry_from_dir_diff, ignore_dir_entry, CDirEntryDiff, DiffEntry, DiffFile, FileEntryDiff, TDiff, ADD_DT_IDX, MOD_DT_IDX, NUM_DT, REM_DT_IDX}, scan::add_combined_diffs, walk::{CDirEntry, FileEntry}};

const _START_VECTOR_BYTES: u64 = 8;

pub fn get_hash_from_root_path(root: &std::path::PathBuf) -> String {
    let root_hash_str: String;

    let mut hasher = DefaultHasher::new();
    hasher.write(root.as_os_str().as_bytes());
    root_hash_str = format!("{:x}", hasher.finish());

    return root_hash_str;
}

pub fn read_save_file(file_path: PathBuf) -> io::Result<Vec<CDirEntry>> {
    let fp = File::open(&file_path)?;
    let reader = BufReader::new(fp);
    let res: Result<Vec<CDirEntry>, _> = bincode::deserialize_from(reader);

    // Handle the deserialization error
    match res {
        Ok(entries) => Ok(entries),
        Err(e) => Err(io::Error::new(io::ErrorKind::InvalidData, format!("Deserialization error: {}", e))),
    }
}

pub fn read_diff_file(file_path: &PathBuf) -> io::Result<DiffFile> {
    let fp = File::open(&file_path)?;
    let reader = BufReader::new(fp);
    let res: Result<DiffFile, _> = bincode::deserialize_from(reader);

    // Handle the deserialization error
    match res {
        Ok(entries) => Ok(entries),
        Err(e) => Err(io::Error::new(io::ErrorKind::InvalidData, format!("Deserialization error: {}", e))),
    }
}

pub fn diff_saves(mut original_file: DiffFile, o: Vec<CDirEntry>, n: Vec<CDirEntry>, combined_diffs: DiffEntry, min_diff_bytes: usize, cache_merged_diffs: bool) -> DiffFile {
    if o.len() == 0 && n.len() == 0 {
        return original_file;
    }

    let mut new_entry = DiffEntry {   
        diffs: Default::default(),
        move_to_paths: HashMap::new(),
    };

    let mut oi = 0;
    let mut ni = 0;
    let mut old = &o[0];
    let mut new = &o[0];
    let mut remove_hash_idxs: HashMap<[u8; 16], usize> = HashMap::new();
    let mut add_hash_idxs: HashMap<[u8; 16], usize> = HashMap::new();
    
    let mut add_rem_vec = combined_diffs.diffs[ADD_DT_IDX].clone();
    add_rem_vec.append(&mut combined_diffs.diffs[REM_DT_IDX].clone());
    let mut add_rem_set: HashSet<PathBuf> = HashSet::from_iter(add_rem_vec.into_iter().map(|d|{d.p}));
    let mut moved_paths: Vec<PathBuf> = combined_diffs.move_to_paths.clone().into_values().collect();
    add_rem_set.extend(moved_paths.clone());

    while oi < o.len() || ni < n.len() {
        let old_left = oi < o.len();
        if old_left {
            old = &o[oi];
        }
        let new_left = ni < n.len();
        if new_left {
            new = &n[ni];
        }

        let mut diff_type: usize = ADD_DT_IDX; // -> new_left        
        if old_left && new_left {
            let old_new_cmp = old.p.cmp(&new.p);
            match old_new_cmp {
                Ordering::Equal => {
                    diff_type = MOD_DT_IDX;
                },
                Ordering::Less => {
                    // old item removed, NEXT old item *might* match CURRENT new
                    diff_type = REM_DT_IDX;
                    ni -= 1;
                },
                Ordering::Greater => {   
                    // new item added, CURRENT old item *might* match NEXT new 
                    diff_type = ADD_DT_IDX;
                    oi -= 1;
                }
            }
        } else if old_left {
            diff_type = REM_DT_IDX;
        }

        let diff_passes_threshold;
        match diff_type {
            ADD_DT_IDX => {
                add_rem_set.insert(new.p.clone());
                let maybe_move_match = remove_hash_idxs.get(&new.md5);
                if maybe_move_match.is_some() {
                    let update_idx = maybe_move_match.unwrap();
                    let old_path = &new_entry.diffs[REM_DT_IDX][*update_idx].p.clone();
                    if moved_paths.iter().find(|el|{ old_path.starts_with(el) }).is_none() {
                        // TODO: Make it clearer that setting a "default" PathBuf here marks it as ignored
                        new_entry.diffs[REM_DT_IDX][*update_idx].p = PathBuf::new();
                        new_entry.move_to_paths.insert(old_path.to_path_buf(), new.p.clone());
                        moved_paths.push(old_path.to_path_buf());
                    }
                    remove_hash_idxs.remove(&new.md5);

                    oi += 1;
                    ni += 1;
                    continue;
                }
                
                if new.p.parent().is_some() && add_rem_set.contains(new.p.parent().unwrap()) {
                    oi += 1;
                    ni += 1;
                    continue;
                }
                add_hash_idxs.insert(new.md5, new_entry.diffs[ADD_DT_IDX].len());
                new_entry.diffs[ADD_DT_IDX].push(CDirEntryDiff{                    
                    p: new.p.clone(),
                    t_diff: get_t_diff_from_md(new.md, false),
                
                    files_here: new.files_here,
                    files_below: new.files_below,
                    dirs_here: new.dirs_here,
                    dirs_below: new.dirs_below,
                    size_here: new.size_here as i64,
                    size_below: new.size_below as i64,
                
                    files: get_file_diffs(Vec::new(), new.files.to_vec(),),
                    symlinks: get_file_diffs(Vec::new(), new.symlinks.to_vec()),
                });
            },
            REM_DT_IDX => {
                add_rem_set.insert(old.p.clone());
                let maybe_move_match = add_hash_idxs.get(&old.md5);
                if maybe_move_match.is_some() {
                    let update_idx = maybe_move_match.unwrap();
                    let new_path = new_entry.diffs[ADD_DT_IDX][*update_idx].p.clone();
                    if moved_paths.iter().find(|el|{ old.p.starts_with(el) }).is_none() {
                        // TODO: Make it clearer that setting a "default" PathBuf here marks it as ignored
                        new_entry.diffs[ADD_DT_IDX][*update_idx].p = PathBuf::new();
                        new_entry.move_to_paths.insert(old.p.clone(), new_path.to_path_buf());
                        moved_paths.push(old.p.to_path_buf());
                    }
                    add_hash_idxs.remove(&old.md5);

                    oi += 1;
                    ni += 1;
                    continue;
                }

                if old.p.parent().is_some() && add_rem_set.contains(old.p.parent().unwrap()) {
                    oi += 1;
                    ni += 1;
                    continue;
                }
                remove_hash_idxs.insert(old.md5, new_entry.diffs[REM_DT_IDX].len());
                new_entry.diffs[REM_DT_IDX].push(CDirEntryDiff{
                    p: old.p.clone(),
                    t_diff: get_t_diff_from_md(old.md, true),
                
                    files_here: old.files_here,
                    files_below: old.files_below,
                    dirs_here: old.dirs_here,
                    dirs_below: old.dirs_below,
                    size_here: old.size_here as i64 * -1,
                    size_below: old.size_below as i64 * -1,
                
                    files: get_file_diffs(old.files.to_vec(), Vec::new()),
                    symlinks: get_file_diffs(old.symlinks.to_vec(), Vec::new()),
                });
            },
            MOD_DT_IDX => {
                let maybe_modified_dir_diff = get_maybe_modified_dir_diff(old.clone(), new.clone());
                match maybe_modified_dir_diff {
                    Some(d) => {
                        diff_passes_threshold = d.size_here.abs() >= min_diff_bytes as i64;
                        if diff_passes_threshold {
                            new_entry.diffs[MOD_DT_IDX].push(d);
                        }
                    },
                    None => {}
                }
            },
            _ => { /* Should never be triggered here */ }
        }

        oi += 1;
        ni += 1;
    }

    // Apply filter given `-md` argument
    for i in 0..new_entry.diffs.len() {
        new_entry.diffs[i] = new_entry.diffs[i].clone().into_iter().filter(|it| {
        return !ignore_dir_entry(it) && (it.size_here + it.size_below).abs() >= min_diff_bytes as i64}).collect();
    }

    // Pop off existing combined diff (if it exists)
    let mut combined_diff_entries = None;
    if original_file.has_merged_diff {
        combined_diff_entries = original_file.entries.pop();
        original_file.timestamps.pop();
    }

    // Add new (non-empty) entry
    let mut largest_num_diffs = 0;
    for i in 0..NUM_DT {
        let curr_num_diffs = new_entry.diffs[i].len();
        if curr_num_diffs > largest_num_diffs {
            largest_num_diffs = curr_num_diffs;
        }
    }
    let new_entry_empty = largest_num_diffs == 0 && new_entry.move_to_paths.len() == 0;
    if !new_entry_empty {
        original_file.entries.push(new_entry.clone());
        original_file.timestamps.push(SystemTime::now());
    }
    
    // Push combined diff back on (maybe), `cache_merged_diffs` overrides whatever the file says
    original_file.has_merged_diff = cache_merged_diffs;
    if cache_merged_diffs {
        let mut new_combined_diff = None;
        if combined_diff_entries.is_some() {
            new_combined_diff = Some(add_diffs(&o, vec![combined_diff_entries.unwrap(), new_entry]));
        } else {
            let maybe_combined = add_combined_diffs(&original_file, &o, None, None);
            // TODO: Handle error
            if maybe_combined.is_ok() {
                new_combined_diff = Some(maybe_combined.unwrap());
            }
        }
        
        if new_combined_diff.is_some() {
            // LAST index should be cached entry...
            original_file.entries.push(new_combined_diff.unwrap());
            original_file.timestamps.push(SystemTime::UNIX_EPOCH);
        } else {
            original_file.has_merged_diff = false;
        }
    }

    return original_file;
}

fn get_maybe_modified_dir_diff(ent_o: CDirEntry, ent_n: CDirEntry) -> Option<CDirEntryDiff> {    
    let diff_here = ent_o.dirs_here != ent_n.dirs_here || ent_o.files_here != ent_n.files_here || ent_o.size_here != ent_n.size_here;
    if !diff_here {
        return None;
    }

    let t_diff_o = get_t_diff_from_md(ent_o.md, false);
    let t_diff_n = get_t_diff_from_md(ent_n.md, false);
    
    return Some(CDirEntryDiff {
        p: ent_n.p,
        t_diff: TDiff{
            s_diff: t_diff_n.s_diff - t_diff_o.s_diff,
            ns_diff: t_diff_n.ns_diff - t_diff_o.ns_diff,
        },
    
        files_here: ent_n.files_here - ent_o.files_here,
        files_below: ent_n.files_below - ent_o.files_below,
        dirs_here: ent_n.dirs_here - ent_o.dirs_here,
        dirs_below: ent_n.dirs_below - ent_o.dirs_below,
        size_here: (ent_n.size_here - ent_o.size_here) as i64,
        size_below: (ent_n.size_below - ent_o.size_below) as i64,
    
        files: get_file_diffs(ent_o.files.to_vec(), ent_n.files.to_vec()),
        symlinks: get_file_diffs(ent_o.symlinks.to_vec(), ent_n.symlinks.to_vec()),
    });
}

fn get_t_diff_from_md(md: Option<SystemTime>, negate: bool) -> TDiff {
    let sign = if negate {-1} else {1};
    let mut ret = TDiff{
        s_diff: 0,
        ns_diff: 0,
    };
    if !md.is_none() {
        let ost = md.unwrap().duration_since(UNIX_EPOCH).unwrap();
        ret.s_diff = ost.as_secs() as i64 * sign;
        ret.ns_diff = ost.as_nanos() as i128 * sign as i128 ;
    }
    return ret;
}

fn get_file_diffs(o: Vec<FileEntry>, n: Vec<FileEntry>) -> [Vec<FileEntryDiff>; NUM_DT] {
    let mut diffs: [Vec<FileEntryDiff>; 3] = Default::default();

    let root_pb = OsString::from("/");
    let mut maybe_removed_path_idxs = HashMap::<OsString, usize>::new();
    let mut oidx = 0;
    let mut nidx = 0;
    loop {
        let mut curr_o: Option<FileEntry> = None;
        let mut curr_n: Option<FileEntry> = None;
        let mut base_path_o: OsString = root_pb.clone();
        let mut base_path_n: OsString = root_pb.clone();

        if oidx >= o.len() && nidx >= n.len() {
            break;
        }

        if oidx < o.len() {
            curr_o = Some(o[oidx].clone());
            base_path_o = o[oidx].clone().bn;
        }
        if nidx < n.len() {
            curr_n = Some(n[nidx].clone());
            base_path_n = n[nidx].clone().bn;
        }

        if curr_n.is_none() {
            maybe_removed_path_idxs.insert(base_path_o, oidx);
            oidx += 1;
            continue;
        } else if curr_o.is_none() {
            let ent_n = curr_n.clone().unwrap();
            diffs[ADD_DT_IDX].push(FileEntryDiff {
                bn: ent_n.bn,
                t_diff: get_t_diff_from_md(ent_n.md, false),
                sz: ent_n.sz as i128,
            });
            nidx += 1;
            continue;
        }

        let ent_o = curr_o.clone().unwrap();
        let ent_n = curr_n.clone().unwrap();
        
        if base_path_n != base_path_o {
            diffs[ADD_DT_IDX].push(FileEntryDiff {
                bn: ent_n.bn,
                t_diff: get_t_diff_from_md(ent_n.md, false),
                sz: ent_n.sz as i128,
            });

            // TODO: This doesn't recognise if `base_path_o` was ALREADY added to diff, should check if it exists in diff and remove it...
            if base_path_n.as_os_str() > base_path_o.as_os_str() {
                // Remove path o
                maybe_removed_path_idxs.insert(base_path_o.clone(), oidx);
                oidx += 1;
            }
            nidx += 1;
            continue;
        }

        // Modify (maybe)
        let maybe_modified_dir_diff = get_maybe_modified_file_diff(ent_o, ent_n);
        match maybe_modified_dir_diff {
            Some(d) => {diffs[MOD_DT_IDX].push(d)},
            None => {}
        }

        oidx += 1;
        nidx += 1;
    }

    // Add these entries as 'Removed'
    for tup in maybe_removed_path_idxs {
        let ent = &o[tup.1];
        diffs[REM_DT_IDX].push(FileEntryDiff {
            bn: ent.bn.clone(),
            t_diff: get_t_diff_from_md(ent.md, true),
            sz: ent.sz as i128 * -1,
        })
    }

    return diffs;
}

pub fn add_dir_diffs(df: &DiffFile, full_scan_entries: &Vec<CDirEntry>, start_idx: usize, end_idx: usize) -> DiffEntry {
    let mut ret = DiffEntry {
        diffs: Default::default(),
        move_to_paths: HashMap::new(),
    };
    
    let path_entry_lookup: HashMap<PathBuf, usize> = full_scan_entries.clone().into_iter().enumerate().map(|p|{(p.1.p, p.0)}).collect();
    let mut rev_move_to_paths = HashMap::new();
    for i in start_idx..(end_idx + 1) {
        let mut curr = df.entries[i].clone();
        
        let mut is_new_lookup: [Vec<bool>; 3] = Default::default();
        for j in 0..NUM_DT {
            is_new_lookup[j] = vec![false; ret.diffs[j].len()];
            is_new_lookup[j].extend(vec![true; curr.diffs[j].len()]);
        }
        
        let to_paths_dup = ret.move_to_paths.clone();
        for da_idx in 0..curr.diffs[ADD_DT_IDX].len() {
            let new_diff_path = &curr.diffs[ADD_DT_IDX][da_idx].p.clone();

            // 1. A -> B, ADD: A => MOV -> MOD(A), ADD(B) (dup of A w/ different path)
            let maybe_to_path = to_paths_dup.get(new_diff_path);
            if maybe_to_path.is_some() {
                let maybe_entry_idx = path_entry_lookup.get(new_diff_path);
                if maybe_entry_idx.is_none() {
                    continue;
                }
                let old_a = &full_scan_entries[*maybe_entry_idx.unwrap()];
                ret.move_to_paths.remove(new_diff_path);

                // MOD A
                let maybe_a_diff = get_maybe_modified_dir_diff(old_a.clone(), get_entry_from_dir_diff(curr.diffs[ADD_DT_IDX][da_idx].clone()));
                if maybe_a_diff.is_some() {
                    curr.diffs[MOD_DT_IDX][da_idx] = maybe_a_diff.unwrap();
                }
                // ADD B
                let add_b = CDirEntryDiff{
                    p: maybe_to_path.unwrap().to_path_buf(),
                    t_diff: get_t_diff_from_md(old_a.md, false),
                
                    files_here: old_a.files_here,
                    files_below: old_a.files_below,
                    dirs_here: old_a.dirs_here,
                    dirs_below: old_a.dirs_below,
                    size_here: old_a.size_here as i64,
                    size_below: old_a.size_below as i64,
                
                    files: get_file_diffs(Vec::new(), old_a.files.to_vec(),),
                    symlinks: get_file_diffs(Vec::new(), old_a.symlinks.to_vec()),
                };
                ret.diffs[ADD_DT_IDX].push(add_b);
                is_new_lookup[ADD_DT_IDX].push(true);
                rev_move_to_paths.remove(maybe_to_path.unwrap());
            }
        }

        for dr_idx in 0..curr.diffs[REM_DT_IDX].len() {
            let new_diff_path = &curr.diffs[REM_DT_IDX][dr_idx].p.clone();

            // 2. A -> B, REM: B => MOV -> REM(B)
            let maybe_from_path = rev_move_to_paths.get(new_diff_path);
            if maybe_from_path.is_some() {
                let maybe_entry_idx = path_entry_lookup.get(maybe_from_path.unwrap());
                if maybe_entry_idx.is_none() {
                    continue;
                }
                ret.move_to_paths.remove(maybe_from_path.unwrap());
                let old_a = &full_scan_entries[*maybe_entry_idx.unwrap()];
                // REM A
                curr.diffs[REM_DT_IDX][dr_idx] = CDirEntryDiff{                        
                    p: old_a.p.clone(),
                    t_diff: get_t_diff_from_md(old_a.md, true),
                
                    files_here: old_a.files_here,
                    files_below: old_a.files_below,
                    dirs_here: old_a.dirs_here,
                    dirs_below: old_a.dirs_below,
                    size_here: old_a.size_here as i64 * -1,
                    size_below: old_a.size_below as i64 * -1,
                
                    files: get_file_diffs(old_a.files.to_vec(), Vec::new()),
                    symlinks: get_file_diffs(old_a.symlinks.to_vec(), Vec::new()),
                };
                is_new_lookup[REM_DT_IDX][dr_idx] = true;
            }
            rev_move_to_paths.remove(new_diff_path);
        }
        let curr_paths_copy: HashMap<PathBuf, PathBuf> = curr.move_to_paths.clone();
        let mut remove_set: HashSet<PathBuf> = HashSet::new();
        ret.move_to_paths.clone().into_iter().for_each(|kv| {
            let old_from = kv.0;
            let old_to = kv.1;
            let maybe_new_to = curr_paths_copy.get(&old_to);
            if maybe_new_to.is_some() {
                // 3. A -> B, B -> C => A -> C
                let new_to = maybe_new_to.unwrap();
                // REMOVE NEW
                curr.move_to_paths.remove(&old_to);
                remove_set.insert(old_to);
                // UPDATE OLD
                ret.move_to_paths.insert(old_from.to_path_buf(), new_to.to_path_buf());
            }
        });
        // For A -> B -> C => A -> C, any diffs acting on B should be removed
        let mut new_curr_diffs: [Vec<CDirEntryDiff>; NUM_DT] = Default::default();
        for j in 0..curr.diffs.len() {
            for d in &curr.diffs[j] {
                if !remove_set.contains(&d.p.to_path_buf()) {
                    new_curr_diffs[j].push(d.clone());
                }
            }
        }
        curr.diffs = new_curr_diffs;

        for j in 0..NUM_DT {
            ret.diffs[j].extend(curr.diffs[j].clone());
        }
        ret.move_to_paths.extend(curr.move_to_paths.clone());
        let to_paths_iter = curr.move_to_paths.into_iter().map(|kv|{(kv.1, kv.0)});
        rev_move_to_paths.extend(to_paths_iter);

        for j in 0..NUM_DT {
            let mut idx_keys: Vec<(usize, CDirEntryDiff)> = ret.diffs[j].clone().into_iter().enumerate().collect();
            idx_keys.sort_by(|a, b| {
                let path_cmp = a.1.p.cmp(&b.1.p);
                if path_cmp == Ordering::Equal {
                    let rhs_newer = !is_new_lookup[j][a.0] && is_new_lookup[j][b.0];
                    if rhs_newer {
                        return Ordering::Less;
                    }
                    return Ordering::Greater;
                }
                return path_cmp;
            });
            ret.diffs[j] = idx_keys.iter().map(|(_, entry)| entry.to_owned() ).collect();
        }
        
        merge_sorted_vec_duplicates::<CDirEntryDiff>(&mut ret.diffs, |a: &CDirEntryDiff, b: &CDirEntryDiff| {
            return a.p.cmp(&b.p);
        }, is_new_lookup, merge_dir_diff);
    }

    return ret;
}

fn merge_dir_diff(old: &CDirEntryDiff, new: &CDirEntryDiff) -> CDirEntryDiff {
    return CDirEntryDiff{
        p: new.p.clone(),
        t_diff: TDiff{
            s_diff: old.t_diff.s_diff + new.t_diff.s_diff,
            ns_diff: old.t_diff.ns_diff + new.t_diff.ns_diff,
        },
    
        files_here: old.files_here + new.files_here,
        files_below: old.files_below + new.files_below,
        dirs_here: old.dirs_here + new.dirs_here,
        dirs_below: old.dirs_below + new.dirs_below,
        size_here: old.size_here + new.size_here,
        size_below: old.size_below + new.size_below,
    
        files: merge_file_types_diffs(&old.files, &new.files),
        symlinks: merge_file_types_diffs(&old.symlinks, &new.symlinks),
    };
}

fn merge_file_types_diffs(old: &[Vec<FileEntryDiff>; NUM_DT], new: &[Vec<FileEntryDiff>; NUM_DT]) -> [Vec<FileEntryDiff>; NUM_DT] {
    let mut ret: [Vec<FileEntryDiff>; 3] = Default::default();
    let mut is_new_lookup: [Vec::<bool>; NUM_DT] = Default::default();
    for i in 0..NUM_DT {
        is_new_lookup[i] = vec![false; old[i].len()];
        is_new_lookup[i].extend(vec![true; new[i].len()]);
        
        ret[i] = [old[i].clone(), new[i].clone()].concat();
        let mut idx_keys: Vec<(usize, &FileEntryDiff)> = ret[i].iter().enumerate().collect();
        idx_keys.sort_by(|a, b| {
            let path_cmp = a.1.bn.cmp(&b.1.bn);
            if path_cmp == Ordering::Equal {
                let rhs_newer = !is_new_lookup[i][a.0] && is_new_lookup[i][b.0];
                if rhs_newer {
                    return Ordering::Less;
                }
                return Ordering::Greater;
            }
            return path_cmp;
        });
        ret[i] = idx_keys.into_iter().map(|(_, entry)| entry.to_owned() ).collect();
    }

    merge_sorted_vec_duplicates::<FileEntryDiff>(&mut ret, |a: &FileEntryDiff, b: &FileEntryDiff| {
        return a.bn.cmp(&b.bn);
    }, is_new_lookup, merge_file_diff);

    return ret;
}

fn merge_sorted_vec_duplicates<T: Clone>(add_rem_mod_arrays: &mut [Vec::<T>; 3], cmp: fn(a: &T, b: &T) -> Ordering, is_new_lookup: [Vec::<bool>; 3], merge_elems: fn(old: &T, new: &T) -> T) {
    let mut all_arrs_zero = true;
    for i in 0..add_rem_mod_arrays.len() {
        if add_rem_mod_arrays[i].len() > 0 {
            all_arrs_zero = false;
            break;
        }
    }
    if all_arrs_zero {
        return;
    }

    // SUBEQ ITEM MATCH: if (OLD:ADD && NEW:REM) -> SKIP else PUSH(NEW:*)
    let mut ai = 0;
    let mut ri = 0;
    let mut adds_left = ai < add_rem_mod_arrays[ADD_DT_IDX].len();
    let mut rems_left = ri < add_rem_mod_arrays[REM_DT_IDX].len();
    let mut new_add_items = Vec::with_capacity(add_rem_mod_arrays[ADD_DT_IDX].len());
    let mut new_rem_items = Vec::with_capacity(add_rem_mod_arrays[REM_DT_IDX].len());
    while adds_left && rems_left {    
        let add_item = add_rem_mod_arrays[ADD_DT_IDX][ai].clone();
        let rem_item = add_rem_mod_arrays[REM_DT_IDX][ri].clone();

        let ord = cmp(&add_item, &rem_item);
        match ord {
            Ordering::Less => {
                new_add_items.push(add_item);
                ri -= 1;
            },
            Ordering::Greater => {
                new_rem_items.push(rem_item);
                ai -= 1;
            }
            Ordering::Equal => {
                let is_add_new = is_new_lookup[ADD_DT_IDX][ai];
                let is_rem_new = is_new_lookup[REM_DT_IDX][ri];
                new_add_items.push(add_item);
                if is_rem_new && !is_add_new {
                    // Revert default "add new"
                    new_add_items.pop();
                } else if is_rem_new {
                    new_rem_items.push(rem_item);
                }
            }
        }
        ai += 1;
        ri += 1;

        adds_left = ai < add_rem_mod_arrays[ADD_DT_IDX].len();
        rems_left = ri < add_rem_mod_arrays[REM_DT_IDX].len();
    }
    // Add any remaining items
    for i in ai..add_rem_mod_arrays[ADD_DT_IDX].len() {
        new_add_items.push(add_rem_mod_arrays[ADD_DT_IDX][i].clone());
    }
    for i in ri..add_rem_mod_arrays[REM_DT_IDX].len() {
        new_rem_items.push(add_rem_mod_arrays[REM_DT_IDX][i].clone());
    }
    add_rem_mod_arrays[ADD_DT_IDX] = new_add_items;
    add_rem_mod_arrays[REM_DT_IDX] = new_rem_items;
    
    // if SUBSEQ_MATCH(a, b) -> MERGE else PUSH(a)
    let mut new_mod_items: Vec<T> = Vec::with_capacity(add_rem_mod_arrays[MOD_DT_IDX].len());
    for i in 0..add_rem_mod_arrays[MOD_DT_IDX].len() {
        let a = &add_rem_mod_arrays[MOD_DT_IDX][i];
        if (i + 1) < add_rem_mod_arrays[MOD_DT_IDX].len() {
            let b = &add_rem_mod_arrays[MOD_DT_IDX][i + 1];
            let is_next_equal = cmp(&a, &b) == Ordering::Equal;
            if is_next_equal {
                new_mod_items.push(merge_elems(a, b));
                continue;
            } 
        }
        new_mod_items.push(a.clone());
    }
    add_rem_mod_arrays[MOD_DT_IDX] = new_mod_items;
}

fn merge_file_diff(old: &FileEntryDiff, new: &FileEntryDiff) -> FileEntryDiff {

    return FileEntryDiff{
        bn: new.bn.clone(),
        sz: old.sz + new.sz,
        t_diff: TDiff{
            s_diff: old.t_diff.s_diff + new.t_diff.s_diff,
            ns_diff: old.t_diff.ns_diff + new.t_diff.ns_diff,
        },
    };
}

fn get_maybe_modified_file_diff(ent_o: FileEntry, ent_n: FileEntry) -> Option<FileEntryDiff> {    
    if ent_o.md == ent_n.md {
        return None;
    }

    let t_diff_o = get_t_diff_from_md(ent_o.md, false);
    let t_diff_n = get_t_diff_from_md(ent_n.md, false);
    
    return Some(FileEntryDiff {
        bn: ent_n.bn,
        sz: ent_n.sz as i128 - ent_o.sz as i128,
        t_diff: TDiff{
            s_diff: t_diff_n.s_diff - t_diff_o.s_diff,
            ns_diff: t_diff_n.ns_diff - t_diff_o.ns_diff,
        },
    });
}