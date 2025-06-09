use std::{cmp::Ordering, collections::{HashMap, HashSet}, ffi::OsString, fs::File, hash::{DefaultHasher, Hasher}, io::BufReader, os::unix::ffi::OsStrExt, path::PathBuf, time::{SystemTime, UNIX_EPOCH}, usize};
use std::io;
use crate::{diff::{self, get_entry_from_dir_diff, CDirEntryDiff, DiffEntry, DiffFile, DiffType, FileEntryDiff, TDiff}, walk::{CDirEntry, FileEntry}};

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
        diffs: vec![],
        move_to_paths: HashMap::new(),
    };

    let mut oi = 0;
    let mut ni = 0;
    let mut old = &o[0];
    let mut new = &o[0];
    let mut add_rem_set: HashSet<PathBuf> = HashSet::from_iter(combined_diffs.clone().diffs.into_iter().filter(|d| {d.diff_type == DiffType::Remove || d.diff_type == DiffType::Add}).map(|d|{d.p}));
    let mut remove_hash_idxs: HashMap<[u8; 16], usize> = HashMap::new();
    let mut add_hash_idxs: HashMap<[u8; 16], usize> = HashMap::new();
    let mut moved_paths: Vec<PathBuf> = combined_diffs.move_to_paths.clone().into_keys().collect();
    while oi < o.len() || ni < n.len() {
        let old_left = oi < o.len();
        if old_left {
            old = &o[oi];
        }
        let new_left = ni < n.len();
        if new_left {
            new = &n[ni];
        }

        let mut diff_type: DiffType = DiffType::Add; // -> new_left        
        if old_left && new_left {
            let old_new_cmp = old.p.cmp(&new.p);
            match old_new_cmp {
                Ordering::Equal => {
                    diff_type = DiffType::Modify;
                },
                Ordering::Less => {
                    // old item removed, NEXT old item *might* match CURRENT new
                    diff_type = DiffType::Remove;
                    ni -= 1;
                },
                Ordering::Greater => {   
                    // new item added, CURRENT old item *might* match NEXT new 
                    diff_type = DiffType::Add;
                    oi -= 1;
                }
            }
        } else if old_left {
            diff_type = DiffType::Remove;
        }

        let diff_passes_threshold;
        match diff_type {
            DiffType::Add => {
                add_rem_set.insert(new.p.clone());
                let maybe_move_match = remove_hash_idxs.get(&new.md5);
                if maybe_move_match.is_some() {
                    let update_idx = maybe_move_match.unwrap();
                    let old_path = &new_entry.diffs[*update_idx].p.clone();
                    new_entry.diffs[*update_idx].diff_type = diff::DiffType::Ignore;
                    if moved_paths.iter().find(|el|{ old_path.starts_with(el) }).is_none() {
                        new_entry.diffs[*update_idx] = diff::CDirEntryDiff{
                            p: old_path.to_path_buf(),
                            t_diff: TDiff { s_diff: 0, ns_diff: 0 },
                        
                            files_here: 0,
                            files_below: 0,
                            dirs_here: 0,
                            dirs_below: 0,
                            size_here: 0,
                            size_below: 0,
                            
                            diff_type: diff::DiffType::MoveDir,
                            files: vec![],
                            symlinks: vec![],
                        };
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
                add_hash_idxs.insert(new.md5, new_entry.diffs.len());
                new_entry.diffs.push(CDirEntryDiff{
                    diff_type: DiffType::Add,
                    
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
            DiffType::Remove => {
                add_rem_set.insert(old.p.clone());
                let maybe_move_match = add_hash_idxs.get(&old.md5);
                if maybe_move_match.is_some() {
                    let update_idx = maybe_move_match.unwrap();
                    let new_path = new_entry.diffs[*update_idx].p.clone();
                    new_entry.diffs[*update_idx].diff_type = diff::DiffType::Ignore;
                    if moved_paths.iter().find(|el|{ old.p.starts_with(el) }).is_none() {
                        new_entry.diffs[*update_idx] = diff::CDirEntryDiff{
                            p: old.p.clone(),
                            t_diff: TDiff { s_diff: 0, ns_diff: 0 },
                        
                            files_here: 0,
                            files_below: 0,
                            dirs_here: 0,
                            dirs_below: 0,
                            size_here: 0,
                            size_below: 0,
                            
                            diff_type: diff::DiffType::MoveDir,
                            files: vec![],
                            symlinks: vec![],
                        };
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
                remove_hash_idxs.insert(old.md5, new_entry.diffs.len());
                new_entry.diffs.push(CDirEntryDiff{
                    diff_type: DiffType::Remove,
                    
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
            DiffType::Modify => {
                let maybe_modified_dir_diff = get_maybe_modified_dir_diff(old.clone(), new.clone());
                match maybe_modified_dir_diff {
                    Some(d) => {
                        diff_passes_threshold = d.size_here.abs() >= min_diff_bytes as i64;
                        if diff_passes_threshold {
                            new_entry.diffs.push(d);
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
    new_entry.diffs = new_entry.diffs.into_iter().filter(|it| {
        return it.diff_type != DiffType::Ignore && (it.diff_type == DiffType::MoveDir || (it.size_here + it.size_below).abs() >= min_diff_bytes as i64)}).collect();

    let changed_caching = original_file.has_merged_diff != cache_merged_diffs;
    if changed_caching {
        if cache_merged_diffs {
            // 0 index should be cached entry...
            let mut new_entries = vec![combined_diffs];
            new_entries.append(&mut original_file.entries);
            original_file.entries = new_entries;
            
            let mut new_timestamps = vec![SystemTime::UNIX_EPOCH];
            new_timestamps.append(&mut original_file.timestamps);
            original_file.timestamps = new_timestamps;
        } else {
            // Remove 0 index as cache entry
            original_file.entries = original_file.entries.into_iter().skip(1).collect();
            original_file.timestamps = original_file.timestamps.into_iter().skip(1).collect();
        }
        original_file.has_merged_diff = cache_merged_diffs;
    }
    if new_entry.diffs.len() > 0 {
        original_file.entries.push(new_entry);
        original_file.timestamps.push(SystemTime::now());
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
        diff_type: DiffType::Modify,
        
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

fn get_file_diffs(o: Vec<FileEntry>, n: Vec<FileEntry>) -> Vec<FileEntryDiff> {
    let box_size: usize = if n.len() > o.len() {
        n.len()
    } else {
        o.len()
    };
    let mut diffs: Vec<FileEntryDiff> = Vec::with_capacity(box_size);

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
            diffs.push(FileEntryDiff {
                diff_type: DiffType::Add,

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
            diffs.push(FileEntryDiff {
                diff_type: DiffType::Add,

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
            Some(d) => {diffs.push(d)},
            None => {}
        }

        oidx += 1;
        nidx += 1;
    }

    // Add these entries as 'Removed'
    for tup in maybe_removed_path_idxs {
        let ent = &o[tup.1];
        diffs.push(FileEntryDiff {
            diff_type: DiffType::Remove,
            
            bn: ent.bn.clone(),
            t_diff: get_t_diff_from_md(ent.md, true),
            sz: ent.sz as i128 * -1,
        })
    }

    return diffs;
}

pub fn add_dir_diffs(df: &DiffFile, full_scan_entries: &Vec<CDirEntry>, start_idx: usize, end_idx: usize) -> DiffEntry {
    let mut ret = DiffEntry {
        diffs: vec![],
        move_to_paths: HashMap::new(),
    };
    
    let path_entry_lookup: HashMap<PathBuf, usize> = full_scan_entries.clone().into_iter().enumerate().map(|p|{(p.1.p, p.0)}).collect();
    let mut rev_move_to_paths = HashMap::new();
    for i in start_idx..(end_idx + 1) {
        let mut curr = df.entries[i].clone();
        let mut is_new_arr = vec![false; ret.diffs.len()];
        is_new_arr.extend(vec![true; curr.diffs.len()]);
        
        let to_paths_dup = ret.move_to_paths.clone();

        for d_idx in 0..curr.diffs.len() {
            let new_diff = &curr.diffs[d_idx];
            match new_diff.diff_type {
                DiffType::Add => 'add: {
                    // 1. A -> B, ADD: A => MOV -> MOD(A), ADD(B) (dup of A w/ different path)
                    let maybe_to_path = to_paths_dup.get(&new_diff.p);
                    if maybe_to_path.is_some() {
                        let maybe_entry_idx = path_entry_lookup.get(&new_diff.p);
                        if maybe_entry_idx.is_none() {
                            break 'add;
                        }
                        let old_a = &full_scan_entries[*maybe_entry_idx.unwrap()];
                        ret.move_to_paths.remove(&new_diff.p);

                        // MOD A
                        let maybe_a_diff = get_maybe_modified_dir_diff(old_a.clone(), get_entry_from_dir_diff(new_diff.clone()));
                        if maybe_a_diff.is_some() {
                            curr.diffs[d_idx] = maybe_a_diff.unwrap();
                        }
                        // ADD B
                        let add_b = CDirEntryDiff{
                            diff_type: DiffType::Add,
                            
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
                        ret.diffs.push(add_b);
                        is_new_arr.push(true);
                        rev_move_to_paths.remove(maybe_to_path.unwrap());
                    }
                },
                DiffType::Remove => 'rem: {
                    // 2. A -> B, REM: B => MOV -> REM(B)
                    let maybe_from_path = rev_move_to_paths.get(&new_diff.p);
                    if maybe_from_path.is_some() {
                        let maybe_entry_idx = path_entry_lookup.get(maybe_from_path.unwrap());
                        if maybe_entry_idx.is_none() {
                            break 'rem;
                        }
                        ret.move_to_paths.remove(maybe_from_path.unwrap());
                        let old_a = &full_scan_entries[*maybe_entry_idx.unwrap()];
                        // REM A
                        ret.diffs.push(CDirEntryDiff{
                            diff_type: DiffType::Remove,
                            
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
                        });
                        is_new_arr.push(true);
                    }
                    rev_move_to_paths.remove(&new_diff.p);
                },
                _ => {}
            }
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
        let mut new_curr_diffs = Vec::with_capacity(curr.diffs.len());
        for j in 0..curr.diffs.len() {
            if !remove_set.contains(&curr.diffs[j].p.to_path_buf()) {
                new_curr_diffs.push(curr.diffs[j].clone());
            }
        }
        curr.diffs = new_curr_diffs;

        ret.diffs.extend(curr.diffs);
        ret.move_to_paths.extend(curr.move_to_paths.clone());
        let to_paths_iter = curr.move_to_paths.into_iter().map(|kv|{(kv.1, kv.0)});
        rev_move_to_paths.extend(to_paths_iter);

        let mut idx_keys: Vec<(usize, CDirEntryDiff)> = ret.diffs.into_iter().enumerate().collect();
        idx_keys.sort_by(|a, b| {
            let path_cmp = a.1.p.cmp(&b.1.p);
            if path_cmp == Ordering::Equal {
                let rhs_newer = !is_new_arr[a.0] && is_new_arr[b.0];
                if rhs_newer {
                    return Ordering::Less;
                }
                return Ordering::Greater;
            }
            return path_cmp;
        });
    
        ret.diffs = idx_keys.iter().map(|(_, entry)| entry.to_owned() ).collect();
        let new_len = merge_sorted_vec_duplicates::<CDirEntryDiff>(&mut ret.diffs, |a: &CDirEntryDiff, b: &CDirEntryDiff| {
            return a.p == b.p;
        }, merge_dir_diff);
        if ret.diffs.len() > 0 {
            ret.diffs.resize(new_len, ret.diffs[0].clone());
        }
    }

    return ret;
}

fn merge_dir_diff(old: CDirEntryDiff, new: CDirEntryDiff) -> Option<CDirEntryDiff> {
    if (old.diff_type == DiffType::Remove && new.diff_type == DiffType::Add) || 
       (old.diff_type == DiffType::Add && new.diff_type == DiffType::Remove) {
        if diffs_match_except_time(&old, &new) {
            if new.diff_type == DiffType::Add {
                return Some(new);
            }
            return None;
        }
    } else if new.diff_type != DiffType::Modify {
        return Some(new);
    }

    return Some(CDirEntryDiff{
        p: new.p,
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
        
        diff_type: new.diff_type,
    
        files: merge_file_diffs(old.files, new.files),
        symlinks: merge_file_diffs(old.symlinks, new.symlinks),
    });
}

fn diffs_match_except_time(old: &CDirEntryDiff, new: &CDirEntryDiff) -> bool {
    return old.p == new.p 
    && (old.files_here + old.files_below) == (new.files_here + new.files_below)
    && (old.dirs_here + old.dirs_below) == (new.dirs_here + new.dirs_below)
    && (old.size_here + old.size_below) + (new.size_here + new.size_below) == 0
}

fn merge_file_diffs(old: Vec<FileEntryDiff>, new: Vec<FileEntryDiff>) -> Vec<FileEntryDiff> {
    let mut is_new_arr = vec![false; old.len()];
    is_new_arr.extend(vec![true; new.len()]);
    
    let mut ret = [old, new].concat();
    
    let mut idx_keys: Vec<(usize, &FileEntryDiff)> = ret.iter().enumerate().collect();
    idx_keys.sort_by(|a, b| {
        let path_cmp = a.1.bn.cmp(&b.1.bn);
        if path_cmp == Ordering::Equal {
            let rhs_newer = !is_new_arr[a.0] && is_new_arr[b.0];
            if rhs_newer {
                return Ordering::Less;
            }
            return Ordering::Greater;
        }
        return path_cmp;
    });

    ret = idx_keys.into_iter().map(|(_, entry)| entry.to_owned() ).collect();
    let new_len = merge_sorted_vec_duplicates::<FileEntryDiff>(&mut ret, |a: &FileEntryDiff, b: &FileEntryDiff| {
        return a.bn == b.bn;
    }, merge_file_diff);
    if ret.len() > 0 {
        ret.resize(new_len, ret[0].clone());
    }

    return ret;
}

fn merge_sorted_vec_duplicates<T: Clone>(arr: &mut Vec::<T>, is_dup: fn(a: &T, b: &T) -> bool, merge_elems: fn(old: T, new: T) -> Option<T>) -> usize {
    if arr.len() == 0 {
        return 0;
    }
    
    let mut assign_idx = 0;
    let mut look_idx = 0;
    while look_idx < arr.len() {
        let look_at = arr[look_idx].clone();
        if (look_idx + 1) < arr.len() && is_dup(&look_at, &arr[look_idx + 1]) {
            let next = arr[look_idx + 1].clone();
            // Merge two elements INTO the assign idx
            let maybe_elem = merge_elems(look_at, next);
            if maybe_elem.is_some() {
                arr[assign_idx] = maybe_elem.unwrap();
            }
            look_idx += 1;
            assign_idx -= 1;
        } else {
            // `skipped_elems` -> element at `assign_idx` was merged into a previous element and should be overriden
            let skipped_elems = look_idx > assign_idx;
            if skipped_elems {
                arr[assign_idx] = arr[look_idx].clone();
            }
        }
        
        assign_idx += 1;
        look_idx += 1;
    }
    
    // `assign_idx` is the new array length, anything after it can be ignored
    return assign_idx;
}

fn merge_file_diff(old: FileEntryDiff, new: FileEntryDiff) -> Option<FileEntryDiff> {
    if (old.diff_type == DiffType::Remove && new.diff_type == DiffType::Add) ||
       (old.diff_type == DiffType::Add && new.diff_type == DiffType::Remove) {
        if file_diffs_match_except_time(&old, &new) {
            if new.diff_type == DiffType::Add {
                return Some(new);
            }
            return None
        }
    } else if new.diff_type != DiffType::Modify {
        return Some(new);
    }

    return Some(FileEntryDiff{
        bn: new.bn,
        sz: old.sz + new.sz,
        t_diff: TDiff{
            s_diff: old.t_diff.s_diff + new.t_diff.s_diff,
            ns_diff: old.t_diff.ns_diff + new.t_diff.ns_diff,
        },
        diff_type: new.diff_type,
    });
}

fn file_diffs_match_except_time(old: &FileEntryDiff, new: &FileEntryDiff) -> bool {
    return old.bn == new.bn
    && old.sz == new.sz
}

fn get_maybe_modified_file_diff(ent_o: FileEntry, ent_n: FileEntry) -> Option<FileEntryDiff> {    
    if ent_o.md == ent_n.md {
        return None;
    }

    let t_diff_o = get_t_diff_from_md(ent_o.md, false);
    let t_diff_n = get_t_diff_from_md(ent_n.md, false);
    
    return Some(FileEntryDiff {
        diff_type: DiffType::Modify,
        
        bn: ent_n.bn,
        sz: ent_n.sz as i128 - ent_o.sz as i128,
        t_diff: TDiff{
            s_diff: t_diff_n.s_diff - t_diff_o.s_diff,
            ns_diff: t_diff_n.ns_diff - t_diff_o.ns_diff,
        },
    });
}