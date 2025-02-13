use std::{cmp::Ordering, collections::HashMap, ffi::OsString, fs::{DirEntry, File}, hash::{DefaultHasher, Hasher}, io::{BufReader, Seek, SeekFrom}, os::unix::ffi::OsStrExt, path::PathBuf, time::{SystemTime, UNIX_EPOCH}, usize};
use std::io::{self, Read};
use crate::{diff::{CDirEntryDiff, DiffType, FileEntryDiff, TDiff}, walk::{CDirEntry, FileEntry}};

const START_VECTOR_BYTES: u64 = 8;

pub fn get_hash_iteration_count_from_file_names(root: &std::path::PathBuf, save_file_dir: std::path::PathBuf) -> (String, i32) {
    let root_hash_str: String;
    let mut curr_iteration_count: i32 = -1;

    let mut hasher = DefaultHasher::new();
    hasher.write(root.as_os_str().as_bytes());
    root_hash_str = format!("{:x}", hasher.finish());

    let mut initial_exists = false;
    let mut path_to_initial = save_file_dir.clone();
    path_to_initial.push(format!("{}_initial", root_hash_str));
    if let Ok(exists) = std::fs::exists(&path_to_initial) {
        initial_exists = exists;
    }
    if !initial_exists {
        return (root_hash_str, curr_iteration_count)
    }
    curr_iteration_count = 0;

    let root_hash_underscore = format!("{}_", root_hash_str);
    if let Ok(entries) = std::fs::read_dir(save_file_dir) {
        for e in entries {
            let count = get_iteration_count_from_entry(&root_hash_underscore, e);
            if count > curr_iteration_count {
                curr_iteration_count = count;
            }
        }
    }
    
    return (root_hash_str, curr_iteration_count);
}

fn get_iteration_count_from_entry(root_hash_underscore: &String, e: Result<DirEntry, std::io::Error>) -> i32  {
    let ret = -1;
    
    if e.is_err() {
        return ret;
    }
    let file_name = e.unwrap().file_name();
    
    let maybe_string = file_name.as_os_str().to_str();
    if maybe_string.is_none() {
        return ret;
    }
    let file_name_str = maybe_string.unwrap();


    if file_name_str.starts_with(root_hash_underscore) {
        // Split on '_'
        let parts: Vec<&str> = file_name_str.split("_").collect();

        // Try to parse arg[1] as int
        if parts.len() < 3 {
            return ret;
        }

        let maybe_num = parts[2];
        if let Ok(num) = maybe_num.parse::<i32>() {
            return num;
        }
    }

    return ret;
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

pub fn read_diff_file(file_path: PathBuf) -> io::Result<Vec<CDirEntryDiff>> {
    let fp = File::open(&file_path)?;
    let reader = BufReader::new(fp);
    let res: Result<Vec<CDirEntryDiff>, _> = bincode::deserialize_from(reader);

    // Handle the deserialization error
    match res {
        Ok(entries) => Ok(entries),
        Err(e) => Err(io::Error::new(io::ErrorKind::InvalidData, format!("Deserialization error: {}", e))),
    }
}

pub fn get_next_chunk_from_file(f: &mut File, file_size: u64, start: u64, chunk_size: u64) -> io::Result<(Vec<CDirEntry>, u64)> {
    // 1. Read FROM START `chunk_size`
    let res = f.seek(SeekFrom::Start(start));
    if res.is_err() {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, format!("Failed to seek to `start` in file")))
    }
    let mut limit = chunk_size;
    if start + limit > file_size - 1 {
        limit = file_size - start;
    }
    let mut chunk: Vec<u8> = Vec::with_capacity(limit as usize);
    let n = f.by_ref().take(limit).read_to_end(&mut chunk)?;
    if n == 0 {
        return Ok((Vec::new(), start));
    }
    let chunk_box = chunk.into_boxed_slice();

    // 2. Deserialise the bytes until error (sometime)
    let mut ret: Vec<CDirEntry> = Vec::new();
    let mut inner_off = 0;
    loop {
        match bincode::deserialize::<CDirEntry>(&chunk_box[inner_off..]) {
            Ok(item) => {
                let size = bincode::serialized_size(&item).unwrap();
                inner_off += size as usize;
                ret.push(item);
            }
            Err(e) => {
                break;
            }
        }
    }

    // 3. IF error, the next start point is `chunk_size` - `back_off`
    let back_off = chunk_size - inner_off as u64;
    let next_off = start + chunk_size as u64 - back_off;

    Ok((ret, next_off))
}

pub fn get_chunk_entry_offsets_from_file(f: &mut File, file_size: u64, chunk_size: u64) -> io::Result<Vec<u64>> {
    let num_chunks = (file_size as f64 / chunk_size as f64).ceil() as usize;

    let mut chunk_offsets: Vec<u64> = Vec::with_capacity(num_chunks); 
    chunk_offsets.push(START_VECTOR_BYTES);
    let mut num_entries = 0;
    for i in 0..chunk_offsets.capacity() {
        let off = chunk_offsets[i];
        
        // 1. Read FROM START `chunk_size`
        let res = f.seek(SeekFrom::Start(off));
        if res.is_err() {
            break;
        }
        let mut limit = chunk_size;
        if off + limit > file_size - 1 {
            limit = file_size - off;
        }
        let mut chunk: Vec<u8> = Vec::with_capacity(limit as usize);
        let n = f.by_ref().take(limit).read_to_end(&mut chunk)?;
        if n == 0 {
            break;
        }

        // 2. Deserialise the bytes until error (sometime)
        let mut inner_off = 0;
        loop {
            match bincode::deserialize::<CDirEntry>(&chunk[inner_off..]) {
                Ok(item) => {
                    let size = bincode::serialized_size(&item).unwrap();
                    inner_off += size as usize;
                    num_entries += 1;
                }
                Err(_) => {
                    break;
                }
            }
        }

        // 3. IF error, the next start point is `chunk_size` - `back_off`
        let back_off = chunk_size - inner_off as u64;
        let next_off = off + chunk_size as u64 - back_off;
        chunk_offsets.push(next_off);
        chunk.clear();
    }

    Ok(chunk_offsets)
}

pub fn diff_saves(mut o: Vec<CDirEntry>, mut n: Vec<CDirEntry>, newest_initial_scan_time: SystemTime, diff_no: u16, min_diff_bytes: i64) -> std::vec::Vec<CDirEntryDiff> {
    let min_diff_threshold = min_diff_bytes as i64;
    let mut diffs: Vec<CDirEntryDiff> = Vec::new();
    
    // Append `old` to `new` and sort in path and INCREASING modified date order
    n.append(&mut o);
    n.sort_by(|a, b| {
        let c = a.p.cmp(&b.p);
        if c == Ordering::Equal {
            return a.md.cmp(&b.md);
        }
        return c;
    });

    // "diff" CDirEntrys, by comparing `curr` w/ `next`, if 
    let mut merge_with_prev = false;
    for i in 0..n.len() - 1 {
        let curr = &n[i];
        let mut next_p = &PathBuf::from("/");
        if i < n.len() - 1 {
            next_p = &n[i + 1].p
        }

        if *curr.p == *next_p {
            merge_with_prev = true;
        } else if merge_with_prev {
            let prev = &n[i - 1];
            let maybe_modified_dir_diff = get_maybe_modified_dir_diff(prev.clone(), curr.clone(), diff_no, min_diff_bytes);
            match maybe_modified_dir_diff {
                Some(d) => {
                    diffs.push(d)                    
                },
                None => {}
            }
            merge_with_prev = false;
        } else if !curr.md.is_none() {
            // Add/Remove
            // IF there's one, it could be OLD or NEW, how do I distinguish?
            let is_remove = curr.md.unwrap() <= newest_initial_scan_time;
            let mut diff_type = DiffType::Add;
            let mut diff_sign = 1;
            if is_remove {
                diff_type = DiffType::Remove;
                diff_sign = -1;
            }

            if curr.size_here >= min_diff_threshold {
                diffs.push(CDirEntryDiff {
                    diff_type: diff_type,
                    diff_no: diff_no,
                    
                    p: curr.p.clone(),
                    t_diff: get_t_diff_from_md(curr.md, is_remove),
                
                    files_here: curr.files_here,
                    files_below: curr.files_below,
                    dirs_here: curr.dirs_here,
                    dirs_below: curr.dirs_below,
                    size_here: curr.size_here as i64 * diff_sign,
                    size_below: curr.size_below as i64 * diff_sign,
                    memory_usage_here: curr.memory_usage_here,
                    memory_usage_below: curr.memory_usage_below,
                    
                
                    files: get_file_diffs(Vec::new(), curr.files.to_vec(), diff_no),
                });
            } 
        }
    }

    return diffs;
}

fn get_maybe_modified_dir_diff(ent_o: CDirEntry, ent_n: CDirEntry, diff_no: u16, min_diff_threshold: i64) -> Option<CDirEntryDiff> {    
    let diff_here = ent_o.dirs_here != ent_n.dirs_here || ent_o.files_here != ent_n.files_here || ent_o.size_here != ent_n.size_here;
    if !diff_here || (ent_n.size_here - ent_o.size_here).abs() < min_diff_threshold {
        return None;
    }

    let t_diff_o = get_t_diff_from_md(ent_o.md, false);
    let t_diff_n = get_t_diff_from_md(ent_n.md, false);
    
    return Some(CDirEntryDiff {
        diff_type: DiffType::Modify,
        diff_no: diff_no,
        
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
        memory_usage_here: ent_n.memory_usage_here - ent_o.memory_usage_here,
        memory_usage_below: ent_n.memory_usage_below - ent_o.memory_usage_below,
    
        files: get_file_diffs(ent_o.files.to_vec(), ent_n.files.to_vec(), diff_no),
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

fn get_file_diffs(o: Vec<FileEntry>, n: Vec<FileEntry>, diff_no: u16) -> Box<[FileEntryDiff]> {
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
                diff_no: diff_no,
                hash: [0; 32]
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
                diff_no: diff_no,      
                hash: [0; 32]             
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
        let maybe_modified_dir_diff = get_maybe_modified_file_diff(ent_o, ent_n, diff_no);
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
            diff_no: diff_no,
            hash: [0; 32],
        })
    }

    return diffs.into_boxed_slice();
}

pub fn add_dir_diffs(to: &mut Vec<CDirEntryDiff>, from: &Vec<CDirEntryDiff>) {
    to.extend_from_slice(from);
    to.sort_by(|a, b| {
        let path_cmp = a.p.cmp(&b.p);
        if path_cmp == Ordering::Equal {
            if a.diff_no <= b.diff_no {
                return Ordering::Less;
            } else {
                return Ordering::Greater;
            }
        }
        return path_cmp;
    });

    let new_len = merge_sorted_vec_duplicates::<CDirEntryDiff>(to, |a: &CDirEntryDiff, b: &CDirEntryDiff| {
        return a.p == b.p;
    }, merge_dir_diff);
    to.resize(new_len, to[0].clone());
}

fn merge_dir_diff(old: CDirEntryDiff, new: CDirEntryDiff) -> CDirEntryDiff {
    if new.diff_type != DiffType::Modify {
        return new;
    }

    return CDirEntryDiff{
        p: new.p,
        t_diff: TDiff{
            s_diff: old.t_diff.s_diff + new.t_diff.s_diff,
            ns_diff: old.t_diff.ns_diff + new.t_diff.ns_diff,
        },
        diff_no: new.diff_no,
    
        files_here: old.files_here + new.files_here,
        files_below: old.files_below + new.files_below,
        dirs_here: old.dirs_here + new.dirs_here,
        dirs_below: old.dirs_below + new.dirs_below,
        size_here: old.size_here + new.size_here,
        size_below: old.size_below + new.size_below,
        memory_usage_here: old.memory_usage_here + new.memory_usage_here,
        memory_usage_below: old.memory_usage_below + new.memory_usage_below,
        
        diff_type: new.diff_type,
    
        files: merge_file_diffs(old.files, new.files),
    }
}

fn merge_file_diffs(old: Box<[FileEntryDiff]>, new: Box<[FileEntryDiff]>) -> Box<[FileEntryDiff]> {
    let mut ret = [old, new].concat();
    ret.sort_by(|a, b| {
        let initial = a.bn.cmp(&b.bn);
        if initial == Ordering::Equal {
            if a.diff_no <= b.diff_no {
                return Ordering::Less
            }
            return Ordering::Greater
        }
        return initial;
    });

    let new_len = merge_sorted_vec_duplicates::<FileEntryDiff>(&mut ret, |a: &FileEntryDiff, b: &FileEntryDiff| {
        return a.bn == b.bn;
    }, merge_file_diff);

    return ret[0..new_len].to_vec().into_boxed_slice();
}

fn merge_sorted_vec_duplicates<T: Clone>(arr: &mut Vec::<T>, is_dup: fn(a: &T, b: &T) -> bool, merge_elems: fn(old: T, new: T) -> T) -> usize {
    if arr.len() == 0 {
        return 0;
    }
    
    let mut assign_idx = 1;
    let mut look_idx = 1;
    let mut assign_at = arr[0].clone();
    while look_idx < arr.len() {
        let look_at = arr[look_idx].clone();
        
        if is_dup(&assign_at, &look_at) {
            // Merge two elements INTO the assign idx
            assign_idx -= 1;
            arr[assign_idx] = merge_elems(assign_at, look_at);
        } else {
            // `skipped_elems` -> element at `assign_idx` was merged into a previous element and should be overriden
            let skipped_elems = look_idx > assign_idx;
            if skipped_elems {
                arr[assign_idx] = arr[look_idx].clone();
            }
        }
        assign_at = arr[assign_idx].clone();
        
        assign_idx += 1;
        look_idx += 1;
    }
    
    // `assign_idx` is the new array length, anything after it can be ignored
    return assign_idx;
}

fn merge_file_diff(old: FileEntryDiff, new: FileEntryDiff) -> FileEntryDiff {
    if new.diff_type != DiffType::Modify {
        return new;
    }

    return FileEntryDiff{
        bn: new.bn,
        sz: old.sz + new.sz,
        t_diff: TDiff{
            s_diff: old.t_diff.s_diff + new.t_diff.s_diff,
            ns_diff: old.t_diff.ns_diff + new.t_diff.ns_diff,
        },
        diff_no: new.diff_no,
        diff_type: new.diff_type,
        hash: [0; 32]
    }
}

fn get_maybe_modified_file_diff(ent_o: FileEntry, ent_n: FileEntry, diff_no: u16) -> Option<FileEntryDiff> {    
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
        diff_no: diff_no,
        hash: [0; 32]
    });
}