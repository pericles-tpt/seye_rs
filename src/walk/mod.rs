extern crate queues;
use std::{ffi::OsString, fs::DirEntry, mem, os::unix::fs::MetadataExt, time::SystemTime};
use std::{collections::{HashMap, HashSet}, fs::{symlink_metadata, Metadata}, path::PathBuf, time::Duration};
use serde::{Deserialize, Deserializer, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct FileEntry {
    pub bn: OsString,
    pub sz: u64,
    pub md: Option<SystemTime>,
    pub hash: [u8; 32],
}
impl Default for FileEntry {
    fn default() -> Self {
        FileEntry {
            bn: OsString::new(),
            sz: 0,
            md: None,
            hash: [0; 32],
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct CDirEntry {
    pub p: PathBuf,
    pub md: Option<SystemTime>,

    pub files_here: usize,
    pub files_below: usize,
    pub dirs_here: usize,
    pub dirs_below: usize,
    pub size_here: i64,
    pub size_below: i64,
    pub memory_usage_here: usize,
    pub memory_usage_below: usize,

    #[serde(deserialize_with = "deserialize_boxed_slice")]
    pub files: Box<[FileEntry]>,
}

fn deserialize_boxed_slice<'de, D>(deserializer: D) -> Result<Box<[FileEntry]>, D::Error>
where
    D: Deserializer<'de>,
{
    // Deserialize into a Vec<FileEntry>
    let vec: Vec<FileEntry> = Vec::deserialize(deserializer)?;
    // Convert Vec<FileEntry> into Box<[FileEntry]>
    Ok(vec.into_boxed_slice())
}

// TODO: Look into this, splitting CDireEntry by its 64B numeric properties and variable properties
//       could improve caching performance for accessing sizing data
// #[derive(Serialize, Deserialize, Debug, Clone)]
// pub struct CDirEntrySz {
//     pub files_here: usize,
//     pub files_below: usize,
//     pub dirs_here: usize,
//     pub dirs_below: usize,
//     pub size_here: i64,
//     pub size_below: i64,
//     pub memory_usage_here: usize,
//     pub memory_usage_below: usize,
// }
// #[derive(Serialize, Deserialize, Debug, Clone)]
// pub struct CDirEntryOther {
//     pub p: PathBuf,
//     pub md: Option<SystemTime>,

//     #[serde(deserialize_with = "deserialize_boxed_slice")]
//     pub files: Box<[FileEntry]>,
// }


lazy_static! {
    static ref IGNORE_LIST: HashSet<&'static std::ffi::OsStr> = {
        let mut set = HashSet::new();
        set.insert(std::ffi::OsStr::new(".DS_Store"));
        set
    };
}

// NOTE: `memory_usage` calculations are up to 10% LOWER than observed memory usage. This offset is applied to `memory_usage` props to provide more useful memory usage 
//       information to the user
const MEMORY_OFF_FACTOR: f64 = 1.1;

pub fn walk_until_end(root: std::path::PathBuf, parent_map: &mut HashMap<std::path::PathBuf, usize>, skip_set: &mut HashSet<PathBuf>) -> std::vec::Vec<CDirEntry> {
    let mut df: std::vec::Vec<CDirEntry> = Vec::new();
    let mut v: Vec<std::path::PathBuf> = Vec::new();

    let mut total_time_stat = Duration::new(0, 0);
    let mut sc = 0;
    let mut total_time_readdir = Duration::new(0, 0);
    let mut rdc = 0;

    // Stage 1. Traverse file tree and add to list
    // Push root onto stack
    let _ = v.push(root);
    let mut idx:i64 = -1;
    loop {
        idx += 1;
        if idx as usize >= v.len() {
            break;
        }
        let mp = &v[idx as usize];

        if skip_set.contains(mp) {
            continue;
        }

        let bef1 = std::time::Instant::now();
        let rd = std::fs::read_dir(mp);
        let trd = bef1.elapsed();
        total_time_readdir += trd;
        rdc += 1;

        if rd.is_err() {
            // TODO: Handle error
            continue;
        }
        let entries: Vec<Result<DirEntry, std::io::Error>> = rd.unwrap().collect();

        let bef2 = std::time::Instant::now();
        let maybe_md = symlink_metadata(mp);
        let ts = bef2.elapsed();
        total_time_stat += ts;
        sc += 1;
        if maybe_md.is_err() {
            // TODO: Handle error
            continue;
        }
        let md = maybe_md.unwrap();
        let curr_idx = insert_dir_entry(&md, &mp, &mut df, parent_map);

        let mut file_entries: Vec<FileEntry> = Vec::with_capacity(entries.len());
        for ent in entries {
            if ent.is_err() {
                // TODO: Handle error
                continue;
            }
            let val = ent.unwrap();

            let p = &val.path();
            let maybe_basename = p.file_name();
            if maybe_basename.is_none() {
                continue;
            }
            let basename = maybe_basename.unwrap();
            if IGNORE_LIST.contains(basename) {
                continue;
            }

            let maybe_ft = val.file_type();
            if maybe_ft.is_err() {
                // TODO: Error
                continue;
            }
            let ft = maybe_ft.unwrap();

            if ft.is_symlink() {
                continue;
            }
            
            if ft.is_dir() {
                let _ = v.push(p.to_path_buf());
                continue 
            }

            let bef2 = std::time::Instant::now();
            let maybe_fmd = symlink_metadata(p);
            let ts = bef2.elapsed();
            total_time_stat += ts;
            sc += 1;
            if maybe_fmd.is_err() {
                // TODO: Handle error
                continue;
            }
            let fmd = maybe_fmd.unwrap();

            let basename_string = basename.to_os_string();
            let basename_string_len = basename_string.len();
            insert_file_entry(&fmd, basename_string, &mut file_entries);

            df[curr_idx].files_here += 1;
            df[curr_idx].size_here += fmd.size() as i64;

            let file_entry_size = size_of::<FileEntry>() + basename_string_len;
            df[curr_idx].memory_usage_here += file_entry_size;
        }        
        df[curr_idx].files = file_entries.into_boxed_slice();
    }
    
    return df;
}

pub fn walk_collect_until_limit(some: &mut Vec<std::path::PathBuf>, skip_set: &HashSet<PathBuf>, other_entries: &mut Vec<CDirEntry>, thread_readdir_limit: usize) -> std::io::Result<Vec<PathBuf>> {
    let mut dIdx = 0;
    let mut fIdx = 0;
    
    let mut readdir_limit = thread_readdir_limit;
    if readdir_limit < some.len() {
        readdir_limit = some.len();
    }
    
    let mut dir_q: Vec<PathBuf> = Vec::with_capacity(readdir_limit);
    dir_q.append(some);

    let mut pm = HashMap::new();
    while (dIdx + fIdx) < readdir_limit && dIdx < dir_q.len() {
        let rd = std::fs::read_dir(&dir_q[dIdx]);
        if rd.is_err() {
            return Err(std::io::Error::new(std::io::ErrorKind::Other, format!("{:?}", rd.err())));
        }

        let maybe_md = symlink_metadata(&dir_q[dIdx]);
        if maybe_md.is_err() {
            return Err(std::io::Error::new(std::io::ErrorKind::Other, format!("{:?}", maybe_md.err())));
        }
        let md = maybe_md.unwrap();

        let curr_idx = insert_dir_entry(&md, &dir_q[dIdx], other_entries, &mut pm);
        let entries: Vec<Result<DirEntry, std::io::Error>> = rd.unwrap().collect();
        let mut file_entries: Vec<FileEntry> = Vec::with_capacity(entries.len());
        for ent in entries {
            if ent.is_err() {
                continue;
            }
            let val = ent.unwrap();
            // NOTE: This was commented out as it has a BIG impact on performance
            // if skip_set.contains(&val.path()) {
            //     continue;
            // }
    
            let filename = val.file_name();
            if IGNORE_LIST.contains(filename.as_os_str()) {
                continue 
            }
            
            let maybe_ft = val.file_type();
            if maybe_ft.is_err() {
                continue;
            }
            let ft = maybe_ft.unwrap();
            if !ft.is_file() {
                if ft.is_dir() {
                    dir_q.push(val.path());
                }
                continue;
            }
    
            let maybe_fmd = symlink_metadata(val.path());
            if maybe_fmd.is_err() {
                // TODO: Handle error
                continue;
            }
            let fmd = maybe_fmd.unwrap();

            fIdx += 1;
    
            let filename_len = filename.len();
            insert_file_entry(&fmd, filename, &mut file_entries);
    
            other_entries[curr_idx].files_here += 1;
            other_entries[curr_idx].size_here += fmd.size() as i64;
    
            let file_entry_size = size_of::<FileEntry>() + filename_len;
            other_entries[curr_idx].memory_usage_here += file_entry_size;
        }        
        other_entries[curr_idx].files = file_entries.into_boxed_slice();

        dIdx += 1;
    }

    return Ok(dir_q.drain(dIdx..).collect());
}

pub fn walk_search_until_limit(target: &String, some: &mut Vec<std::path::PathBuf>, skip_set: &HashSet<PathBuf>, other_entries: &mut Vec<String>, thread_readdir_limit: usize, search_hidden: bool) -> std::io::Result<Vec<PathBuf>> {
    let mut readdir_limit = thread_readdir_limit;
    if readdir_limit < some.len() {
        readdir_limit = some.len();
    }
    
    let mut dir_q: Vec<PathBuf> = Vec::with_capacity(readdir_limit);
    dir_q.append(some);
    
    let mut dIdx = 0;
    let mut fIdx = 0;
    while (fIdx + dIdx) < readdir_limit && dIdx < dir_q.len() {
        let rd = std::fs::read_dir(&dir_q[dIdx]);
        if rd.is_err() {
            // TODO: Handle error
            return Err(std::io::Error::new(std::io::ErrorKind::Other, format!("{:?}", rd.err())));
        }

        let bn = dir_q[dIdx].file_name().unwrap().to_str().unwrap();
        if bn.contains(target) {
            let p = dir_q[dIdx].as_path().as_os_str().to_str().unwrap();
            other_entries.push(format!("{}/", p));
        }

        let entries: Vec<Result<DirEntry, std::io::Error>> = rd.unwrap().collect();
        for ent in entries {
            if ent.is_err() {
                continue;
            }
            let val = ent.unwrap();
            // NOTE: This was commented out as it has a BIG impact on performance
            // if skip_set.contains(&val.path()) {
            //     continue;
            // }

            // FILTER
            let maybe_ft = val.file_type();
            if maybe_ft.is_err() {
                continue;
            }

            let ft = maybe_ft.unwrap();
            if !ft.is_dir() && !ft.is_file() {
                continue;
            }

            let filename = val.file_name();
            let bn: &str = filename.to_str().unwrap();
            if !search_hidden && bn.starts_with(".") {
                continue;
            }

            // let p_contains_substr = val.path().to_str().unwrap().contains(target);
            if ft.is_dir() {
                dir_q.push(val.path());
                continue;
            }
            
            fIdx += 1;
            if bn.contains(target) {
                other_entries.push(val.path().into_os_string().into_string().unwrap());
            }
        }        

        dIdx += 1;
    }

    return Ok(dir_q.drain(dIdx..).collect());
}

fn insert_file_entry(md: &Metadata, bn: OsString, dest: &mut Vec<FileEntry>) -> usize {
    let t = match md.modified() {
        Ok(st) => {Some(st)}
        Err(_) => None
    };
    let e = FileEntry{
        bn: bn,
        sz: md.len(),
        md: t,
        hash: [0; 32]
    };
    dest.push(e);
    return dest.len() - 1;
}

fn insert_dir_entry(md: &Metadata, p: &PathBuf, all_dirs: &mut Vec<CDirEntry>, path_idx_map: &mut HashMap<PathBuf, usize>) -> usize {
    let t = match md.modified() {
        Ok(st) => {Some(st)}
        Err(_) => None
    };
    let pb = p.to_path_buf();
    let e = CDirEntry{
        p: pb.clone(),
        md: t,

        files_here: 0,
        files_below: 0,
        dirs_here: 0,
        dirs_below: 0,
        size_here: 0,
        size_below: 0,
        memory_usage_here: 0,
        memory_usage_below: 0,

        files: Box::new([FileEntry::default()]),
    };
    all_dirs.push(e);
    path_idx_map.insert(pb, all_dirs.len() - 1);
    return all_dirs.len() - 1;                                           
}

#[cfg(test)]
mod tests {
    use std::{collections::{HashMap, HashSet}, str::FromStr};

    use crate::utility::get_cwd;

    use super::{walk_until_end};

    #[test]
    fn one_root_file_iter() {
        let wd = get_cwd();
        let path = std::path::PathBuf::from_str(format!("{}/tests/test_dir/b", wd.display()).as_str());
        match path {
            Ok(p) => {
                let mut pm = HashMap::new();
                let res = walk_until_end(p.to_path_buf(), &mut pm, &mut HashSet::new());
                assert_eq!(res.len(), 1);

                assert_eq!(res[0].p, p);
                assert_eq!(res[0].files_here, 1);
                assert_eq!(res[0].files_below, 0);
                assert_eq!(res[0].dirs_here, 0);
                assert_eq!(res[0].dirs_below, 0);
                assert_eq!(res[0].size_here, 4);
                assert_eq!(res[0].size_below, 0);
            }
            Err(e) => {
                panic!("failed to get path buf: {}", e)
            }
        }
    }

    #[test]
    fn one_root_folder_iter() {
        let wd = get_cwd();
        
        let path = std::path::PathBuf::from_str(format!("{}/tests/test_dir/c", wd.display()).as_str());
        match path {
            Ok(p) => {
                let mut pm = HashMap::new();
                let res = walk_until_end(p.to_path_buf(), &mut pm,&mut HashSet::new());
                assert_eq!(res.len(), 2);

                assert_eq!(res[0].p, p);
                assert_eq!(res[0].files_here, 0);
                assert_eq!(res[0].files_below, 0);
                assert_eq!(res[0].dirs_here, 1);
                assert_eq!(res[0].dirs_below, 0);
                assert_eq!(res[0].size_here, 0);
                assert_eq!(res[0].size_below, 0);
                
                let fp = p.join("./d");
                assert_eq!(res[1].p, fp);
                assert_eq!(res[1].files_here, 0);
                assert_eq!(res[1].files_below, 0);
                assert_eq!(res[1].dirs_here, 0);
                assert_eq!(res[1].dirs_below, 0);
                assert_eq!(res[1].size_here, 0);
                assert_eq!(res[1].size_below, 0);
            }
            Err(e) => {
                panic!("failed to get path buf: {}", e)
            }
        }
    }

    #[test]
    fn dirs_files_below_iter() {
        let wd = get_cwd();
        
        let path = std::path::PathBuf::from_str(format!("{}/tests/test_dir/a/e", wd.display()).as_str());
        match path {
            Ok(p) => {
                let mut pm = HashMap::new();
                let res = walk_until_end(p.to_path_buf(), &mut pm, &mut HashSet::new());
                assert_eq!(res.len(), 3);

                assert_eq!(res[0].p, p);
                assert_eq!(res[0].files_here, 1);
                assert_eq!(res[0].files_below, 1);
                assert_eq!(res[0].dirs_here, 1);
                assert_eq!(res[0].dirs_below, 1);
                assert_eq!(res[0].size_here, 0);
                assert_eq!(res[0].size_below, 3);
            }
            Err(e) => {
                panic!("failed to get path buf: {}", e)
            }
        }
    }

    #[test]
    fn all_dirs_files_below_iter() {
        let wd = get_cwd();

        let path = std::path::PathBuf::from_str(format!("{}/tests/test_dir", wd.display()).as_str());
        match path {
            Ok(p) => {
                let mut pm = HashMap::new();
                let res = walk_until_end(p.to_path_buf(), &mut pm,&mut HashSet::new());
                assert_eq!(res.len(), 8);

                assert_eq!(res[0].p, p);
                assert_eq!(res[0].files_here, 1);
                assert_eq!(res[0].files_below, 4);
                assert_eq!(res[0].dirs_here, 3);
                assert_eq!(res[0].dirs_below, 4);
                assert_eq!(res[0].size_here, 12);
                assert_eq!(res[0].size_below, 8);
            }
            Err(e) => {
                panic!("failed to get path buf: {}", e)
            }
        }
    }
}