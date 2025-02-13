extern crate queues;
use std::{ffi::{OsStr, OsString}, fs::DirEntry, mem, os::unix::fs::MetadataExt, time::SystemTime};
use std::{collections::{HashMap, HashSet}, fs::{symlink_metadata, Metadata}, path::PathBuf, time::Duration};
use serde::{Deserialize, Deserializer, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct FileEntry {
    pub bn: OsString,
    pub sz: u64,
    pub md: Option<SystemTime>,
}
impl Default for FileEntry {
    fn default() -> Self {
        FileEntry {
            bn: OsString::new(),
            sz: 0,
            md: None,
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

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CDirEntry {
    pub p: PathBuf,
    md: Option<SystemTime>,

    files_here: usize,
    files_below: usize,
    dirs_here: usize,
    dirs_below: usize,
    size_here: i128,
    size_below: i128,
    pub memory_usage_here: usize,
    pub memory_usage_below: usize,

    #[serde(deserialize_with = "deserialize_boxed_slice")]
    files: Box<[FileEntry]>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CDirEntryDiff {
    pub p: PathBuf,
    md: Option<SystemTime>,

    files_here: usize,
    files_below: usize,
    dirs_here: usize,
    dirs_below: usize,
    size_here: i128,
    size_below: i128,
    pub memory_usage_here: usize,
    pub memory_usage_below: usize,
    
    diff_type: DiffType,

    #[serde(deserialize_with = "deserialize_boxed_slice")]
    files: Box<[FileEntry]>,
}

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

pub fn walk_iter(root: std::path::PathBuf, stop_before_path: Option<std::path::PathBuf>, skip_set: &mut HashSet<PathBuf>) -> std::vec::Vec<CDirEntry> {
    let mut df: std::vec::Vec<CDirEntry> = Vec::new();
    let mut v: Vec<std::path::PathBuf> = Vec::new();
    let mut pm: HashMap<std::path::PathBuf, usize> = HashMap::new();

    let mut total_time_stat = Duration::new(0, 0);
    let mut sc = 0;
    let mut total_time_readdir = Duration::new(0, 0);
    let mut rdc = 0;

    // Stage 1. Traverse file tree and add to list
    // Push root onto stack
    let _ = v.push(root.clone());
    let mut idx:i64 = -1;
    let has_stop_before_path = (&stop_before_path).is_some();
    loop {
        idx += 1;
        if idx as usize >= v.len() {
            break;
        }
        let mp = &v[idx as usize].clone();

        if has_stop_before_path && *mp >= stop_before_path.as_deref().unwrap() {
            break;
        }

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
        let curr_idx = insert_dir_entry(&md, &mp, &mut df, &mut pm);
        
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

    
    // Stage 2. Re-traverse elements, in-reverse to "bubble up" `below` properties up the ds
    if df.len() > 0 {
        let cap_len_dirs = df.capacity() as f64 / df.len() as f64;
        let cap_len_dirs_map = pm.capacity() as f64 / pm.len() as f64;
        let pm_entry_size = mem::size_of_val(&pm.entry(df[0].p.clone()));
        let mut_dt = &mut df;
        for i in 0..mut_dt.len() {
            // Calculate memory usage for self
            let curr_idx = mut_dt.len() - 1 - i;
            let d = mut_dt[curr_idx].clone();
            
            mut_dt[curr_idx].memory_usage_here = (d.memory_usage_here as f64 * MEMORY_OFF_FACTOR) as usize;
            mut_dt[curr_idx].memory_usage_below = (d.memory_usage_below as f64 * MEMORY_OFF_FACTOR) as usize;
    
            if let Some(parent) = d.p.parent() {
                if let Some(maybe_ent) = pm.get(parent) {
                    let idx = *maybe_ent;
    
                    mut_dt[idx].dirs_here += 1;
                    mut_dt[idx].dirs_below += d.dirs_here + d.dirs_below;
                    mut_dt[idx].files_below += d.files_here + d.files_below;
                    mut_dt[idx].size_below += d.size_here + d.size_below;
    
                    mut_dt[idx].memory_usage_here += (((size_of::<CDirEntry>() + size_of::<PathBuf>()) as f64) * cap_len_dirs) as usize + (pm_entry_size as f64 * cap_len_dirs_map) as usize + (3 * d.p.capacity()); // dir + v + pm
                    mut_dt[idx].memory_usage_below += d.memory_usage_here + d.memory_usage_below;
                }
            }
        }
    }

    df.sort_by(|a, b| {
        return a.p.cmp(&b.p);
    });
    
    return df;
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
    };
    dest.push(e);
    return dest.len() - 1;
}

fn insert_dir_entry(md: &Metadata, p: &PathBuf, all_dirs: &mut Vec<CDirEntry>, path_idx_map: &mut HashMap<PathBuf, usize>) -> usize {
    let t = match md.modified() {
        Ok(st) => {Some(st)}
        Err(_) => None
    };
    let e = CDirEntry{
        p: p.clone(),
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
    path_idx_map.insert(p.clone(), all_dirs.len() - 1);
    return all_dirs.len() - 1;                                           
}

#[cfg(test)]
mod tests {
    use std::{collections::{HashMap, HashSet}, path::PathBuf, str::FromStr};

    use crate::utility::get_cwd;

    use super::{walk_iter};

    #[test]
    fn one_root_file_iter() {
        let wd = get_cwd();
        let path = std::path::PathBuf::from_str(format!("{}/tests/test_dir/b", wd.display()).as_str());
        match path {
            Ok(p) => {
                let res = walk_iter(p.clone(), None, &mut HashSet::new());
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
                let res = walk_iter(p.clone(), None,&mut HashSet::new());
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
                let res = walk_iter(p.clone(), None,&mut HashSet::new());
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
                let res = walk_iter(p.clone(), None,&mut HashSet::new());
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