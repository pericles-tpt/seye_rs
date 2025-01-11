extern crate queues;
use std::{ffi::{OsStr, OsString}, fs::DirEntry, mem, os::unix::fs::MetadataExt, time::SystemTime};
use std::{collections::{HashMap, HashSet}, fs::{symlink_metadata, Metadata}, path::PathBuf, time::Duration};
use serde::{Deserialize, Deserializer, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
enum DiffType {
    Add,
    Remove,
    Modify
    // TODO: rename, a bit harder to do, do I need file hashes?
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FileEntry {
    bn: OsString,
    sz: u64,
    md: Option<SystemTime>,
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

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FileEntryDiff {
    bn: OsString,
    sz: u64,
    md: Option<SystemTime>,
    diff_type: DiffType,
}


pub fn deserialize_boxed_slice<'de, D>(deserializer: D) -> Result<Box<[FileEntry]>, D::Error>
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

pub fn find_iter(target: OsString, root: std::path::PathBuf) {
    let mut found: Vec<OsString> = Vec::new();
    let mut v: Vec<PathBuf> = Vec::new();
    let lossy_target = target.to_string_lossy();

    // Stage 1. Traverse file tree and add to list
    // Push root onto stack
    let _ = v.push(root.clone());
    let mut idx:i64 = -1;
    loop {
        idx += 1;
        if idx as usize >= v.len() {
            break;
        }
        let mp = &v[idx as usize].clone();
        
        let rd = std::fs::read_dir(mp);
        if rd.is_err() {
            // TODO: Handle error
            continue;
        }

        if mp.to_string_lossy().contains(lossy_target.as_ref()) {
            found.push(mp.clone().into_os_string());
        }

        for ent in rd.unwrap() {
            if ent.is_err() {
                // TODO: Handle error
                continue;
            }
            let val = ent.unwrap();

            let maybe_ft = val.file_type();
            if maybe_ft.is_err() {
                // TODO: Error
                continue;
            }

            let ft = maybe_ft.unwrap();
            let p = val.path();
            if ft.is_dir() {
                v.push(p);
            } else if p.to_string_lossy().contains(lossy_target.as_ref()) {
                found.push(p.into_os_string());
            }
        }
    }

    let sep: &OsStr = OsStr::new("\n");
    println!("{:?}", found.join(sep));
}

pub fn walk_iter(root: std::path::PathBuf, skip_set: &mut HashSet<PathBuf>) -> std::vec::Vec<CDirEntry> {
    let mut df: std::vec::Vec<CDirEntry> = Vec::new();
    let mut v: Vec<std::path::PathBuf> = Vec::new();
    let mut pm: HashMap<std::path::PathBuf, usize> = HashMap::new();

    let mut totalTimeStat = Duration::new(0, 0);
    let mut sc = 0;
    let mut totalTimeReaddir = Duration::new(0, 0);
    let mut rdc = 0;

    // Stage 1. Traverse file tree and add to list
    // Push root onto stack
    let _ = v.push(root.clone());
    let mut idx:i64 = -1;
    loop {
        idx += 1;
        if idx as usize >= v.len() {
            break;
        }
        let mp = &v[idx as usize].clone();

        if skip_set.contains(mp) {
            continue;
        }

        let bef1 = std::time::Instant::now();
        let rd = std::fs::read_dir(mp);
        let trd = bef1.elapsed();
        totalTimeReaddir += trd;
        rdc += 1;

        if rd.is_err() {
            // TODO: Handle error
            continue;
        }
        let entries: Vec<Result<DirEntry, std::io::Error>> = rd.unwrap().collect();

        let bef2 = std::time::Instant::now();
        let maybe_md = symlink_metadata(mp);
        let ts = bef2.elapsed();
        totalTimeStat += ts;
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
            totalTimeStat += ts;
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
            df[curr_idx].size_here += fmd.size() as i128;

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


    // OPTIONAL: Logging
    // println!("cap len dirs: {}", cap_len_dirs);

    // let mut extraStringCap = 0;
    // for d in &df {
    //     extraStringCap += d.p.capacity();
    // }

    // if sc > 0 {
    //     println!("Avg time stat: {:.2}us", (totalTimeStat.as_nanos() as f64 / sc  as f64) / 1000.0);
    // }
    // if rdc > 0 {
    //     println!("Avg time readdir (adjusted for entries in dir): {:.2}us", (totalTimeReaddir.as_nanos() as f64 / rdc  as f64) / 1000.0);
    // }

    // println!("Total syscall time: {}ms", totalTimeStat.as_millis() + totalTimeReaddir.as_millis());

    // println!("num dir: {}", df.len());
    // println!("dirs size: {}", df.len() * size_of_val(&df[0]));

    // println!("extra string size: {}", extraStringCap);

    // println!("top elem: {:?}", df[0]);
    // println!("bottom elem: {:?}", df[df.len() - 1]);

    return df;
}

// TODO: This returns the wrong count of `_below` properties for some reasons, not a high priority since iterative is:
//  - faster
//  - lower cpu usage
//  - lower memory usage
// Recursive only wins for small file tree (i.e. 1's - 10's of thousands of files/dirs)
pub fn walk_rec(root: std::path::PathBuf, df: &mut std::vec::Vec<CDirEntry>, pm: &mut HashMap<std::path::PathBuf, usize>, skip_set: &mut HashSet<PathBuf>, depth: usize) -> Option<CDirEntry> {
    if skip_set.contains(&root) {
        return None;
    }

    let rd = std::fs::read_dir(&root);
    if rd.is_err() {
        // TODO: Handle error
        return None;
    }
    let entries: Vec<Result<DirEntry, std::io::Error>> = rd.unwrap().collect();
    
    let maybe_md = symlink_metadata(&root);
    if maybe_md.is_err() {
        // TODO: Handle error
        return None;
    }
    let md = maybe_md.unwrap();
    let node_idx = insert_dir_entry(&md, &root, df, pm);

    // NOTE: Ratios all set to 1 here since can't accurately calculate them for recursive yet
    let cap_len_files = 1.0;
    let cap_len_dirs = 1.0;
    let cap_len_dirs_map = 1.0;
    let pm_entry_size = mem::size_of_val(&pm.entry(df[0].p.clone()));
    
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

        if ft.is_file() {
            let maybe_fmd = symlink_metadata(&val.path());
            if maybe_fmd.is_err() {
                // TODO: Handle error
                return None;
            }
            let fmd = maybe_fmd.unwrap();
    
            let basename_string = basename.to_os_string();
            let basename_string_len = basename_string.len();
            insert_file_entry(&fmd, basename.to_os_string(),  &mut file_entries);
    
            // Stage 2. Re-traverse elements, in-reverse to "bubble up" `below` properties up the ds
            df[node_idx].files_here += 1;
            df[node_idx].size_here += fmd.size() as i128;

            let file_entry_size = size_of::<FileEntry>() + basename_string_len;
            df[node_idx].memory_usage_here += file_entry_size;
            continue;
        }

        let maybe_de = walk_rec(p.clone(), df, pm, skip_set, depth + 1);
        if maybe_de.is_none() {
            // TODO: Error
            continue;
        }
        let de = maybe_de.unwrap();

        df[node_idx].memory_usage_here = (de.memory_usage_here as f64 * MEMORY_OFF_FACTOR) as usize;
        df[node_idx].memory_usage_below = (de.memory_usage_below as f64 * MEMORY_OFF_FACTOR) as usize;

        df[node_idx].dirs_here += 1;
        df[node_idx].dirs_below += de.dirs_here + de.dirs_below;
        df[node_idx].files_below += de.files_here + de.files_below;
        df[node_idx].size_below += de.size_here + de.size_below;

        df[node_idx].memory_usage_here += (((size_of::<CDirEntry>() + size_of::<PathBuf>()) as f64) * cap_len_dirs) as usize + (pm_entry_size as f64 * cap_len_dirs_map) as usize + (3 * de.p.capacity()); // dir + v + pm
        df[node_idx].memory_usage_below += de.memory_usage_here + de.memory_usage_below;
    }
    df[node_idx].files = file_entries.into_boxed_slice();

    let ret = df[node_idx].clone();

    // OPTIONAL: Logging
    if depth == 0 {
        // println!("cap len files: {}", cap_len_files);
        // println!("cap len dirs: {}", cap_len_dirs);
    
        // let df_len = df.len();
        // let df0 = df[0].clone();

        // let mut extraStringCap = 0;
        // for d in df {
        //     extraStringCap += d.p.capacity();
        // }
    
        // println!("num dir: {}", df_len);
        // println!("dirs size: {}", df_len * size_of_val(&df0));
    
        // println!("extra string size: {}", extraStringCap);
    
        // println!("top elem: {:?}", df0);
    }

    return Some(ret);
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
    use std::{collections::{HashMap, HashSet}, str::FromStr};

    use super::{get_cwd, walk_iter, walk_rec, CDirEntry};

    #[test]
    fn one_root_file_iter() {
        let wd = get_cwd();
        
        let path = std::path::PathBuf::from_str(format!("{}/src/walk/test_dir/b", wd.display()).as_str());
        match path {
            Ok(p) => {
                let res = walk_iter(p.clone(), &mut HashSet::new());
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
        
        let path = std::path::PathBuf::from_str(format!("{}/src/walk/test_dir/c", wd.display()).as_str());
        match path {
            Ok(p) => {
                let res = walk_iter(p.clone(), &mut HashSet::new());
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
        
        let path = std::path::PathBuf::from_str(format!("{}/src/walk/test_dir/a/e", wd.display()).as_str());
        match path {
            Ok(p) => {
                let res = walk_iter(p.clone(), &mut HashSet::new());
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

        let path = std::path::PathBuf::from_str(format!("{}/src/walk/test_dir", wd.display()).as_str());
        match path {
            Ok(p) => {
                let res = walk_iter(p.clone(), &mut HashSet::new());
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

    #[test]
    fn one_root_file_rec() {
        let wd = get_cwd();
        
        let path = std::path::PathBuf::from_str(format!("{}/src/walk/test_dir/b", wd.display()).as_str());
        match path {
            Ok(p) => {
                let mut res: std::vec::Vec<CDirEntry> = Vec::new();
                let mut path_map: HashMap<std::path::PathBuf, usize> = HashMap::new();
                let root: Option<crate::walk::CDirEntry> = walk_rec(p.clone(), &mut res, &mut path_map, &mut HashSet::new(), 0);
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
    fn one_root_folder_rec() {
        let wd = get_cwd();
        
        let path = std::path::PathBuf::from_str(format!("{}/src/walk/test_dir/c", wd.display()).as_str());
        match path {
            Ok(p) => {
                let mut res: std::vec::Vec<CDirEntry> = Vec::new();
                let mut path_map: HashMap<std::path::PathBuf, usize> = HashMap::new();
                let root: Option<crate::walk::CDirEntry> = walk_rec(p.clone(), &mut res, &mut path_map, &mut HashSet::new(), 0);
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
    fn dirs_files_below_rec() {
        let wd = get_cwd();
        
        let path = std::path::PathBuf::from_str(format!("{}/src/walk/test_dir/a/e", wd.display()).as_str());
        match path {
            Ok(p) => {
                let mut res: std::vec::Vec<CDirEntry> = Vec::new();
                let mut path_map: HashMap<std::path::PathBuf, usize> = HashMap::new();
                let root: Option<crate::walk::CDirEntry> = walk_rec(p.clone(), &mut res, &mut path_map, &mut HashSet::new(), 0);
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
    fn all_dirs_files_below_rec() {
        let wd = get_cwd();

        let path = std::path::PathBuf::from_str(format!("{}/src/walk/test_dir", wd.display()).as_str());
        match path {
            Ok(p) => {                
                let mut res: std::vec::Vec<CDirEntry> = Vec::new();
                let mut path_map: HashMap<std::path::PathBuf, usize> = HashMap::new();
                let root: Option<crate::walk::CDirEntry> = walk_rec(p.clone(), &mut res, &mut path_map, &mut HashSet::new(), 0);
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

fn get_cwd () -> PathBuf {
    let cwd = std::env::current_dir();
    let mut wd: PathBuf = PathBuf::new();
    match cwd {
        Ok(wd1) => {
            wd = wd1;
        }
        Err(e) => {
            panic!("failed to get wd")
        }
    }
    return wd;
}