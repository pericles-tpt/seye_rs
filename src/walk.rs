use std::fs::metadata;
use std::{ffi::OsString, fs::DirEntry, os::unix::fs::MetadataExt, time::SystemTime};
use std::{collections::{HashMap, HashSet}, fs::{symlink_metadata, Metadata}, path::PathBuf, time::Duration};
use serde::{Deserialize, Deserializer, Serialize};
use chksum_md5 as md5;

use crate::diff::CDirEntryDiff;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct FileEntry {
    pub sz: u64,
    pub bn: OsString,
    pub md: Option<SystemTime>,
}
impl Default for FileEntry {
    fn default() -> Self {
        FileEntry {
            sz: 0,
            bn: OsString::new(),
            md: None,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct CDirEntry {
    pub files_here: usize,
    pub files_below: usize,
    pub dirs_here: usize,
    pub dirs_below: usize,
    pub size_here: i64,
    pub size_below: i64,
    
    pub p: PathBuf,
    pub md: Option<SystemTime>,

    #[serde(deserialize_with = "deserialize_boxed_slice")]
    pub files: Box<[FileEntry]>,
    #[serde(deserialize_with = "deserialize_boxed_slice")]
    pub symlinks: Box<[FileEntry]>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Default)]
pub struct FullScan {
    pub entries: Vec<CDirEntry>,
    pub hashes: Vec<Vec<String>>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DiffScan {
    pub entries: Vec<CDirEntryDiff>,
    // `DiffScan`s won't hash every entry, just the ones it suspects are moved
    pub hashes: Vec<(usize, Vec<String>)>,
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

pub fn walk_until_end(root: std::path::PathBuf, parent_map: &mut HashMap<std::path::PathBuf, usize>, skip_set: &mut HashSet<PathBuf>, is_initial_scan: bool) -> FullScan {
    let mut ret = FullScan{
        entries: Vec::new(),
        hashes: Vec::new(),
    };
    let mut v: Vec<std::path::PathBuf> = Vec::new();

    let mut total_time_stat = Duration::new(0, 0);
    // let mut sc = 0;
    let mut total_time_readdir = Duration::new(0, 0);
    // let mut rdc = 0;

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
        // rdc += 1;

        if rd.is_err() {
            // TODO: Handle error
            continue;
        }
        let entries: Vec<Result<DirEntry, std::io::Error>> = rd.unwrap().collect();

        let bef2 = std::time::Instant::now();
        let maybe_md = symlink_metadata(mp);
        let ts = bef2.elapsed();
        total_time_stat += ts;
        // sc += 1;
        if maybe_md.is_err() {
            // TODO: Handle error
            continue;
        }
        let md = maybe_md.unwrap();
        let curr_idx = insert_dir_entry(&md, &mp, &mut ret.entries, parent_map);
        if is_initial_scan {
            ret.hashes.push(Vec::with_capacity(entries.len() + 1));
            let last_idx = ret.hashes.len() - 1;
            let dir_path = mp.clone().into_os_string().into_string().unwrap();
            ret.hashes[last_idx].push(dir_path);
        }

        let mut file_entries: Vec<FileEntry> = Vec::with_capacity(entries.len());
        let mut symlink_entries: Vec<FileEntry> = Vec::with_capacity(entries.len());
        for ent in entries {
            let Ok(val) = ent else { continue };
            let Ok(ft) = val.file_type() else { continue };

            // if IGNORE_LIST.contains(basename) {
            //     continue;
            // }

            if ft.is_symlink() {
                continue;
            }

            let p = &val.path();
            let maybe_basename = p.file_name();
            if maybe_basename.is_none() {
                continue;
            }
            let basename = maybe_basename.unwrap();

            if ft.is_dir() {
                let _ = v.push(p.to_path_buf());
                if is_initial_scan {
                    ret.hashes[curr_idx].push(format!("/{}", basename.to_str().unwrap()));
                }
                continue 
            }

            let bef2 = std::time::Instant::now();
            let maybe_fmd = symlink_metadata(p);
            let ts = bef2.elapsed();
            total_time_stat += ts;
            // sc += 1;
            if maybe_fmd.is_err() {
                // TODO: Handle error
                continue;
            }
            let fmd = maybe_fmd.unwrap();

            let basename_string = basename.to_os_string();
            
            if is_initial_scan {
                // Read / Hash the file
                let mut hash_string: String = String::from("");
                let file = std::fs::File::open(val.path());
                if file.is_ok() {
                    let digest = md5::chksum(file.unwrap());
                    if digest.is_ok() {
                        hash_string = digest.unwrap().to_hex_lowercase();
                    }
                }

                // Store the hash in the vector
                ret.hashes[curr_idx].push(hash_string);
            }
            
            if fmd.is_symlink() {
                insert_file_entry(&fmd, basename_string, &mut symlink_entries);
            } else {
                insert_file_entry(&fmd, basename_string, &mut file_entries);
            }

            ret.entries[curr_idx].files_here += 1;
            ret.entries[curr_idx].size_here += fmd.size() as i64;
        }

        ret.entries[curr_idx].symlinks = symlink_entries.into_boxed_slice();
        ret.entries[curr_idx].files = file_entries.into_boxed_slice();
    }
    
    return ret;
}

pub fn walk_collect_until_limit(some: &mut Vec<std::path::PathBuf>, _skip_set: &HashSet<PathBuf>, other_entries: &mut Vec<CDirEntry>, other_hashes: &mut Vec<Vec<String>>, thread_readdir_limit: usize, is_initial_scan: bool) -> std::io::Result<Vec<PathBuf>> {
    let mut d_idx = 0;
    let mut f_idx = 0;
    
    let mut readdir_limit = thread_readdir_limit;
    if readdir_limit < some.len() {
        readdir_limit = some.len();
    }
    
    let mut dir_q: Vec<PathBuf> = Vec::with_capacity(readdir_limit);
    dir_q.append(some);

    let mut pm = HashMap::new();
    while (d_idx + f_idx) < readdir_limit && d_idx < dir_q.len() {
        let rd = std::fs::read_dir(&dir_q[d_idx]);
        if rd.is_err() {
            return Err(std::io::Error::new(std::io::ErrorKind::Other, format!("{:?}", rd.err())));
        }

        let maybe_md = symlink_metadata(&dir_q[d_idx]);
        if maybe_md.is_err() {
            return Err(std::io::Error::new(std::io::ErrorKind::Other, format!("{:?}", maybe_md.err())));
        }
        let md = maybe_md.unwrap();

        let curr_idx = insert_dir_entry(&md, &dir_q[d_idx], other_entries, &mut pm);
        if is_initial_scan {
            other_hashes.push(Vec::with_capacity(other_entries.len() + 1));
            let last_idx = other_hashes.len() - 1;
            let dir_path = dir_q[d_idx].clone().into_os_string().into_string().unwrap();
            other_hashes[last_idx].push(dir_path);
        }

        let entries: Vec<Result<DirEntry, std::io::Error>> = rd.unwrap().collect();
        let mut file_entries: Vec<FileEntry> = Vec::with_capacity(entries.len());
        let mut symlink_entries: Vec<FileEntry> = Vec::with_capacity(entries.len());
        for ent in entries {
            let Ok(val) = ent else { continue };
            let Ok(ft) = val.file_type() else { continue };
            // NOTE: This was commented out as it has a BIG impact on performance
            // if skip_set.contains(&val.path()) {
            //     continue;
            // }
    
            // if IGNORE_LIST.contains(filename.as_os_str()) {
            //     continue 
            // }
            let filename = val.file_name();
                
            if ft.is_dir() {
                dir_q.push(val.path());
                if is_initial_scan {
                    other_hashes[curr_idx].push(format!("/{}", filename.to_str().unwrap()));
                }
                continue;
            } else if !(ft.is_file() || ft.is_symlink()) {
                continue;
            }

            let Ok(fmd) = metadata(val.path()) else {continue};    
                
            if is_initial_scan {
                // Read / Hash the file
                let mut hash_string: String = String::from("F");
                if ft.is_file() {
                    let file = std::fs::File::open(val.path());
                    if file.is_ok() {
                        let digest = md5::chksum(file.unwrap());
                        if digest.is_ok() {
                            hash_string = digest.unwrap().to_hex_lowercase();
                        }
                    }
                } else if ft.is_symlink() {
                    hash_string = String::from("S");
                }

                // Store the hash in the vector
                other_hashes[curr_idx].push(hash_string);
            }

            f_idx += 1;
            let filename = val.file_name();
            if fmd.is_symlink() {
                insert_file_entry(&fmd, filename, &mut symlink_entries);
            } else {
                insert_file_entry(&fmd, filename, &mut file_entries);
            }
    
            other_entries[curr_idx].files_here += 1;
            other_entries[curr_idx].size_here += fmd.size() as i64;
        }        
        other_entries[curr_idx].symlinks = symlink_entries.into_boxed_slice();
        other_entries[curr_idx].files = file_entries.into_boxed_slice();

        d_idx += 1;
    }

    return Ok(dir_q.drain(d_idx..).collect());
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

        files: Box::new([FileEntry::default()]),
        symlinks: Box::new([FileEntry::default()]),
    };
    all_dirs.push(e);
    path_idx_map.insert(pb, all_dirs.len() - 1);
    return all_dirs.len() - 1;                                           
}

#[cfg(test)]
mod tests {
    use std::{collections::{HashMap, HashSet}, str::FromStr};

    use crate::utility;

    use super::walk_until_end;

    #[test]
    fn one_root_file_iter() {
        let wd = utility::get_cwd();
        let path = std::path::PathBuf::from_str(format!("{}/tests/test_dir/b", wd.display()).as_str());
        match path {
            Ok(p) => {
                let mut pm = HashMap::new();
                let res = walk_until_end(p.to_path_buf(), &mut pm, &mut HashSet::new(), true);
                assert_eq!(res.entries.len(), 1);

                assert_eq!(res.entries[0].p, p);
                assert_eq!(res.entries[0].files_here, 1);
                assert_eq!(res.entries[0].files_below, 0);
                assert_eq!(res.entries[0].dirs_here, 0);
                assert_eq!(res.entries[0].dirs_below, 0);
                assert_eq!(res.entries[0].size_here, 4);
                assert_eq!(res.entries[0].size_below, 0);
            }
            Err(e) => {
                panic!("failed to get path buf: {}", e)
            }
        }
    }

    #[test]
    fn one_root_folder_iter() {
        let wd = utility::get_cwd();
        
        let path = std::path::PathBuf::from_str(format!("{}/tests/test_dir/c", wd.display()).as_str());
        match path {
            Ok(p) => {
                let mut pm = HashMap::new();
                let res = walk_until_end(p.to_path_buf(), &mut pm,&mut HashSet::new(), true);
                assert_eq!(res.entries.len(), 2);

                assert_eq!(res.entries[0].p, p);
                assert_eq!(res.entries[0].files_here, 0);
                assert_eq!(res.entries[0].files_below, 0);
                assert_eq!(res.entries[0].dirs_here, 1);
                assert_eq!(res.entries[0].dirs_below, 0);
                assert_eq!(res.entries[0].size_here, 0);
                assert_eq!(res.entries[0].size_below, 0);
                
                let fp = p.join("./d");
                assert_eq!(res.entries[1].p, fp);
                assert_eq!(res.entries[1].files_here, 0);
                assert_eq!(res.entries[1].files_below, 0);
                assert_eq!(res.entries[1].dirs_here, 0);
                assert_eq!(res.entries[1].dirs_below, 0);
                assert_eq!(res.entries[1].size_here, 0);
                assert_eq!(res.entries[1].size_below, 0);
            }
            Err(e) => {
                panic!("failed to get path buf: {}", e)
            }
        }
    }

    #[test]
    fn dirs_files_below_iter() {
        let wd = utility::get_cwd();
        
        let path = std::path::PathBuf::from_str(format!("{}/tests/test_dir/a/e", wd.display()).as_str());
        match path {
            Ok(p) => {
                let mut pm = HashMap::new();
                let res = walk_until_end(p.to_path_buf(), &mut pm, &mut HashSet::new(), true);
                assert_eq!(res.entries.len(), 3);

                assert_eq!(res.entries[0].p, p);
                assert_eq!(res.entries[0].files_here, 1);
                assert_eq!(res.entries[0].files_below, 1);
                assert_eq!(res.entries[0].dirs_here, 1);
                assert_eq!(res.entries[0].dirs_below, 1);
                assert_eq!(res.entries[0].size_here, 0);
                assert_eq!(res.entries[0].size_below, 3);
            }
            Err(e) => {
                panic!("failed to get path buf: {}", e)
            }
        }
    }

    #[test]
    fn all_dirs_files_below_iter() {
        let wd = utility::get_cwd();

        let path = std::path::PathBuf::from_str(format!("{}/tests/test_dir", wd.display()).as_str());
        match path {
            Ok(p) => {
                let mut pm = HashMap::new();
                let res = walk_until_end(p.to_path_buf(), &mut pm,&mut HashSet::new(), true);
                assert_eq!(res.entries.len(), 8);

                assert_eq!(res.entries[0].p, p);
                assert_eq!(res.entries[0].files_here, 1);
                assert_eq!(res.entries[0].files_below, 4);
                assert_eq!(res.entries[0].dirs_here, 3);
                assert_eq!(res.entries[0].dirs_below, 4);
                assert_eq!(res.entries[0].size_here, 12);
                assert_eq!(res.entries[0].size_below, 8);
            }
            Err(e) => {
                panic!("failed to get path buf: {}", e)
            }
        }
    }
}