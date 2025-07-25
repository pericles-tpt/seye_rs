use std::fs::metadata;
use std::{ffi::OsString, fs::DirEntry, os::unix::fs::MetadataExt, time::SystemTime};
use std::{collections::HashMap, fs::{symlink_metadata, Metadata}, path::PathBuf};
use serde::{Deserialize, Serialize};
use crate::utility;

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
    pub md5: [u8; 16],

    pub files: Vec<FileEntry>,
    pub symlinks: Vec<FileEntry>,
}

pub fn walk_collect_until_limit(some: &mut Vec<std::path::PathBuf>, other_entries: &mut Vec<CDirEntry>, thread_readdir_limit: usize) -> std::io::Result<Vec<PathBuf>> {
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
        let entries: Vec<Result<DirEntry, std::io::Error>> = rd.unwrap().collect();
        let mut file_entries: Vec<FileEntry> = Vec::with_capacity(entries.len());
        let mut symlink_entries: Vec<FileEntry> = Vec::with_capacity(entries.len());
        for ent in entries {
            let Ok(val) = ent else { continue };
            let Ok(ft) = val.file_type() else { continue };
                
            if ft.is_dir() {
                dir_q.push(val.path());
                continue;
            }

            let Ok(fmd) = metadata(val.path()) else {continue};    
                
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
        other_entries[curr_idx].symlinks = symlink_entries;
        other_entries[curr_idx].files = file_entries;

        other_entries[curr_idx].md5 = utility::get_md5_of_cdirentry(other_entries[curr_idx].clone());

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
        md5: [0; 16],

        files: vec![],
        symlinks: vec![],
    };
    all_dirs.push(e);
    path_idx_map.insert(pb, all_dirs.len() - 1);
    return all_dirs.len() - 1;                                           
}