use std::{fs::{DirEntry, File}, hash::{DefaultHasher, Hasher}, io::{BufReader, Seek, SeekFrom}, os::unix::ffi::OsStrExt, path::PathBuf, usize};
use std::io::{self, Read};

use crate::{scan::START_VECTOR_BYTES, walk::CDirEntry};

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
        if parts.len() < 2 {
            return ret;
        }

        let maybe_num = parts[1];
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