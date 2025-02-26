use std::{collections::HashSet, io::Error};

use crate::{utility::thread_from_root, walk::walk_search_until_limit};

pub fn find(target_substring: String, target_path: std::path::PathBuf, num_threads: usize, thread_add_dir_limit: usize) -> Result<Vec<String>, Error> {
    if num_threads <= 1 {
        return Err(std::io::Error::new(std::io::ErrorKind::Other, format!("Provided 0 or 1 number of threads, must provide '-t' argument > 1")))
    }

    let maybe_curr_scan: std::io::Result<Vec<String>> = thread_from_root(
        target_path, 
        HashSet::new(),
        &target_substring, 
        num_threads, 
        thread_add_dir_limit, 
        None, 
        Some(walk_search_until_limit), 
        |a, b| {
            return a.cmp(b);
        }
    );
    if maybe_curr_scan.is_err() {
        return Err(std::io::Error::new(std::io::ErrorKind::Other, format!("Failed to do MT find: {:?}", maybe_curr_scan.err())))
    }
    let mut curr_scan = maybe_curr_scan.unwrap();

    curr_scan.sort_by(|a, b| {
        return a.cmp(b);
    });

    Ok(curr_scan)
}