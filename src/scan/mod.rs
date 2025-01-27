use std::{collections::{HashMap, HashSet}, fs::File, hash::Hash, io::{BufWriter, Error}, os::unix::fs::MetadataExt, path::{self, Path, PathBuf}, ptr::null, str::FromStr, sync::{mpsc::channel, Arc, Mutex}, thread::{self, JoinHandle}, time::Duration, usize};

use memory_stats::memory_stats;
use crate::{save::{get_chunk_entry_offsets_from_file, get_hash_iteration_count_from_file_names, get_next_chunk_from_file, read_save_file}, walk::{walk_iter, walk_rec, CDirEntry, CDirEntryDiff}};

pub const START_VECTOR_BYTES: u64 = 8;

pub fn scan(target_path: std::path::PathBuf, output_path: std::path::PathBuf, min_diff_bytes: u64) -> Result<(), Error> {
    let save_file_data = get_hash_iteration_count_from_file_names(&target_path, output_path.to_path_buf());
    let mut path_to_initial = output_path.clone();
    path_to_initial.push(format!("{}_initial", save_file_data.0));

    let iteration_count = save_file_data.1;
    let is_initial_scan =iteration_count < 0;
    if is_initial_scan {
        // TODO: Figure out if I can multithread and memory limit the initial scan, I can't know how to split the traversal ahead of time. Maybe
        //       if I do a pre-scan, before I do the stat's? It might just be quicker to do it on a single thread...
        let mut path_idx_map: HashMap<std::path::PathBuf, usize> = HashMap::new();
        let df = walk_iter(target_path, &mut HashSet::new());

        let f  = File::create(path_to_initial)?;
        let writer = BufWriter::new(f);
        bincode::serialize_into(writer, &df).expect("failed to seralise");
    
        return Ok(())
    }

    // TODO: The function below doesn't currently work and it's preventing me from implementing the "memory limit" feature, have to read the WHOLE file for now
    // let _ = read_save_file_chunks(path_to_initial, adjusted_memory_limit, thread_limit);

    // Open file
    let mut f = File::open(&path_to_initial)?;
    let mut f_sz= 0;
    if let Ok(md) = f.metadata() {
        f_sz = md.size();
    }
    if f_sz == 0 {
        return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, format!("Failed to get size for provided file path")))
    }

    // Get ENTIRE file, chunk-by-chunk
    // TODO: In future should fetch and process one chunk at a time instead of stitching them all together here
    let mut last_scan: Vec<CDirEntry> = Vec::new();
    let maybe_last_scan = read_save_file(path_to_initial);
    match maybe_last_scan {
        Ok(entries) => {last_scan = entries}
        Err(e) => {return Err(std::io::Error::new(std::io::ErrorKind::Other, format!("Failed to read entries from file: {}", e)))}
    }
    last_scan.reverse();

    let mut diffs: Vec<CDirEntryDiff> = Vec::new();

    let mut path_to_subsequent = output_path.clone();
    path_to_subsequent.push(format!("{}_diff_{}", save_file_data.0, iteration_count));
    let f  = File::create(path_to_subsequent)?;
    let writer = BufWriter::new(f);
    bincode::serialize_into(writer, &diffs).expect("failed to seralise");

    Ok(())
}