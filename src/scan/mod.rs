use std::{collections::{HashMap, HashSet}, fs::File, hash::Hash, io::{BufWriter, Error}, os::unix::fs::MetadataExt, path::{self, Path, PathBuf}, ptr::null, str::FromStr, sync::{mpsc::channel, Arc, Mutex}, thread::{self, JoinHandle}, time::Duration, usize};

use memory_stats::memory_stats;
use crate::{save::{get_chunk_entry_offsets_from_file, get_hash_iteration_count_from_file_names, get_next_chunk_from_file, read_save_file}, walk::{walk_iter, walk_rec, CDirEntry, CDirEntryDiff}};

pub const START_VECTOR_BYTES: u64 = 8;

pub fn scan(target_path: std::path::PathBuf, output_path: std::path::PathBuf, thread_limit: usize, memory_limit: usize, is_recursive: bool) -> Result<(), Error> {
    // Adjust memory limit account for pre-scan usage and thread memory 
    let mut adjusted_memory_limit = memory_limit;
    let mut memory_usage_pre_scan = 0;
    if let Some(usage) = memory_stats() {
        memory_usage_pre_scan = usage.physical_mem;
    }

    // NOTE: This is a guesstimate, since I don't know the memory used by the last scan yet, just assume each thread has A LOT of paths in its list
    //       e.g. 8 threads -> (24 + 8 + 256) * (8 * 256) = 589.8K
    let avg_path_size_guess = 256;
    let paths_per_thread_guess = 256;
    let mut memory_usage_threads = (size_of::<PathBuf>() + size_of::<usize>() + avg_path_size_guess) * (thread_limit * paths_per_thread_guess);

    adjusted_memory_limit -= memory_usage_pre_scan + memory_usage_threads;

    let save_file_data = get_hash_iteration_count_from_file_names(&target_path, output_path.to_path_buf());
    let mut path_to_initial = output_path.clone();
    path_to_initial.push(format!("{}_initial", save_file_data.0));

    let iteration_count = save_file_data.1;
    let is_initial_scan =iteration_count < 0;
    if is_initial_scan {
        // TODO: Figure out if I can multithread and memory limit the initial scan, I can't know how to split the traversal ahead of time. Maybe
        //       if I do a pre-scan, before I do the stat's? It might just be quicker to do it on a single thread...
        let mut df: Vec<CDirEntry> = Vec::new();
        let mut path_idx_map: HashMap<std::path::PathBuf, usize> = HashMap::new();
        if is_recursive {
            if let None = walk_rec(target_path, &mut df, &mut path_idx_map, &mut HashSet::new(), 0) {
                // TODO: Get better error from recursive
                return Err(std::io::Error::new(std::io::ErrorKind::Other, "error occured when running recursing scan"));
            }
        } else {
            df = walk_iter(target_path, &mut HashSet::new());
        }

        let f  = File::create(path_to_initial)?;
        let writer = BufWriter::new(f);
        bincode::serialize_into(writer, &df).expect("failed to seralise");
    
        return Ok(())
    }

    // 1. Read file from leaves to root, storing:
    //  - paths per thread
    //  - file offset
    let mut thread_paths: Vec<Vec<PathBuf>> = vec![Vec::<PathBuf>::new(); thread_limit];
    let mut thread_path_offsets: Vec<Vec<usize>> = vec![Vec::<usize>::new(); thread_limit];

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
    let bef1 = std::time::Instant::now();
    let mut last_scan: Vec<CDirEntry> = Vec::new();
    let memory_gt_file_size = memory_limit == 0 || f_sz < memory_limit as u64;
    if memory_gt_file_size {
        let maybe_last_scan = read_save_file(path_to_initial);
        match maybe_last_scan {
            Ok(entries) => {last_scan = entries}
            Err(e) => {return Err(std::io::Error::new(std::io::ErrorKind::Other, format!("Failed to read entries from file: {}", e)))}
        }
        last_scan.reverse();
    } else {
        // READ CHUNKS START -> END: ~38% faster than going in reverse, reasons: 
        //                              - it deserialises the items twice (get offsets THEN get chunks, from the end)
        //                              - it reverses each chunk before `extend_from_slice` (rather than once on entire `last_scan` at end)
        //                           BUT going from root -> leaves is less useful than the reverse I think
        // let mut start = START_VECTOR_BYTES;
        // loop {
        //     if start == f_sz {
        //         break;
        //     }
        //     let maybe_chunk_next_offset = get_next_chunk_from_file(&mut f, f_sz, start, memory_limit as u64);
        //     match maybe_chunk_next_offset {
        //         Ok(chunk_next_offset) => {
        //             let chunk = chunk_next_offset.0;
        //             last_scan.extend_from_slice(&chunk);
        //             start = chunk_next_offset.1;
        //         }
        //         Err(e) => {
        //             return Err(std::io::Error::new(std::io::ErrorKind::Other, format!("Failed to retrieve CDirEntry's from chunk at offset: {}, err: {}", start, e)))
        //         }
        //     }
        // }
        // last_scan.reverse();
        // READ CHUNKS END -> START: Slower than going from start, but figuring out how to split paths between threads from leaves is easier
        let maybe_chunk_offsets = get_chunk_entry_offsets_from_file(&mut f, f_sz, memory_limit as u64);
        let mut offsets: Vec<u64>;
        match maybe_chunk_offsets {
            Ok(chunk_offsets) => {offsets = chunk_offsets}
            Err(e) => {return Err(std::io::Error::new(std::io::ErrorKind::Other, format!("Failed to get chunk offsets from file: {}", e)))}
        }
        offsets.reverse();
        for off in offsets {
            let maybe_chunk_next_offset = get_next_chunk_from_file(&mut f, f_sz, off, memory_limit as u64);
            match maybe_chunk_next_offset {
                Ok(chunk_next_offset) => {
                    let mut chunk = chunk_next_offset.0;
                    chunk.reverse();
                    last_scan.extend_from_slice(&chunk);
                }
                Err(e) => {
                    return Err(std::io::Error::new(std::io::ErrorKind::Other, format!("Failed to retrieve CDirEntry's from chunk at offset: {}, err: {}", off, e)))
                }
            }
        }
    }

    // Going from leaves to root
    let root_node_mem_usage = last_scan[last_scan.len() - 1].memory_usage_below + last_scan[last_scan.len() - 1].memory_usage_here;
    if memory_limit == 0 || memory_limit > root_node_mem_usage {
        adjusted_memory_limit += root_node_mem_usage - memory_limit;
    }
    
    // Iterating through tree BF in reverse, i.e. row-by-row from bottom to top
    let mem_per_thread = (adjusted_memory_limit as f64 / thread_limit as f64).ceil() as usize;
    let mut th_idx = 0;
    let mut children_lookup: HashMap<Option<&Path>, usize> = HashMap::new();
    let mut children: Vec<(Vec<PathBuf>, usize)> = Vec::new();
    let mut added_sizes: HashMap<PathBuf, usize> = HashMap::new();
    let mut curr_sz_sum = 0;
    for ent in &last_scan {
        // Entry (for this node as parent) exists in lookup -> this node represents ALL children in entry -> remove entry
        if let Some(curr_entry_idx) = children_lookup.get(&Some(&ent.p.as_path())) {
            // NOTE: Just clear the vector in `children`, so entry remains to not screw up idx                    
            added_sizes.insert(ent.p.to_path_buf(), children[*curr_entry_idx].1);
            
            children[*curr_entry_idx].0.clear();
            children[*curr_entry_idx].1 = 0;
            children_lookup.remove(&Some(&ent.p.as_path()));

        }

        let mut curr_sz = ent.memory_usage_here + ent.memory_usage_below;
        if let Some(sz_to_sub) = added_sizes.get(&ent.p) {
            curr_sz -= *sz_to_sub;
        }
        curr_sz_sum += curr_sz;
        
        // Until the total EXCEEDS the mem_per_thread, collect all nodes and record their parent
        if curr_sz_sum < mem_per_thread {
            // Parent exists in lookup -> add to this elem to vec
            if let Some(parent_entry_idx) = children_lookup.get(&ent.p.parent()) {
                children[*parent_entry_idx].0.push(ent.p.clone());
                children[*parent_entry_idx].1 += curr_sz;
            } else {
                // Either way, if the parent !exist, add an entry for it with this element
                children.push((vec![ent.p.clone()], curr_sz));
                children_lookup.insert(ent.p.parent(), children.len() - 1);
            }
            continue;
        }

        // Once the traversed total + curr total EXCEED per thread limit, add them to a thread
        let mut i = 0;
        while i < children.len()-1 {
            if children[i].0.len() == 0 {
                i += 1;
                continue;
            }
            let parent = children[i].0[0].parent();
            match parent {
                Some(p) => {
                    thread_paths[th_idx].push(p.to_path_buf());
                    added_sizes.insert(p.to_path_buf(), children[i].1);
                }
                None => {/* This is root */}
            }
            children[i].0.clear();

            i += 1;

            // TODO: Need to subtract added children from the parent

        }
        let last_children_set = &children[i].0;
        // let is_curr_sibling = last_children_set[0].0.parent() == ent.p.parent();
        // let partial_children = is_curr_sibling;
        thread_paths[th_idx].extend_from_slice(&last_children_set);
        if last_children_set.len() > 0 {
            if let Some(parent) = children[i].0[0].parent() {
                added_sizes.insert(parent.to_path_buf(), children[i].1);
            }
        }
        
        // children_lookup.clear();
        // children.clear();
        curr_sz_sum = 0;
        th_idx = (th_idx + 1) % thread_limit;

        // if mem_above_threshold {
        //     tot = mem_total;
        //     thread_paths[th_idx].push(ent.p);
        //     th_idx = (th_idx + 1) % thread_limit;
        // }

        // let curr_is_sibling = last_path.as_ref().is_some_and(|lp| lp.parent() == ent.p.parent());
        // let curr_is_parent = last_path.as_ref().is_some_and(|lp| lp.parent() == ent.p);
        // let curr_is_unrelated = !curr_is_sibling && !curr_is_parent;
        // // println!("lp: {:?}", tmp.p);
        // // println!("ent par: {:?}", ent.p.parent());
        // if curr_is_sibling {
        //     if !mem_below_threshold {
        //         // Find A sibling whose size is the least over the overage
        //         // TODO: Find multiple siblings
        //         let overage = siblings_sum - mem_per_thread;
        //         let mut closest_path: Option<&PathBuf> = None;
        //         let mut closest = usize::MAX;
        //         let mut other_paths: Vec<&PathBuf> = Vec::new();
        //         for kv in &siblings_map {
        //             // let sz = *kv.1;
        //             // if sz >= overage && (sz - overage) < closest {
        //             //     if !closest_path.is_none() {
        //             //         thread_paths[th_idx].push(closest_path.unwrap().to_path_buf());
        //             //     }
        //             //     closest_path = Some(kv.0);
        //             //     closest = sz;
        //             //     continue;
        //             // }
        //             thread_paths[th_idx].push(kv.0.to_path_buf());
        //         }

        //         // All orphans get added to this thread
        //         for or in &orphans {
        //             thread_paths[th_idx].push(or.clone());
        //         }

        //         th_idx = (th_idx + 1) % thread_limit;
        //         orphans.clear();
        //         orphans_size = 0;
        //         siblings_map = HashMap::new();
        //         siblings_sum = 0;
                
        //         if closest_path != None {
        //             // Add orphaned sibling to orphans
        //             orphans.push(closest_path.unwrap().to_path_buf());
        //             orphans_size += closest;
        //         }
        //     }
        //     siblings_map.insert(ent.p, mem_total);
        //     siblings_sum += mem_total;

        //     last_path = Some(tmp.p);
        //     continue;
        // }

        // Keep adding siblings to the array until reach either:
        //  - Parent

        // If siblings sum is less than required memory, will add the parent instead, probably...
        // if (curr_thread_sum + siblings_sum) <= mem_per_thread {
        //     curr_thread_sum += siblings_sum;
        //     siblings_map.clear();
        //     continue;
        // }
        


        // if curr_is_parent {
        //     // Add parent...
        // }j
        // thread_paths[th_idx].push(ent.p);
        // th_idx = (th_idx + 1) % thread_limit;

        // siblings_map.clear();
        // siblings_sum = 0;
        // last_path = Some(tmp.p);
    }
    let last_idx = thread_paths.len() - 1;
    thread_paths[last_idx].push(target_path);

    for i in 0..thread_paths.len() {
        println!("Num items in thread {i} is {}", thread_paths[i].len())
    }
    
    let mut hs: Vec<JoinHandle<()>> = Vec::new();
    let mut all_dirs: Vec<CDirEntry> = Vec::new();
    let (tx, rx) = channel::<()>();
    let data_arc = Arc::new(Mutex::new(all_dirs));
    for i in 0..thread_paths.len() {
        let curr_paths = thread_paths[i].clone();
        
        // Create skip sets for this thread
        let mut skip: HashSet<PathBuf> = HashSet::new();
        for j in 0.. thread_paths.len() {
            if j == i {
                continue;
            }
            for p in &thread_paths[j] {
                skip.insert(p.to_path_buf());
            }
        }
        
        let (data, tx) = (Arc::clone(&data_arc), tx.clone());
        hs.push(thread::spawn(move || {
            for r in curr_paths {
                let mut path_idx_map: HashMap<std::path::PathBuf, usize> = HashMap::new();
                let mut df: Vec<CDirEntry> = Vec::new();

                // TODO: Handle errors
                if is_recursive {
                    walk_rec(r.to_path_buf(), &mut df, &mut path_idx_map, &mut skip, 0);
                } else {
                    df = walk_iter(r.to_path_buf(), &mut skip);
                }

                let mut data = data.lock().unwrap();
                data.extend_from_slice(&df);
            }
        }));
    }
    for i in hs {
        i.join().unwrap();
    }

    // Sort both arrays in alphabetical order
    last_scan.sort_by(|a, b| {
        return a.p.cmp(&b.p);
    });
    data_arc.lock().unwrap().sort_by(|a, b| {
        return a.p.cmp(&b.p);
    });

    let mut diffs: Vec<CDirEntryDiff> = Vec::new();

    let mut path_to_subsequent = output_path.clone();
    path_to_subsequent.push(format!("{}_diff_{}", save_file_data.0, iteration_count));
    let f  = File::create(path_to_subsequent)?;
    let writer = BufWriter::new(f);
    bincode::serialize_into(writer, &diffs).expect("failed to seralise");

    Ok(())
}

// pub fn scan_by_chunks(target_path: std::path::PathBuf, output_path: std::path::PathBuf, thread_limit: usize, memory_limit: usize, is_recursive: bool) -> Result<(), Error> {
//     // 1. Adjust memory limit to account for pre scan and 
//     //  a. Pre scan usage
//     let mut memory_usage_pre_scan = 0;
//     if let Some(usage) = memory_stats() {
//         memory_usage_pre_scan = usage.physical_mem;
//     }
//     //  b. Path list per thread
//     // NOTE: This is a guess, since I don't know the memory used by the last scan yet, just assume each thread has A LOT of paths in its list
//     //       e.g. 8 threads -> (24 + 8 + 256) * (8 * 256) = 589.8K
//     let avg_path_size_guess = 256;
//     let paths_per_thread_guess = 4096;
//     let memory_usage_threads = (size_of::<PathBuf>() + avg_path_size_guess) * (thread_limit * paths_per_thread_guess);
//     let mut adjusted_memory_limit = memory_limit + memory_usage_pre_scan + memory_usage_threads;

//     // 2. Get prefix for output file
//     let save_file_data = get_hash_iteration_count_from_file_names(&target_path, output_path.to_path_buf());
//     let mut path_to_initial = output_path.clone();
//     path_to_initial.push(format!("{}_initial", save_file_data.0));

//     // 3. IF initial scan, save initial scan to file
//     // NOTE: Doesn't support thread or memory_limit yet
//     let iteration_count = save_file_data.1;
//     let is_initial_scan = iteration_count < 0;
//     if is_initial_scan {
//         // TODO: Figure out if I can multithread and memory limit the initial scan, I can't know how to split the traversal ahead of time. Maybe
//         //       if I do a pre-scan, before I do the stat's? It might just be quicker to do it on a single thread...
//         let mut df: Vec<CDirEntry> = Vec::new();
//         let mut path_idx_map: HashMap<std::path::PathBuf, usize> = HashMap::new();
//         if is_recursive {
//             if let None = walk_rec(target_path, &mut df, &mut path_idx_map, &mut HashSet::new(), 0) {
//                 // TODO: Get better error from recursive
//                 return Err(std::io::Error::new(std::io::ErrorKind::Other, "error occured when running recursing scan"));
//             }
//         } else {
//             df = walk_iter(target_path, &mut HashSet::new());
//         }

//         let f  = File::create(path_to_initial)?;
//         let writer = BufWriter::new(f);
//         bincode::serialize_into(writer, &df).expect("failed to seralise");
    
//         return Ok(())
//     }

//     // 4. ELSE, do subsequent scan with memory limit (by doing doing operations in chunk) and thread limit
//     //  a. Open file
//     let mut f = File::open(&path_to_initial)?;
//     let mut f_sz= 0;
//     if let Ok(md) = f.metadata() {
//         f_sz = md.size();
//     }
//     if f_sz == 0 {
//         return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, format!("Failed to get size for provided file path")))
//     }

//     //  b. Get offsets of CDirEntry chunks from file, from leaves to root
//     let maybe_chunk_offsets = get_chunk_entry_offsets_from_file(&mut f, f_sz, memory_limit as u64);
//     let mut offsets: Vec<u64>;
//     match maybe_chunk_offsets {
//         Ok(chunk_offsets) => {offsets = chunk_offsets}
//         Err(e) => {return Err(std::io::Error::new(std::io::ErrorKind::Other, format!("Failed to get chunk offsets from file: {}", e)))}
//     }
//     offsets.reverse();

//     //  c. Read each chunk, 
//     let mut thread_paths: Vec<Vec<PathBuf>> = vec![Vec::<PathBuf>::new(); thread_limit];
//     let mem_per_thread = (adjusted_memory_limit as f64 / thread_limit as f64).ceil() as usize;
//     let mut th_idx = 0;
//     let mut children_lookup: HashMap<Option<&Path>, usize> = HashMap::new();
//     let mut children: Vec<(Vec<PathBuf>, usize)> = Vec::new();
//     let mut added_sizes: HashMap<PathBuf, usize> = HashMap::new();
//     let mut chunk: Vec<CDirEntry>;
//     for i in 0..offsets.len() {
//         let off = offsets[i];
//         let maybe_chunk_next_offset = get_next_chunk_from_file(&mut f, f_sz, off, adjusted_memory_limit as u64);
//         match maybe_chunk_next_offset {
//             Ok(chunk_next_offset) => {
//                 if chunk_next_offset.0.len() == 0 {
//                     continue;
//                 }
//                 chunk = chunk_next_offset.0;
//             }
//             Err(e) => {
//                 return Err(std::io::Error::new(std::io::ErrorKind::Other, format!("Failed to retrieve CDirEntry's from chunk at offset: {}, err: {}", off, e)))
//             }
//         }
//         chunk.reverse();
    
//         // Going from leaves to root
//         // let root_node_mem_usage = chunk[chunk.len() - 1].memory_usage_below + chunk[chunk.len() - 1].memory_usage_here;
//         // if memory_limit == 0 || memory_limit > root_node_mem_usage {
//         //     adjusted_memory_limit += root_node_mem_usage - memory_limit;
//         // }

//         let mut curr_sz_sum = 0;
//         for ent in &chunk {
//             // Entry (for this node as parent) exists in lookup -> this node represents ALL children in entry -> remove entry
//             if let Some(curr_entry_idx) = children_lookup.get(&Some(ent.p.as_path())) {
//                 // NOTE: Just clear the vector in `children`, so entry remains to not screw up idx    
//                 let pb = ent.p.to_path_buf();                
//                 added_sizes.insert(pb, children[*curr_entry_idx].1);
                
//                 children[*curr_entry_idx].0.clear();
//                 children[*curr_entry_idx].1 = 0;
//                 children_lookup.remove(&Some(ent.p.as_path()));
//             }
    
//             let mut curr_sz = ent.memory_usage_here + ent.memory_usage_below;
//             if let Some(sz_to_sub) = added_sizes.get(&ent.p) {
//                 curr_sz -= *sz_to_sub;
//             }
//             curr_sz_sum += curr_sz;
            
//             // Until the total EXCEEDS the mem_per_thread, collect all nodes and record their parent
//             if curr_sz_sum < mem_per_thread {
//                 // Parent exists in lookup -> add to this elem to vec
//                 if let Some(parent_entry_idx) = children_lookup.get(&ent.p.parent()) {
//                     children[*parent_entry_idx].0.push(ent.p.to_path_buf());
//                     children[*parent_entry_idx].1 += curr_sz;
//                 } else {
//                     // Either way, if the parent !exist, add an entry for it with this element
//                     children.push((vec![(&ent).p.clone()], curr_sz));
//                     children_lookup.insert(ent.p.parent(), children.len() - 1);
//                 }
//                 continue;
//             }
    
//             // Once the traversed total + curr total EXCEED per thread limit, add them to a thread
//             let mut i = 0;
//             while i < children.len()-1 {
//                 if children[i].0.len() == 0 {
//                     i += 1;
//                     continue;
//                 }
//                 let parent = children[i].0[0].parent();
//                 match parent {
//                     Some(p) => {
//                         thread_paths[th_idx].push(p.to_path_buf());
//                         added_sizes.insert(p.to_path_buf(), children[i].1);
//                     }
//                     None => {/* This is root */}
//                 }
//                 children[i].0.clear();
    
//                 i += 1;
    
//                 // TODO: Need to subtract added children from the parent
    
//             }
//             let last_children_set = &children[i].0;
//             thread_paths[th_idx].extend_from_slice(&last_children_set);
//             if last_children_set.len() > 0 {
//                 if let Some(parent) = children[i].0[0].parent() {
//                     added_sizes.insert(parent.to_path_buf(), children[i].1);
//                 }
//             }
            
//             curr_sz_sum = 0;
//             th_idx = (th_idx + 1) % thread_limit;
//         }
//         if i as usize == offsets.len() - 1 {
//             let last_idx = thread_paths.len() - 1;
//             thread_paths[last_idx].push(target_path.clone());
//         } 
        
//         let bef = std::time::Instant::now();
//         let mut hs: Vec<JoinHandle<()>> = Vec::new();
//         let mut all_entries: Vec<CDirEntry> = Vec::new();
//         let mut df_count = 0;
    
//         // let mut th_entries: Vec<Vec<CDirEntry>> = Vec::with_capacity(thread_paths.len());
//         for i in 0..thread_paths.len() {
//             let curr_paths = thread_paths[i].clone();
//             // th_entries.push(vec![]);
    
//             // Create skip sets for this thread
//             let mut skip: HashSet<PathBuf> = HashSet::new();
//             for j in 0.. thread_paths.len() {
//                 if j == i {
//                     continue;
//                 }
//                 for p in &thread_paths[j] {
//                     skip.insert(p.to_path_buf());
//                 }
//             }
    
//             hs.push(thread::spawn(move || {
//                 for r in curr_paths {
//                     let mut df: Vec<CDirEntry> = Vec::new();
//                     let mut path_idx_map: HashMap<std::path::PathBuf, usize> = HashMap::new();
    
//                     // TODO: Handle errors
//                     if is_recursive {
//                         walk_rec(r.to_path_buf(), &mut df, &mut path_idx_map, &mut skip, 0);
//                     } else {
//                         let df = walk_iter(r.to_path_buf(), &mut skip);
//                     }
//                     // &th_entries[i].extend_from_slice(&df);
//                 }
//             }));
//         }
//         for i in hs {
//             i.join().unwrap();
//         }
//         println!("df count: {}", df_count);    
//         println!("Time to walk all directories is: {}ms", bef.elapsed().as_millis());
//     }
//     Ok(())
// }