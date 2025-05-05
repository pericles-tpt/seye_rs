use std::{collections::HashSet, path::PathBuf};
use rayon::iter::{IntoParallelRefMutIterator, ParallelIterator};
use crate::walk::{walk_collect_until_limit, CDirEntry};

pub const KILOBYTE: usize = 1024;
pub const MEGABYTE: usize = 1024 * KILOBYTE;
pub const GIGABYTE: usize = 1024 * MEGABYTE;

pub fn get_shorthand_memory_limit(amount: i64) -> String {
    if amount == 0 {
        return format!("unlimited");
    }
    let mut sign = "+";
    let mut amount_abs = amount as usize;
    if amount < 0 {
        sign = "-";
        amount_abs = (amount * -1) as usize;
    }

    let mut unit = "K";
    let mut mult = KILOBYTE;
    if amount_abs >= MEGABYTE {
        unit = "M";
        mult = MEGABYTE;
        if amount_abs >= GIGABYTE {
            unit = "G";
            mult = GIGABYTE
        }
    }
    return format!("{}{}{}", sign, amount_abs / mult, unit)
}

pub fn get_bytes_from_arg(a: &String) -> std::io::Result<usize> {
    // Expecting string of the form: 500M, 2G, etc
    let memory_shorthand = a.as_str();
    if memory_shorthand.len() < 2 {
        return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "must be at least 2 characters, e.g. 2G"));
    }

    // Get quantity
    let maybe_num_str = &memory_shorthand[0..memory_shorthand.len()-1];
    let maybe_num = maybe_num_str.parse::<usize>();
    if maybe_num.is_err() || maybe_num.clone().unwrap() < 1 {
        return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "the number preceding the last character must be a non-negative integer"));
    }

    // Get unit
    let unit = memory_shorthand.chars().last().unwrap();
    if unit != 'M' && unit != 'G' {
        return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "must end with a valid unit either 'M' (megabytes) or 'G' (gigabytes)"));
    }

    let mut ret = maybe_num.unwrap() * 1024 * 1024;
    if unit == 'G' {
        ret *= 1024;
    } 
    Ok(ret)
}

pub fn get_cwd () -> PathBuf {
    let wd_or_err = std::env::current_dir();
    match wd_or_err {
        Ok(wd) => {
            return wd;
        }
        Err(e) => {
            panic!("error getting cwd: {}", e);
        }
    }
}

pub fn collect_from_root(
    root: PathBuf, 
    skip_set: HashSet<PathBuf>,
    num_threads: usize, 
    num_thread_iterations_before_yield: usize,
) -> std::io::Result<Vec<CDirEntry>> {
    let mut res: Vec<CDirEntry> = Vec::new();

    // Do first pass of thread_*_fn() on root to get multiple items
    let mut initial_dirs = vec![root];
    let maybe_initial_paths: std::io::Result<Vec<PathBuf>> = walk_collect_until_limit(&mut initial_dirs, &skip_set, &mut res, num_thread_iterations_before_yield);
    let Ok(mut paths_to_distribute) = maybe_initial_paths else {
        return Err(std::io::Error::new(std::io::ErrorKind::Other, format!("Failed to read root path: {:?}", maybe_initial_paths.err())))
    };
    
    // Spin up threads to, iterate over items and inform main if: they have excess paths to return OR they're done
    loop {
        // Redistribute paths
        let mut curr_num_threads = num_threads;
        if paths_to_distribute.len() < curr_num_threads {
            curr_num_threads = paths_to_distribute.len();
        }
        let mut paths_per_thread = distribute_paths_per_thread(&mut paths_to_distribute, curr_num_threads);

        // Start "walk" on auxiliary threads
        let new_dirs_and_results: (Vec<Vec<PathBuf>>, Vec<Vec<CDirEntry>>) = paths_per_thread.par_iter_mut().map(|p| {
            let mut new_entries = vec![];
            let Ok(leftover_paths) = walk_collect_until_limit(p, &skip_set, &mut new_entries, num_thread_iterations_before_yield)
            else {
                return (vec![], vec![]);
            };
            return (leftover_paths, new_entries);
        }).unzip();

        // Retrieve paths to distribute and add to all_results    
        paths_to_distribute = new_dirs_and_results.0.into_iter().flatten().collect();
        res.append(&mut new_dirs_and_results.1.into_iter().flatten().collect());
        if paths_to_distribute.len() == 0 {
            break;
        }
    }
    
    Ok(res)
}

fn distribute_paths_per_thread(paths_to_distribute_and_free: &mut Vec<PathBuf>, num_threads: usize) -> Vec<Vec<PathBuf>> {
    // distribute paths such that each thread gets a "fair" allocation of low and high index elements
    let max_num_paths_per_thread = (paths_to_distribute_and_free.len() / num_threads) + 1;
    let mut paths_per_thread: Vec<Vec<PathBuf>> = vec![Vec::with_capacity(max_num_paths_per_thread); num_threads];
    for i in 0..num_threads {
        for j in 0..max_num_paths_per_thread {
            let take_idx = (j * num_threads) + i;
            if take_idx >= paths_to_distribute_and_free.len() {
                break;
            }
            paths_per_thread[i].push(paths_to_distribute_and_free[take_idx].clone());
        }
    }
    
    // the original data is no longer needed, free it
    paths_to_distribute_and_free.clear();
    paths_to_distribute_and_free.shrink_to_fit();

    return paths_per_thread;
}