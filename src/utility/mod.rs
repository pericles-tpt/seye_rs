use std::{cmp::Ordering, collections::HashSet, num, path::PathBuf, sync::mpsc, thread::JoinHandle};

pub const KILOBYTE: usize = 1024;
pub const MEGABYTE: usize = KILOBYTE * 1024;
pub const GIGABYTE: usize = MEGABYTE * 1024;

// Main -> Thread, messaging aliases
const MT_EXIT:      usize = 0;
const MT_NEW_PATHS: usize = 1;

// Thread -> Main, messaging aliases
const TM_NEW_PATHS: i32 = -1;
const TM_NO_PATHS: i32  = 0;
// > 0, reserved for "num items left"

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

// thread_from_root, a function for performing operations across multiple threads where:
//  - each thread does operations iteratively
//  - each thread will return "leftover" items to the main thread once it reaches a `yield limit`
//  - the main thread will redistribute "leftover" items between threads
//  - once ALL threads are done, the main thread will send a message to each thread to terminate
pub fn thread_from_root<T: Clone + std::marker::Send + 'static, U: std::marker::Send + 'static, V: std::marker::Send + 'static + Clone>(
    root: T, 
    skip_set: HashSet<T>,
    find_target: &V,
    num_threads: usize, 
    num_thread_iterations_before_yield: usize,
    thread_collect_fn: Option<fn (input: &mut Vec<T>, skip_set: &HashSet<T>, output: &mut Vec<U>, limit: usize) -> std::io::Result<Vec<T>>>,
    thread_find_fn: Option<fn (target: &V, input: &mut Vec<T>, skip_set: &HashSet<T>, output: &mut Vec<U>, limit: usize) -> std::io::Result<Vec<T>>>,
    sort_output_items: fn (a: &U, b: &U) -> Ordering,
) -> std::io::Result<Vec<U>> {
    let mut res: Vec<U> = Vec::new();

    // Do first pass of thread_*_fn() on root to get multiple items
    let mut initial_input = vec![root];
    let maybe_initial_items: std::io::Result<Vec<T>>;
    if thread_collect_fn.is_some() {
        maybe_initial_items = thread_collect_fn.unwrap()(&mut initial_input, &skip_set, &mut res, num_thread_iterations_before_yield);
    } else {
        maybe_initial_items = thread_find_fn.unwrap()(find_target, &mut initial_input, &skip_set, &mut res, num_thread_iterations_before_yield);
    }
    if maybe_initial_items.is_err() {
        return Err(std::io::Error::new(std::io::ErrorKind::Other, format!("Failed to read root path: {:?}", maybe_initial_items.err())))
    }
    let mut initial_output = maybe_initial_items.unwrap();
    let mut paths_to_distribute: Vec<T> = Vec::with_capacity(initial_output.len());
    paths_to_distribute.append(&mut initial_output);
    
    // Spin up threads to, iterate over items and inform main if: they have excess paths to return OR they're done
    let mut thread_handles: Vec<JoinHandle<Vec<U>>> = Vec::with_capacity(num_threads);
    let mut thread_to_m_chans: Vec<(mpsc::Sender<(i32, Vec<T>)>, mpsc::Receiver<(i32, Vec<T>)>)> = Vec::with_capacity(num_threads);
    let mut m_to_thread_chans: Vec<mpsc::Sender<(usize, Vec<T>)>> = Vec::with_capacity(num_threads);
    for i in 0..num_threads {
        let thread_to_m_chan = mpsc::channel::<(i32, Vec<T>)>();
        thread_to_m_chans.push(thread_to_m_chan);
        let m_to_thread_chan = mpsc::channel::<(usize, Vec<T>)>();
        m_to_thread_chans.push(m_to_thread_chan.0);

        let thread_tx = thread_to_m_chans[i].0.clone();
        let thread_rx = m_to_thread_chan.1;
        let target = find_target.clone();
        let skip = skip_set.clone();

        let hndl: JoinHandle<Vec<U>> = std::thread::spawn(move || {
            let mut buf: Vec<T> = Vec::new();
            let mut results: Vec<U> = Vec::new();
            loop {
                let mut new_paths_len = 0;
                if buf.len() > 0 {
                    let maybe_send_to_main: std::io::Result<Vec<T>>;
                    if thread_collect_fn.is_some() {
                        maybe_send_to_main = thread_collect_fn.unwrap()(&mut buf, &skip, &mut results, num_thread_iterations_before_yield);
                    } else {
                        maybe_send_to_main = thread_find_fn.unwrap()(&target, &mut buf, &skip, &mut results, num_thread_iterations_before_yield);
                    }
                    if maybe_send_to_main.is_err() {
                        // TODO:
                    }

                    let new_paths = maybe_send_to_main.unwrap();
                    new_paths_len = new_paths.len();
                    if new_paths_len > 0 {
                        let mut sent = false;
                        while !sent {
                            let res= thread_tx.send((TM_NEW_PATHS, new_paths.clone()));
                            sent = !res.is_err();
                        }
                    }
                }

                if new_paths_len == 0 {
                    let mut sent = false;
                    while !sent {
                        let send_res = thread_tx.send((TM_NO_PATHS, Vec::new()));
                        sent = !send_res.is_err();
                    }
                    results.sort_by(sort_output_items);
                }

                let maybe_msg = thread_rx.recv();
                if !maybe_msg.is_err() {
                    let msg = maybe_msg.unwrap();
                    match msg.0 {
                        MT_EXIT => {
                            break;
                        }
                        MT_NEW_PATHS => {
                            let mut new_paths = msg.1.clone();
                            buf.append(&mut new_paths);
                        }
                        default => {}
                    }
                }
            }
            return results;
        });
        thread_handles.push(hndl);
    }
    
    // Spin up main thread to check for messages from other threads
    let mut ready_thread_idxs: Vec<usize> = Vec::with_capacity(num_threads);
    loop {
        // Check for messages from other friends to determine if: paths returned OR no excess paths
        let mut all_threads_stopped = true;
        for ti in 0..thread_to_m_chans.len() {
            let maybe_msg = thread_to_m_chans[ti].1.try_recv();
            let busy = maybe_msg.is_err();
            if !busy {
                let msg = maybe_msg.unwrap();
                if msg.0 != TM_NO_PATHS {
                    all_threads_stopped = false;
                }

                if msg.0 == TM_NEW_PATHS {
                    let mut new_paths = msg.1.clone();
                    paths_to_distribute.append(&mut new_paths);
                }
                ready_thread_idxs.push(ti);
            }
        }
        all_threads_stopped = all_threads_stopped && ready_thread_idxs.len() == num_threads;
        
        // No paths left to distribute -> kill all threads
        let curr_paths = paths_to_distribute.clone();
        if curr_paths.len() == 0 {
            if all_threads_stopped {
                for sr in m_to_thread_chans {
                    sr.send((MT_EXIT, Vec::new()));
                }
                break
            }
            continue;
        }

        // Distribute excess paths from threads back to threads (round robin)
        let num_ready = ready_thread_idxs.len();
        if num_ready > 0 {
            let min_paths_per_thread = curr_paths.len() / num_ready;
            let mut rem_paths = curr_paths.len() - (min_paths_per_thread * num_ready);
            let mut curr_path_start_idx = 0;
            for ri in 0..num_ready {
                let to_thread = ready_thread_idxs[ri];
                let mut num_thread_paths = min_paths_per_thread;
                if rem_paths > 0 {
                    num_thread_paths += 1;
                    rem_paths -= 1;
                }
    
                let thread_paths = curr_paths[curr_path_start_idx..curr_path_start_idx + num_thread_paths].to_vec();
    
                m_to_thread_chans[to_thread].send((MT_NEW_PATHS, thread_paths));
                curr_path_start_idx += num_thread_paths;
            }

            paths_to_distribute.drain(0..curr_paths.len());
            ready_thread_idxs.clear();
        }
    }

    // Join finished threads and retrieve the items from each
    for th in thread_handles {
        let mut th_res = th.join().unwrap();
        res.append(&mut th_res);
    }
    Ok(res)
}