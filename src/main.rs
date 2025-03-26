mod walk;
mod save;
mod scan;
mod diff;
mod report;
mod utility;

extern crate libc;

use std::num::ParseIntError;
use std::path::PathBuf;
use std::env;
use report::report_changes;
use scan::scan;
use utility::{get_cwd, MEGABYTE};

const DEFAULT_NUM_THREADS: usize = 84;
const DEFAULT_FD_LIMIT: usize = 2048;
const MIN_MEMORY_LIMIT: usize = 10 * MEGABYTE;

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() == 0 || args[0].len() == 0 {
        eprintln!("no arguments provided, for a list of commands add the --help argument");
        return;
    }

    // TODO: This is just here to silence a compiler warning, remove this and come up with a better solution
    let _ = get_cwd();

    let is_root = unsafe { libc::geteuid() == 0 };
    let cmd = args[1].as_str();
    let params: Vec<_> = args.iter().skip(2).collect();
    let valid_commands_options = vec!["-p", "-md", "-t", "-tdl"];
    match cmd {
        "scan" => {
            let mut is_root_msg = "NOT ";
            if is_root {
                is_root_msg = "";
            }
            println!("{}running as SUDO", is_root_msg);

            if params.len() < 2 {
                eprintln!("insufficient arguments for `scan`, expected at least [INPUT SCAN PATH] and [OUTPUT SCAN FILE PATH]");
                return;
            }
            let optional_args: Vec<_> = params.iter().collect();
            let num_args = optional_args.len();

            // Get optional params
            // NOTE: memory_limit == 0 -> no limit
            let mut num_threads = DEFAULT_NUM_THREADS; //0;
            let mut memory_limit: usize = 0;
            // let mut is_recursive = false;
            let mut show_perf_info = false;
            let mut min_diff_bytes: i64 = (50 * MEGABYTE) as i64;
            let mut thread_add_dir_limit = DEFAULT_FD_LIMIT; //256;
            let mut scan_hidden = true;
            let mut sorted = false;
            let arg_eval_res = eval_optional_args("scan", optional_args, &mut show_perf_info, &mut memory_limit, &mut min_diff_bytes, &mut num_threads, &mut thread_add_dir_limit, &mut scan_hidden, &mut sorted);
            if arg_eval_res.is_err() {
                eprintln!("invalid argument provided: {}", arg_eval_res.err().unwrap());
                return;
            }
            
            // Scanning on 1 additional thread doesn't justify the performance overhead
            if num_threads == 1 {
                println!("WARNING: `num_threads` set to 1, setting `num_threads` to 0 (1 extra thread isn't worth the performance overhead)");
                num_threads = 0;
            }

            // Get input scan path
            let maybe_target_str = params[num_args - 2];
            let maybe_target_pb = validate_get_pathbuf(maybe_target_str);
            if maybe_target_pb.is_err() {
                eprintln!("invalid target path provided: {}", maybe_target_pb.err().unwrap());
                return;
            }
            let target_pb = maybe_target_pb.unwrap();

            // Get scan output path
            let maybe_output_pb = validate_get_pathbuf(params[num_args - 1]);
            if maybe_output_pb.is_err() {
                eprintln!("invalid output scan path provided: {}", maybe_output_pb.err().unwrap());
                return;
            }
            let mut output_pb = maybe_output_pb.unwrap();
            
            // Create `su` folder if it doesn't exist
            let mut su_path = output_pb.clone();
            su_path.push("su/");
            let su_exists = std::fs::exists(&su_path);
            if su_exists.is_err() {
                eprintln!("failed to check if 'su' path exists: {:?}", su_exists.err());
                return;
            }
            if !su_exists.unwrap() {
                let res = std::fs::create_dir(&su_path);
                if res.is_err() {
                    eprintln!("failed to create 'su' path: {:?}", res.err());
                    return;
                }
            }
            if is_root {
                output_pb = su_path;
            }

            let bef = std::time::Instant::now();
            let res = scan(target_pb, output_pb, min_diff_bytes, num_threads, thread_add_dir_limit);
            let took = bef.elapsed();
            match res {
                Ok((num_files, num_dirs)) => {
                    if show_perf_info {
                        println!("Scanned {} files, {} directories in: {}ms", num_files, num_dirs, took.as_millis())
                    }
                }
                Err(e) => {
                    eprintln!("error occured while scanning: {}", e);
                }
            }
        }
        "report" => {
            if params.len() < 2 {
                eprintln!("insufficient arguments for `scan`, expected at least [INPUT SCAN PATH] and [OUTPUT SCAN FILE PATH]");
                return;
            }
            let optional_args: Vec<_> = params.iter().collect();
            let num_args = optional_args.len();
            
            // Get input scan path
            let maybe_target_str = params[num_args - 2];
            let maybe_target_pb = validate_get_pathbuf(maybe_target_str);
            if maybe_target_pb.is_err() {
                eprintln!("invalid target path provided: {}", maybe_target_pb.err().unwrap());
                return;
            }
            let target_pb = maybe_target_pb.unwrap();

            // Get scan output path
            let maybe_output_pb = validate_get_pathbuf(params[num_args - 1]);
            if maybe_output_pb.is_err() {
                eprintln!("invalid output scan path provided: {}", maybe_output_pb.err().unwrap());
                return;
            }
            let mut output_pb = maybe_output_pb.unwrap();

            // println!("Running scan of '{}', with {} threads and a {} memory limit", maybe_target_str, num_threads, get_shorthand_memory_limit(memory_limit));
            
            // Create `su` folder if it doesn't exist
            let mut su_path = output_pb.clone();
            su_path.push("su/");
            let su_exists = std::fs::exists(&su_path);
            if su_exists.is_err() {
                eprintln!("failed to check if 'su' path exists: {:?}", su_exists.err());
                return;
            }
            if !su_exists.unwrap() {
                let res = std::fs::create_dir(&su_path);
                if res.is_err() {
                    eprintln!("failed to create 'su' path: {:?}", res.err());
                    return;
                }
            }
            if is_root {
                output_pb = su_path;
            }
            
            let res = report_changes(target_pb, output_pb);
            match res {
                Ok(()) => {}
                Err(e) => {
                    eprintln!("error occured while reporting: {}", e);
                }
            }
        }
        "--help" => {
            print_help_text();
        }
        _ => {
            eprintln!("invalid command '{}' provided, must be one of: {}", cmd, valid_commands_options.join(", "));
            return;
        }
    }
    return;
}

fn validate_get_pathbuf(p: &String) -> std::io::Result<PathBuf> {
    let exists = std::fs::exists(p)?;
    if !exists {
        return Err(std::io::Error::new(std::io::ErrorKind::NotFound, format!("provided path '{}', does not exist", p)));
    }
    return Ok(PathBuf::from(&p));
}

fn eval_optional_args(cmd: &str, args: Vec<&&String>, show_perf_info: &mut bool, memory_limit: &mut usize, min_diff_bytes: &mut i64, num_threads: &mut usize, thread_add_dir_limit: &mut usize, show_hidden: &mut bool, sorted: &mut bool) -> std::io::Result<()> {    
    let mut i = 0;
    let valid_command_options = vec!["-p", "-md", "-t", "-fdl"];
    while i < args.len() {
        let before_directory_args = i < args.len() - 2;
        let a = args[i].as_str();
        if before_directory_args && !valid_command_options.contains(&a) {
            let valid_params: Vec<_> = valid_command_options.clone().into_iter().collect();
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, format!("invalid parameter '{}' provided for {} command, must be one of: {}", a, cmd, valid_params.join(", "))));
        }
    
        match cmd {
            "scan" => 'scan: {
                // NO VALUE OPTIONS
                let mut is_no_val_opt = true;
                match a {
                    "-p" => {
                        *show_perf_info = true;
                    }
                    // "-r" => {
                    //     *is_recursive = true;
                    // }
                    _ => {is_no_val_opt = false;}
                }
                if is_no_val_opt {
                    break 'scan;
                }

                // ONE VALUE OPTIONS
                i += 1;
                if i >= args.len() {
                    return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, format!("missing additional argument for '{}' flag", a)));
                }
                match a {
                    "-m" => {
                        let maybe_memory_limit = get_bytes_from_arg(args[i]);
                        if maybe_memory_limit.is_err() {
                            return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, format!("invalid memory limit argument, {}", maybe_memory_limit.err().unwrap())));
                        }
                        *memory_limit = maybe_memory_limit.unwrap();
                        if *memory_limit < MIN_MEMORY_LIMIT {
                            return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, format!("memory limit too low must be at least {}M", MIN_MEMORY_LIMIT / MEGABYTE)));
                        }
                    }
                    "-md" => {
                        let maybe_min_diff_bytes = get_bytes_from_arg(args[i]);
                        if maybe_min_diff_bytes.is_err() {
                            // Try to parse as u64 num bytes
                            let maybe_min_diff_bytes_raw = args[i].parse::<i64>();
                            if maybe_min_diff_bytes_raw.is_err() {
                                return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, format!("invalid min diff bytes argument, failed to parse as shorthand (e.g. 10M, 1G, etc) or raw bytes (e.g. 1000)")));
                            }
                            *min_diff_bytes = maybe_min_diff_bytes_raw.unwrap();
                        } else {
                            *min_diff_bytes = maybe_min_diff_bytes.unwrap() as i64;
                        }
                    }
                    "-t" => {
                        let maybe_threads: Result<usize, ParseIntError> = args[i].parse();
                        if maybe_threads.is_err() || maybe_threads.clone().unwrap() < 1 {
                            return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "invalid thread argument, must be a non-negative integer"));
                        }
                        *num_threads = maybe_threads.unwrap();
                    }
                    "-tdl" => {
                        let maybe_thread_add_dir_limit: Result<usize, ParseIntError> = args[i].parse();
                        if maybe_thread_add_dir_limit.is_err() || maybe_thread_add_dir_limit.clone().unwrap() < 1 {
                            return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "invalid thread add dir limit argument, must be a non-negative integer"));
                        }
                        *thread_add_dir_limit = maybe_thread_add_dir_limit.unwrap();
                    }
                    _ => {
                        if before_directory_args {
                            return Err(std::io::Error::new(std::io::ErrorKind::Other, format!("unimplemented parameter: {}, for command: {}", a, cmd)));
                        }
                    }
                }
            }
            "find" => 'find: {
                // NO VALUE OPTIONS
                let mut is_no_val_opt = true;
                match a {
                    "-h" => {
                        *show_hidden = true;
                    }
                    "-s" => {
                        *sorted = true;
                    }
                    _ => {is_no_val_opt = false;}
                }
                if is_no_val_opt {
                    break 'find;
                }

                // ONE VALUE OPTIONS
                i += 1;
                if i >= args.len() {
                    return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, format!("missing additional argument for '{}' flag", a)));
                }
                match a {
                    "-t" => {
                        let maybe_threads: Result<usize, ParseIntError> = args[i].parse();
                        if maybe_threads.is_err() || maybe_threads.clone().unwrap() < 1 {
                            return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "invalid thread argument, must be a non-negative integer"));
                        }
                        *num_threads = maybe_threads.unwrap();
                    }
                    "-fdl" => {
                        let maybe_thread_add_dir_limit: Result<usize, ParseIntError> = args[i].parse();
                        if maybe_thread_add_dir_limit.is_err() || maybe_thread_add_dir_limit.clone().unwrap() < 1 {
                            return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "invalid thread add dir limit argument, must be a non-negative integer"));
                        }
                        *thread_add_dir_limit = maybe_thread_add_dir_limit.unwrap();
                    }
                    _ => {
                        if before_directory_args {
                            return Err(std::io::Error::new(std::io::ErrorKind::Other, format!("unimplemented parameter: {}, for command: {}", a, cmd)));
                        }
                    }
                }
            }
            _ => {
                return Err(std::io::Error::new(std::io::ErrorKind::Other, format!("unimplemented command: {}", cmd)));
            }
        }

        i += 1;
    }
 
    Ok(())    
}

fn get_bytes_from_arg(a: &String) -> std::io::Result<usize> {
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

fn print_help_text() {
    println!("Pretty Fast Scan, scans for items in your filesystem. It (mostly) performs best with NO optional args.

Usage: pfs scan [options] [pattern] [path]
       pfs report [options] [pattern] [path]
Scan Arguments:
    --help                                  Prints help
    --version                               Prints version

    -p                                      Show performance statistics after scan
    -md                   (default:  50MB)  Specify the minimum size difference to include in diffs, can specify one of: n, nK, nM or nG, e.g. 1M
    
    -t   <num>            (default:    {})  Specify the number of threads, MUST BE >= 2
    -fdl <num>            (default:  {})  Specify the maximum 'files + dirs' to traverse before returning results from each thread", 
    DEFAULT_NUM_THREADS, DEFAULT_FD_LIMIT);
}