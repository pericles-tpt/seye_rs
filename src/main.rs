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

const DEFAULT_NUM_THREADS: usize    = 84;
const DEFAULT_FD_LIMIT: usize       = 384; // Was 2048, which performs better when NOT hashing
const DEFAULT_MIN_DIFF_BYTES: usize = 50 * utility::MEGABYTE;

// TODO: Add `-mvs` (move_show) flag, that shows Move'd items (that normally wouldn't be shown since a `Move` is a 0B diff)
//       set the default to `false`
struct Config {
    num_threads: usize,
    file_dir_limit: usize,
    min_diff_bytes: usize,
    // scan_hidden: bool,
    show_perf_info: bool,
    // sorted: bool,
    move_depth_threshold: i32,
    show_moved_files: bool,
}

fn main() {
    let mut cfg = Config {
        num_threads:          DEFAULT_NUM_THREADS,
        file_dir_limit:       DEFAULT_FD_LIMIT,
        min_diff_bytes:       DEFAULT_MIN_DIFF_BYTES,
        // scan_hidden:          true,
        show_perf_info:       false,
        // sorted:               false,
        move_depth_threshold: 0,
        show_moved_files:     false,
    };

    let args: Vec<String> = env::args().collect();
    if args.len() <= 1 || args[0].len() == 0 {
        eprintln!("no arguments provided, for a list of commands add the --help argument");
        return;
    }

    // TODO: This is just here to silence a compiler warning, remove this and come up with a better solution
    let _ = utility::get_cwd();

    let is_root  = unsafe { libc::geteuid() == 0 };
    let cmd      = args[1].as_str();
    let params: Vec<_> = args.iter().skip(2).collect();
    let valid_commands_options = vec!["-p", "-md", "-t", "-fdl"];
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
            let arg_eval_res = eval_optional_args("scan", optional_args, &mut cfg);
            if arg_eval_res.is_err() {
                eprintln!("invalid argument provided: {}", arg_eval_res.err().unwrap());
                return;
            }
            
            // Scanning on 1 additional thread doesn't justify the performance overhead
            if cfg.num_threads == 1 {
                println!("WARNING: `num_threads` set to 1, setting `num_threads` to 0 (1 extra thread isn't worth the performance overhead)");
                cfg.num_threads = 0;
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
            let res = scan::scan(target_pb, output_pb, cfg.min_diff_bytes, cfg.num_threads, cfg.file_dir_limit);
            let took = bef.elapsed();
            match res {
                Ok((num_files, num_dirs)) => {
                    if cfg.show_perf_info {
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
                eprintln!("insufficient arguments for `report`, expected at least [INPUT SCAN PATH] and [OUTPUT SCAN FILE PATH]");
                return;
            }
            let optional_args: Vec<_> = params.iter().collect();
            let num_args = optional_args.len();

            // Get optional params
            let arg_eval_res = eval_optional_args("report", optional_args, &mut cfg);
            if arg_eval_res.is_err() {
                eprintln!("invalid argument provided: {}", arg_eval_res.err().unwrap());
                return;
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
            
            let res = report::report_changes(target_pb, output_pb, cfg.move_depth_threshold, cfg.show_moved_files);
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

fn eval_optional_args(cmd: &str, args: Vec<&&String>, cfg: &mut Config) -> std::io::Result<()> {    
    let mut i = 0;
    let valid_command_options = vec!["-p", "-md", "-t", "-fdl", "-mvd", "-mvs"];
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
                        cfg.show_perf_info = true;
                    }
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
                    "-md" => {
                        let maybe_min_diff_bytes = utility::get_bytes_from_arg(args[i]);
                        if maybe_min_diff_bytes.is_err() {
                            // Try to parse as u64 num bytes
                            let maybe_min_diff_bytes_raw = args[i].parse::<usize>();
                            if maybe_min_diff_bytes_raw.is_err() {
                                return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, format!("invalid min diff bytes argument, failed to parse as shorthand (e.g. 10M, 1G, etc) or raw bytes (e.g. 1000)")));
                            }
                            cfg.min_diff_bytes = maybe_min_diff_bytes_raw.unwrap();
                        } else {
                            cfg.min_diff_bytes = maybe_min_diff_bytes.unwrap();
                        }
                    }
                    "-t" => {
                        let maybe_threads: Result<usize, ParseIntError> = args[i].parse();
                        if maybe_threads.is_err() || maybe_threads.clone().unwrap() < 1 {
                            return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "invalid thread argument, must be a non-negative integer"));
                        }
                        cfg.num_threads = maybe_threads.unwrap();
                    }
                    "-fdl" => {
                        let maybe_file_dir_limit: Result<usize, ParseIntError> = args[i].parse();
                        if maybe_file_dir_limit.is_err() || maybe_file_dir_limit.clone().unwrap() < 1 {
                            return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "invalid thread add dir limit argument, must be a non-negative integer"));
                        }
                        cfg.file_dir_limit = maybe_file_dir_limit.unwrap();
                    }
                    _ => {
                        if before_directory_args {
                            return Err(std::io::Error::new(std::io::ErrorKind::Other, format!("unimplemented parameter: {}, for command: {}", a, cmd)));
                        }
                    }
                }
            }
            "report" => 'report: {
                // NO VALUE OPTIONS
                let mut is_no_val_opt = true;
                match a {
                    "-mvs" => {
                        cfg.show_moved_files = true;
                    }
                    _ => {is_no_val_opt = false;}
                }
                if is_no_val_opt {
                    break 'report;
                }

                // ONE VALUE OPTIONS
                i += 1;
                if i >= args.len() {
                    return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, format!("missing additional argument for '{}' flag", a)));
                }
                match a {
                    "-mvd" => {
                        let maybe_move_depth_threshold: Result<i32, ParseIntError> = args[i].parse();
                        if maybe_move_depth_threshold.is_err() {
                            return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "invalid move depth threshold argument, must be a non-negative integer"));
                        }
                        cfg.move_depth_threshold = maybe_move_depth_threshold.unwrap();
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

fn print_help_text() {
    println!("Storage eye, scans for items in your filesystem. It (mostly) performs best with NO optional args.

Usage: seye scan [options] [pattern] [path]
       seye report [options] [pattern] [path]
Scan Arguments:
    --help                                  Prints help
    --version                               Prints version

    -p                                      Show performance statistics after scan
    -md                   (default:  50MB)  Specify the minimum size difference to include in diffs, can specify one of: n, nK, nM or nG, e.g. 1M
    
    -t   <num>            (default:    {})  Specify the number of threads, MUST BE >= 2
    -fdl <num>            (default:  {})  Specify the maximum 'files + dirs' to traverse before returning results from each thread
Report Arguments:
    -mvd <num>            (default:     0)  Specifies the maximum directory depth difference for two matching entries in different locations to be
                                            classified as a MOVE, otherwise they're treated as separate REMOVEs and ADDs
                                            e.g. In the following case an `-mvd` value >= 3 will classify this as a MOVE
                                                 a:    4     3    2   1     0            4     3    2   1     0
                                                 a: /jumps/over/the/lazy/dog.txt, b: /jumps/under/the/lazy/dog.txt

    -mvs                                    Show moved files in the report output (even though the size of a MOVE is 0B)
", 
    DEFAULT_NUM_THREADS, DEFAULT_FD_LIMIT);
}