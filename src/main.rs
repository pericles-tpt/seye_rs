mod walk;
mod save;
mod scan;
mod diff;
mod report;
mod utility;

extern crate libc;

const DEFAULT_NUM_THREADS: usize    = 84;
const DEFAULT_FD_LIMIT: usize       = 2048;
const DEFAULT_MIN_DIFF_BYTES: usize = 50 * utility::MEGABYTE;

struct Config {
    num_threads: usize,
    file_dir_limit: usize,
    min_diff_bytes: usize,
    show_perf_info: bool,
    show_moved_files: bool,
    cache_merged_diff: bool,
    maybe_start_report_time: Option<std::time::SystemTime>,
    maybe_end_report_time: Option<std::time::SystemTime>
}

fn main() {
    let mut cfg = Config {
        num_threads:             DEFAULT_NUM_THREADS,
        file_dir_limit:          DEFAULT_FD_LIMIT,
        min_diff_bytes:          DEFAULT_MIN_DIFF_BYTES,
        show_perf_info:          false,
        show_moved_files:        false,
        cache_merged_diff:       false,
        maybe_start_report_time: None,
        maybe_end_report_time:   None,
    };

    let args: Vec<String> = std::env::args().collect();
    if args.len() <= 1 || args[0].len() == 0 {
        eprintln!("no arguments provided, for a list of commands add the --help argument");
        return;
    }

    let is_root  = unsafe { libc::geteuid() == 0 };
    let cmd      = args[1].as_str();
    let params: Vec<_> = args.iter().skip(2).collect();
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
            let res = scan::scan(target_pb, output_pb, cfg.min_diff_bytes, cfg.num_threads, cfg.file_dir_limit, cfg.cache_merged_diff);
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
            if is_root && !su_exists.unwrap() {
                eprintln!("no 'su' records exist for path: {:?}", output_pb);
                return;
            }
            if is_root {
                output_pb = su_path;
            }
            
            let res = report::report_changes(target_pb, output_pb, cfg);
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
            eprintln!("invalid command '{}' provided, must be one of: {}", cmd, vec!["scan", "report", "--help"].join(", "));
            return;
        }
    }
    return;
}

fn validate_get_pathbuf(p: &String) -> std::io::Result<std::path::PathBuf> {
    let exists = std::fs::exists(p)?;
    if !exists {
        return Err(std::io::Error::new(std::io::ErrorKind::NotFound, format!("provided path '{}', does not exist", p)));
    }
    return Ok(std::path::PathBuf::from(&p));
}

fn eval_optional_args(cmd: &str, args: Vec<&&String>, cfg: &mut Config) -> std::io::Result<()> {    
    let mut i = 0;
    let valid_command_options = vec!["-p", "-md", "-t", "-fdl", "-mvs", "--start-report", "--end-report", "--cache-merged-diff"];
    let local_tz_offset_secs = chrono::Local::now().offset().local_minus_utc();
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
                    "--cache-merged-diff" => {
                        cfg.cache_merged_diff = true;
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
                        let maybe_threads: Result<usize, std::num::ParseIntError> = args[i].parse();
                        if maybe_threads.is_err() || maybe_threads.clone().unwrap() < 2 {
                            return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "invalid thread argument, must be at least 2"));
                        }
                        cfg.num_threads = maybe_threads.unwrap();
                    }
                    "-fdl" => {
                        let maybe_file_dir_limit: Result<usize, std::num::ParseIntError> = args[i].parse();
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
                    "--start-report" => {
                        let maybe_start_report: Result<String, std::string::ParseError> = args[i].parse();
                        if maybe_start_report.is_err() {
                            return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "invalid start report argument, must be an ISO 8601 datetime without timezone"));
                        }
                        let maybe_datetime = utility::datetime_from_iso8601_without_tz(maybe_start_report.unwrap().as_str(), local_tz_offset_secs);
                        if maybe_datetime.is_err() {
                            return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "invalid start report argument, must be an ISO 8601 datetime without timezone"));
                        }
                        cfg.maybe_start_report_time = Some(std::time::SystemTime::from(maybe_datetime.unwrap()));
                    }
                    "--end-report" => {
                        let maybe_end_report: Result<String, std::string::ParseError> = args[i].parse();                    
                        if maybe_end_report.is_err() {
                            return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "invalid end report argument, must be an ISO 8601 datetime without timezone"));
                        }
                        let maybe_datetime = utility::datetime_from_iso8601_without_tz(maybe_end_report.unwrap().as_str(), local_tz_offset_secs);
                        if maybe_datetime.is_err() {
                            return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "invalid end report argument, must be an ISO 8601 datetime without timezone"));
                        }
                        cfg.maybe_end_report_time = Some(std::time::SystemTime::from(maybe_datetime.unwrap()));
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
    println!("Storage eye, identifies changes in disk usage and moved files in a target directory through scanning over time

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
    -mvs                                    Show moved files in the report output (even though the size of a MOVE is 0B)

    --start-report        (default: first)  Specifies the earliest diff that will be included in the report (format: 2025-05-05T10:00:00, uses system timezone)
    --end-report          (default:  last)  Specifies the latest diff that will be included in the report (format: 2025-05-05T10:00:00, uses system timezone)
", 
    DEFAULT_NUM_THREADS, DEFAULT_FD_LIMIT);
}