use std::path::PathBuf;

pub const KILOBYTE: usize = 1024;
pub const MEGABYTE: usize = KILOBYTE * 1024;
pub const GIGABYTE: usize = MEGABYTE * 1024;

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