use crate::walk;
use crate::utility;

pub const NUM_DT: usize = 3;
pub const ADD_DT_IDX: usize = 0;
pub const REM_DT_IDX: usize = 1;
pub const MOD_DT_IDX: usize = 2;

pub fn ignore_file_entry(f: &FileEntryDiff) -> bool {
    return f.bn.capacity() == 0;
}

pub fn ignore_dir_entry(d: &CDirEntryDiff) -> bool {
    return d.p.capacity() == 0;
}

pub fn get_diff_type_shorthand(id: usize) -> String {
    let mut ret = "???";
    match id {
        0 => {
            ret = "ADD";
        }
        1 => {
            ret = "REM";
        }
        2 => {
            ret = "MOD";
        }
        _ => {}
    }
    return String::from(ret);
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct FileEntryDiff {
    pub bn: std::ffi::OsString,
    pub sz: i128,
    pub t_diff: TDiff,
}
impl Default for FileEntryDiff {
    fn default() -> Self {
        FileEntryDiff {
            bn: std::ffi::OsString::new(),
            sz: 0,
            t_diff: TDiff{
                s_diff: 0,
                ns_diff: 0,
            },
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct TDiff {
    pub s_diff: i64,
    pub ns_diff: i128,
}

#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub struct DiffFile {
    pub has_merged_diff: bool,
    pub timestamps: Vec<std::time::SystemTime>,
    pub entries: Vec<DiffEntry>,
}
#[derive(serde::Serialize, serde::Deserialize, Default, Clone, Debug)]
pub struct DiffEntry {
    // 0 -> add, 1 -> rem, 2 -> mod
    pub diffs: [Vec<CDirEntryDiff>; NUM_DT],
    pub move_to_paths: std::collections::HashMap<std::path::PathBuf, std::path::PathBuf>,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct CDirEntryDiff {
    pub p: std::path::PathBuf,
    pub t_diff: TDiff,

    pub files_here: usize,
    pub files_below: usize,
    pub dirs_here: usize,
    pub dirs_below: usize,
    pub size_here: i64,
    pub size_below: i64,

    pub files: [Vec<FileEntryDiff>; NUM_DT],
    pub symlinks: [Vec<FileEntryDiff>; NUM_DT]
}

pub fn add_diffs_to_items<I: Clone + std::fmt::Debug + PartialEq, D: Clone + std::fmt::Debug>(
    items: &mut Vec<I>, 
    add_rem_mod_diffs: &mut [Vec<D>; NUM_DT], 
    items_sort: fn(a: &I, b: &I) -> std::cmp::Ordering,
    item_diff_match: fn(it: &I, d: &D) -> bool,
    ignore_diff: fn(d: &D) -> bool,
    get_item_from_diff: fn(d: D) -> I,
    add_diff_to_item: fn(it: &mut I, d: D) -> (),
) -> std::io::Result<()> {
    if items.len() == 0 || (add_rem_mod_diffs[ADD_DT_IDX].len() == 0 && add_rem_mod_diffs[REM_DT_IDX].len() == 0 && add_rem_mod_diffs[MOD_DT_IDX].len() == 0) {
        return Ok(());
    }
    
    // Pre-filter diffs to ignore
    add_rem_mod_diffs[ADD_DT_IDX].retain(|d|!ignore_diff(d));
    add_rem_mod_diffs[REM_DT_IDX].retain(|d|!ignore_diff(d));
    add_rem_mod_diffs[MOD_DT_IDX].retain(|d|!ignore_diff(d));

    // Resize the array to fit the ADD diffs
    // TODO: Figure out how to remove the clone() here...
    let mut add_items: Vec<I> = add_rem_mod_diffs[ADD_DT_IDX].iter().map(|d|{get_item_from_diff(d.clone())}).collect();
    items.resize(items.len() + add_items.len() - add_rem_mod_diffs[1].len(), items[items.len() - 1].clone()); 
    
    // Modify / Remove
    let mut look_idx = 0;
    let mut assign_idx = 0;
    let mut mod_diff_idx = 0;
    let mut rem_diff_idx = 0;
    let rem_diffs_len = add_rem_mod_diffs[REM_DT_IDX].len();
    let mod_diffs_len = add_rem_mod_diffs[MOD_DT_IDX].len();
    while look_idx < items.len() {
        if assign_idx < look_idx {
            items[assign_idx] = items[look_idx].clone();
        }
        let mut curr = items[look_idx].clone();
        let modify = mod_diff_idx < mod_diffs_len && item_diff_match(&curr, &add_rem_mod_diffs[MOD_DT_IDX][mod_diff_idx]);
        let remove = rem_diff_idx < rem_diffs_len && item_diff_match(&curr, &add_rem_mod_diffs[REM_DT_IDX][rem_diff_idx]);
        if remove {
            look_idx += 1;
            rem_diff_idx += 1;
            continue;
        }
        if modify {
            add_diff_to_item(&mut curr, add_rem_mod_diffs[MOD_DT_IDX][mod_diff_idx].clone());
            items[assign_idx] = curr;
            mod_diff_idx += 1;
        }
        assign_idx += 1;
        look_idx += 1;
    }

    // Add
    if add_items.len() == 0 {
        return Ok(());
    }
    look_idx = 0;
    while look_idx < items.len() {
        let curr = items[look_idx].clone();
        let add    = items_sort(&curr, &add_items[0]) == std::cmp::Ordering::Greater;
        if !add {
            look_idx += 1;
            continue;
        }

        // Swap them
        let tmp = add_items[0].clone();
        items[look_idx] = tmp;
        
        // TODO: Look at VecDequeue
        // Shift everything down until a spot is found for `curr`
        let mut i = 1;
        while i < add_items.len() && items_sort(&curr, &add_items[i]) != std::cmp::Ordering::Less {
            add_items[i - 1] = add_items[i].clone();
            i += 1;
        }
        add_items[i - 1] = curr;
    
        look_idx += 1;
    }
    let num_items = items.len() - 1;
    if items_sort(&add_items[add_items.len() - 1], &items[num_items]) == std::cmp::Ordering::Greater {
        items[num_items] = add_items[add_items.len() - 1].clone();
    }

    return Ok(());
}

pub fn merge_dir_diff_to_entry(ent: &mut walk::CDirEntry, d: CDirEntryDiff) {
    ent.md = t_diff_to_system_time(d.t_diff, ent.md);

    ent.files_here += d.files_here;
    ent.files_below += d.files_below;
    ent.dirs_here += d.dirs_here;
    ent.dirs_below += d.dirs_below;
    ent.size_here += d.size_here;
    ent.size_below += d.size_below;

    let mut files_vec = ent.files.to_vec();
    _ = add_diffs_to_items::<walk::FileEntry, FileEntryDiff>(&mut files_vec, &mut d.files.clone(),
    |a, b|{return a.bn.cmp(&b.bn)}, 
    |it, d| {return it.bn == d.bn}, 
    ignore_file_entry,
    get_entry_from_file_diff, 
    merge_file_diff_to_entry);
    ent.files = files_vec;

    let mut symlinks_vec = ent.symlinks.to_vec();
    _ = add_diffs_to_items::<walk::FileEntry, FileEntryDiff>(&mut symlinks_vec, &mut d.symlinks.clone(),
    |a, b|{return a.bn.cmp(&b.bn)}, 
    |it, d| {return it.bn == d.bn},
    ignore_file_entry, 
    get_entry_from_file_diff, 
    merge_file_diff_to_entry);
    ent.symlinks = symlinks_vec;

}

pub fn merge_file_diff_to_entry(ent: &mut walk::FileEntry, d: FileEntryDiff) {
    ent.md = t_diff_to_system_time(d.t_diff, ent.md);
    ent.sz = d.sz as u64;
}

pub fn get_entry_from_dir_diff(d: CDirEntryDiff) -> walk::CDirEntry {
    let mut ret = walk::CDirEntry {
        p: d.p,
        md: t_diff_to_system_time(d.t_diff, None),
        files_here: d.files_here,
        files_below: d.files_below,
        dirs_here: d.dirs_here,
        dirs_below: d.dirs_below,
        size_here: d.size_here,
        size_below: d.size_below,
        md5: [0; 16],
        files: get_f_entries_from_f_diffs(d.files),
        symlinks: get_f_entries_from_f_diffs(d.symlinks),
    };
    ret.md5 = utility::get_md5_of_cdirentry(ret.clone());
    
    return ret;
}

pub fn get_entry_from_file_diff(d: FileEntryDiff) -> walk::FileEntry {
    return walk::FileEntry {
        bn: d.bn,
        sz: d.sz as u64,
        md: t_diff_to_system_time(d.t_diff, None),
    }
}

pub fn get_f_entries_from_f_diffs(fs: [Vec<FileEntryDiff>; NUM_DT]) -> Vec<walk::FileEntry> {
    let mut cap = 0;
    for f in &fs {
        cap += f.len();
    }
    let mut ret = Vec::with_capacity(cap);
    for i in 0..fs.len() {
        let da = fs[i].clone();
        let mut nda = Vec::with_capacity(da.len());
        for d in da {
            nda.push(get_entry_from_file_diff(d))
        }
        ret.append(&mut nda);
    }

    // Files need to be re-sorted since we're just doing: concat(add, rem, mod)
    ret.sort_by(|a, b| {
        return a.bn.cmp(&b.bn);
    });

    return ret;
}

pub fn t_diff_to_system_time(td: TDiff, old_md: Option<std::time::SystemTime>) -> Option<std::time::SystemTime> {
    let mut nmd = std::time::SystemTime::UNIX_EPOCH;
    if !old_md.is_none() {
        nmd = old_md.unwrap();
    }
    let mut dur = std::time::Duration::new(td.s_diff as u64, td.ns_diff as u32);
    if td.s_diff < 0 {
        dur = std::time::Duration::new((td.s_diff * -1) as u64, (td.ns_diff * -1) as u32);
        nmd.checked_sub(dur);
    } else {
        nmd.checked_add(dur);
    }
    return Some(nmd)
}