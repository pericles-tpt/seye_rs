use std::{cmp::Ordering, ffi::OsString, path::PathBuf, time::{Duration, SystemTime}};
use serde::{Deserialize, Deserializer, Serialize};
use crate::walk::{CDirEntry, FileEntry};

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum DiffType {
    Add,
    Remove,
    Modify,
    Move,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FileEntryDiff {
    pub bn: OsString,
    pub sz: i128,
    pub t_diff: TDiff,
    pub diff_type: DiffType,
}
impl Default for FileEntryDiff {
    fn default() -> Self {
        FileEntryDiff {
            bn: OsString::new(),
            sz: 0,
            t_diff: TDiff{
                s_diff: 0,
                ns_diff: 0,
            },
            diff_type: DiffType::Modify,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TDiff {
    pub s_diff: i64,
    pub ns_diff: i128,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CDirEntryDiff {
    pub p: PathBuf,
    pub t_diff: TDiff,

    pub files_here: usize,
    pub files_below: usize,
    pub dirs_here: usize,
    pub dirs_below: usize,
    pub size_here: i64,
    pub size_below: i64,
    
    pub diff_type: DiffType,

    #[serde(deserialize_with = "deserialize_boxed_slice")]
    pub files: Box<[FileEntryDiff]>,
    #[serde(deserialize_with = "deserialize_boxed_slice")]
    pub symlinks: Box<[FileEntryDiff]>,
}

fn deserialize_boxed_slice<'de, D>(deserializer: D) -> Result<Box<[FileEntryDiff]>, D::Error>
where
    D: Deserializer<'de>,
{
    // Deserialize into a Vec<FileEntry>
    let vec: Vec<FileEntryDiff> = Vec::deserialize(deserializer)?;
    // Convert Vec<FileEntry> into Box<[FileEntry]>
    Ok(vec.into_boxed_slice())
}

pub fn add_diffs_to_items<I: Clone + std::fmt::Debug + PartialEq, D: Clone + std::fmt::Debug>(
    items: &mut Vec<I>, 
    diffs: &mut Vec<D>, 
    items_sort: fn(a: &I, b: &I) -> Ordering,
    item_diff_match: fn(it: &I, d: &D) -> bool,
    diff_adds_items: fn(d: &D) -> bool,
    diff_removes_item: fn(d: &D) -> bool,
    get_item_from_diff: fn(d: D) -> I,
    add_diff_to_item: fn(it: &mut I, d: D) -> (),
) -> std::io::Result<()> {
    if items.len() == 0 || diffs.len() == 0 {
        return Ok(());
    }
    
    // Split diffs into: ADD and MODIFY/REMOVE
    let mut add_items: Vec<I> = Vec::new();
    let mut rem_diffs: Vec<D> = Vec::new();
    let mut mod_diffs: Vec<D> = Vec::new();
    for i in 0..diffs.len() {
        if diff_adds_items(&diffs[i]) {     
            let item_to_add = get_item_from_diff(diffs[i].clone());    
            add_items.push(item_to_add);
        } else if diff_removes_item(&diffs[i]) {
            rem_diffs.push(diffs[i].clone());
        } else {
            mod_diffs.push(diffs[i].clone());
        }
    }

    // Resize the array to fit the ADD diffs
    items.resize(items.len() + add_items.len() - rem_diffs.len(), items[items.len() - 1].clone()); 
    
    // Modify / Remove
    let mut look_idx = 0;
    let mut assign_idx = 0;
    let mut mod_diff_idx = 0;
    let mut rem_diff_idx = 0;
    while look_idx < items.len() {
        if assign_idx < look_idx {
            items[assign_idx] = items[look_idx].clone();
        }
        let mut curr = items[look_idx].clone();
        let modify = mod_diff_idx < mod_diffs.len() && item_diff_match(&curr, &mod_diffs[mod_diff_idx]);
        let remove = rem_diff_idx < rem_diffs.len() && item_diff_match(&curr, &rem_diffs[rem_diff_idx]);
        if remove {
            look_idx += 1;
            rem_diff_idx += 1;
            continue;
        }
        if modify {
            add_diff_to_item(&mut curr, mod_diffs[mod_diff_idx].clone());
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
        let add    = items_sort(&curr, &add_items[0]) == Ordering::Greater;
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
        while i < add_items.len() && items_sort(&curr, &add_items[i]) != Ordering::Less {
            add_items[i - 1] = add_items[i].clone();
            i += 1;
        }
        add_items[i - 1] = curr;
    
        look_idx += 1;
    }
    let num_items = items.len() - 1;
    if items_sort(&add_items[add_items.len() - 1], &items[num_items]) == Ordering::Greater {
        items[num_items] = add_items[add_items.len() - 1].clone();
    }

    return Ok(());
}

pub fn merge_dir_diff_to_entry(ent: &mut CDirEntry, d: CDirEntryDiff) {
    ent.md = t_diff_to_system_time(d.t_diff, ent.md);

    ent.files_here += d.files_here;
    ent.files_below += d.files_below;
    ent.dirs_here += d.dirs_here;
    ent.dirs_below += d.dirs_below;
    ent.size_here += d.size_here;
    ent.size_below += d.size_below;

    let mut files_vec = ent.files.to_vec();
    _ = add_diffs_to_items::<FileEntry, FileEntryDiff>(&mut files_vec, &mut d.files.to_vec(), 
    |a, b|{return a.bn.cmp(&b.bn)}, 
    |it, d| {return it.bn == d.bn}, 
    |d|{return d.diff_type == DiffType::Add}, 
    |d|{return d.diff_type == DiffType::Remove}, 
    get_entry_from_file_diff, 
    merge_file_diff_to_entry);
    ent.files = files_vec.into_boxed_slice();

    let mut symlinks_vec = ent.symlinks.to_vec();
    _ = add_diffs_to_items::<FileEntry, FileEntryDiff>(&mut symlinks_vec, &mut d.symlinks.to_vec(), 
    |a, b|{return a.bn.cmp(&b.bn)}, 
    |it, d| {return it.bn == d.bn}, 
    |d|{return d.diff_type == DiffType::Add}, 
    |d|{return d.diff_type == DiffType::Remove}, 
    get_entry_from_file_diff, 
    merge_file_diff_to_entry);
    ent.symlinks = symlinks_vec.into_boxed_slice();

}

pub fn merge_file_diff_to_entry(ent: &mut FileEntry, d: FileEntryDiff) {
    ent.md = t_diff_to_system_time(d.t_diff, ent.md);
    ent.sz = d.sz as u64;
}

pub fn get_entry_from_dir_diff(d: CDirEntryDiff) -> CDirEntry {
    return CDirEntry {
        p: d.p,
        md: t_diff_to_system_time(d.t_diff, None),
        files_here: d.files_here,
        files_below: d.files_below,
        dirs_here: d.dirs_here,
        dirs_below: d.dirs_below,
        size_here: d.size_here,
        size_below: d.size_below,
        files: get_f_entries_from_f_diffs(d.files),
        symlinks: get_f_entries_from_f_diffs(d.symlinks),
    }
}

pub fn get_entry_from_file_diff(d: FileEntryDiff) -> FileEntry {
    return FileEntry {
        bn: d.bn,
        sz: d.sz as u64,
        md: t_diff_to_system_time(d.t_diff, None),
    }
}

pub fn get_f_entries_from_f_diffs(fs: Box<[FileEntryDiff]>) -> Box<[FileEntry]> {
    let mut ret = Vec::with_capacity(fs.len());
    for f in fs {
        ret.push(get_entry_from_file_diff(f))
    }
    return ret.into_boxed_slice();
}

pub fn t_diff_to_system_time(td: TDiff, old_md: Option<SystemTime>) -> Option<SystemTime> {
    let mut nmd = SystemTime::UNIX_EPOCH;
    if !old_md.is_none() {
        nmd = old_md.unwrap();
    }
    let mut dur = Duration::new(td.s_diff as u64, td.ns_diff as u32);
    if td.s_diff < 0 {
        dur = Duration::new((td.s_diff * -1) as u64, (td.ns_diff * -1) as u32);
        nmd.checked_sub(dur);
    } else {
        nmd.checked_add(dur);
    }
    return Some(nmd)
}

#[cfg(test)]
mod tests {
    use std::{collections::{HashMap, HashSet}, fs::File, path::PathBuf, str::FromStr};

    use crate::{scan::{bubble_up_props, scan}, walk::walk_until_end};

    use crate::utility::get_cwd;

    use super::{add_diffs_to_items, DiffType};

    #[derive(Debug, Clone, PartialEq)]
    pub struct Num {
        pub id: String,
        pub d: i32,
    }

    #[derive(Debug, Clone)]
    pub struct NumDiff {
        pub id: String,
        pub dt: DiffType,
        pub d: i32,
    }

    #[test]
    fn add_file_to_middle() {
        let wd: PathBuf = get_cwd();
        
        let path = std::path::PathBuf::from_str(format!("{}/tests/test_dir/b", wd.display()).as_str());
        match path {
            Ok(test_input_path) => {
                let new_file_path = PathBuf::from(format!("{}/tests/test_dir/b/jmiddle", wd.display()));
                let test_output_path: PathBuf = std::path::PathBuf::from_str(format!("{}/tests/test_output_dir", wd.display()).as_str()).expect("Failed to get output path");
                match std::fs::remove_file(&new_file_path) {
                    Ok(()) => {}
                    Err(err) =>{
                        panic!("failed to remove file: {}", err);
                    }
                }
                match std::fs::remove_dir_all(&test_output_path) {
                    Ok(()) => {}
                    Err(err) =>{
                        panic!("failed to remove dir and contents: {}", err);
                    }
                }
                match std::fs::create_dir(&test_output_path) {
                    Ok(()) => {}
                    Err(err) =>{
                        panic!("failed to create dir: {}", err);
                    }
                }

                // Scan before adding file
                let mut pm: HashMap<std::path::PathBuf, usize> = HashMap::new();
                let mut initial_scan = walk_until_end(new_file_path.clone(), &mut pm, &mut HashSet::new());

                initial_scan.sort_by(|a, b| {
                    return a.p.cmp(&b.p);
                });

                bubble_up_props(&mut initial_scan, &mut pm);

                // Add file
                let mf = File::create(new_file_path.clone());
                if mf.is_err() {
                    panic!("error creating file: {}", mf.err().unwrap());
                }

                // Get diff after scan
                let _res = scan(test_input_path, test_output_path, 1, 0, 256);
                
                // Do diff

            }
            Err(e) => {
                panic!("failed to get path buf: {}", e)
            }
        }
    }

    #[test]
    fn add_diffs_to_items_test() {
        let mut items: Vec<Num> = [Num{
            id: String::from("a"),
            d: 1,
        }, Num{
            id: String::from("b"),
            d: 2,
        }, Num{
            id: String::from("c"),
            d: 3,
        }, Num{
            id: String::from("d"),
            d: 4,
        }, Num{
            id: String::from("e"),
            d: 5,
        }].to_vec();
        let mut diffs: Vec<NumDiff> = [NumDiff{
            id: String::from("b"),
            d: 1,
            dt: DiffType::Remove
        }, NumDiff{
            id: String::from("d"),
            d: 1,
            dt: DiffType::Remove
        }, NumDiff{
            id: String::from("e"),
            d: -1,
            dt: DiffType::Modify
        }, NumDiff{
            id: String::from("A"),
            d: 3,
            dt: DiffType::Add
        }, NumDiff{
            id: String::from("c1"),
            d: 2,
            dt: DiffType::Add
        }, NumDiff{
            id: String::from("f"),
            d: 10,
            dt: DiffType::Add
        }].to_vec();

        match add_diffs_to_items(&mut items, &mut diffs, |a, b|{
            return a.id.cmp(&b.id);
        }, |a, b| {
            return *a.id == b.id;
        }, |d| {
            return d.dt == DiffType::Add;
        }, |d| {
            return d.dt == DiffType::Remove;
        }, |d| {
            return Num{
                id: d.id,
                d: d.d,
            };
        }, |it, d| {
            it.d += d.d;
        }) {
            Ok(()) => {},
            Err(err) => {
                panic!("failed to add diffs to items: {}", err)
            },
        }
        

        assert_eq!(items, [Num{
            id: String::from("A"),
            d: 3,
        }, Num{
            id: String::from("a"),
            d: 1,
        }, Num{
            id: String::from("c"),
            d: 3,
        }, Num{
            id: String::from("c1"),
            d: 2,
        }, Num{
            id: String::from("e"),
            d: 4,
        }, Num{
            id: String::from("f"),
            d: 10,
        }].to_vec());
    }
}