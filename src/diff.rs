use std::{cmp::Ordering, ffi::OsString, path::PathBuf, time::{Duration, SystemTime}};
use serde::{Deserialize, Deserializer, Serialize};
use crate::walk::{CDirEntry, FileEntry};

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum DiffType {
    Add,
    Remove,
    Modify,
    // Rename, // TODO: rename, a bit harder to do, do I need file hashes?
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FileEntryDiff {
    pub bn: OsString,
    pub sz: i128,
    pub t_diff: TDiff,
    pub diff_type: DiffType,
    pub diff_no: u16,
    pub hash: [u8; 32],
    pub is_symlink: bool,
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
            diff_no: 0,
            hash: [0; 32],
            is_symlink: false
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
    pub diff_no: u16,

    pub files_here: usize,
    pub files_below: usize,
    pub dirs_here: usize,
    pub dirs_below: usize,
    pub size_here: i64,
    pub size_below: i64,
    pub memory_usage_here: usize,
    pub memory_usage_below: usize,
    
    pub diff_type: DiffType,

    #[serde(deserialize_with = "deserialize_boxed_slice")]
    pub files: Box<[FileEntryDiff]>,
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

// NOTE: Assumes diffs are sorted
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
    let mut add_items_idxs: Vec<usize> = Vec::new();
    let mut rem_mod_diffs: Vec<D> = Vec::new();
    let mut num_remove_items = 0;
    for i in 0..diffs.len() {
        if diff_adds_items(&diffs[i]) {     
            let item_to_add = get_item_from_diff(diffs[i].clone());    
            add_items_idxs.push(i);
            add_items.push(item_to_add);
        } else  {
            if diff_removes_item(&diffs[i]) {
                num_remove_items += 1;
            }
            rem_mod_diffs.push(diffs[i].clone());
        }
    }
    // Resize the array to fit the ADD diffs
    items.resize(items.len() + add_items_idxs.len() - num_remove_items, items[0].clone());
    
    let mut assign_idx = 0;
    let mut look_idx = 0;
    
    let mut add_diff_idx = 0;
    let mut rem_mod_diff_idx = 0;
    // let mut dur = Duration::new(0, 0);
    while look_idx < items.len() && ((rem_mod_diff_idx < rem_mod_diffs.len()) || add_diff_idx < add_items.len()) {
        if assign_idx < look_idx {
            items[assign_idx] = items[look_idx].clone();
        }

        // Iterators
        let curr = &items[look_idx].clone();
        let mut next = None;
        if look_idx < items.len() - 1 {
            next = Some(items[look_idx + 1].clone());
        }

        let mut removed = false;
        while rem_mod_diff_idx < rem_mod_diffs.len() {
            let rem_mod_diff = &rem_mod_diffs[rem_mod_diff_idx];
            if !item_diff_match(&curr, &rem_mod_diff) {
                break;
            }
            
            if diff_removes_item(rem_mod_diff) {
                assign_idx -= 1;
                removed = true;
            } else {
                let mut_curr = &mut items[look_idx];
                add_diff_to_item(mut_curr, rem_mod_diff.clone());
                
                if assign_idx < look_idx {
                    items[assign_idx] = items[look_idx].clone();
                }
            }
            
            rem_mod_diff_idx += 1;
        }
        
        while !removed && add_diff_idx < add_items.len() {
            let pos: Ordering = items_sort(&add_items[add_diff_idx], curr); // diff_before_item(&add_items[add_diff_idx], prev.clone(), &curr, next.clone());
            let adjacent = pos != Ordering::Equal;
            if !adjacent {
                break
            }
            
            // Items are UNIQUE, so will only be added if adjacent
            // LESS: Add the item at the current `assign_idx`, if there's another item there, put it in the right spot in the add_items array
            // GREATER: ONLY add it at the end?
            
            let at_items_end = &next.is_none();
            let mut increment = true;
            
            // Recreate `add_items`, inserting `curr_clone` in-place
            if pos == Ordering::Less {
                let item_to_add = add_items[add_diff_idx].clone();    
                if assign_idx < look_idx {
                    items[assign_idx] = item_to_add;
                    look_idx = assign_idx;
                } else if assign_idx == look_idx {
                    // TODO: Re-write this to modify `add_items` in-place rather than creating a new array and replacing the old one
                    // Re-populate new_add_items, with `curr_clone`, in correct position   
                    let curr_clone = items[look_idx].clone();                 
                    let mut new_add_items = Vec::with_capacity(add_items.len() + 1);
                    let mut curr_inserted = false;
                    for i in add_diff_idx..add_items.len() {
                        // let it = add_items[i].clone();
                        if add_items[i] == item_to_add {
                            continue
                        }
                        if !curr_inserted && items_sort(&curr, &add_items[i]) == Ordering::Less {
                            new_add_items.push(items[look_idx].clone());
                            curr_inserted = true;
                        }
                        new_add_items.push(add_items[i].clone());
                    }
                    if !curr_inserted {
                        new_add_items.push(curr_clone);
                    }
                    add_items = new_add_items;
                    add_diff_idx = 0;
                    
                    items[assign_idx] = item_to_add;
                    increment = false;
                }
            } else if *at_items_end {
                // ONLY add GREATER at the end
                items[look_idx] = add_items[add_diff_idx].clone();
            } else {
                increment = false;
            }
            
            if !increment {
                break
            }
            add_diff_idx += 1;
        }
        
        assign_idx += 1;
        look_idx += 1;
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
    ent.memory_usage_here += d.memory_usage_here;
    ent.memory_usage_below += d.memory_usage_below;

    _ = add_diffs_to_items::<FileEntry, FileEntryDiff>(&mut ent.files.to_vec(), &mut d.files.to_vec(), 
    |a, b|{return a.bn.cmp(&b.bn)}, 
    |it, d| {return it.bn == d.bn}, 
    |d|{return d.diff_type == DiffType::Add}, 
    |d|{return d.diff_type == DiffType::Remove}, 
    get_entry_from_file_diff, 
    merge_file_diff_to_entry);
    // add_diffs_to_files(&mut ent.files.to_vec(), d.files)
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
        memory_usage_here: d.memory_usage_here,
        memory_usage_below: d.memory_usage_below,
        files: get_f_entries_from_f_diffs(d.files),
    }
}

pub fn get_entry_from_file_diff(d: FileEntryDiff) -> FileEntry {
    return FileEntry {
        bn: d.bn,
        sz: d.sz as u64,
        md: t_diff_to_system_time(d.t_diff, None),
        hash: [0; 32],
        is_symlink: d.is_symlink,
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