use std::fmt::Debug;
use std::hash::Hash;

use crate::col::{map_new, HashMap};
use crate::graph::{Path, PathBox};

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct PathId(pub(crate) u32);

impl Debug for PathId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("p#{}", self.0))
    }
}

pub struct PathsIndex<'a> {
    paths: Vec<PathBox>,

    path_id_by_path: HashMap<&'a Path, PathId>,
}

impl<'a> PathsIndex<'a> {
    pub fn new() -> Self {
        Self {
            paths: vec![],
            path_id_by_path: map_new(),
        }
    }

    pub fn path(&self, path_id: PathId) -> &Path {
        self.paths[path_id.0 as usize].payload()
    }

    pub fn path_ids(&self) -> impl Iterator<Item = PathId> {
        (0..self.paths.len()).map(|id| PathId(id as u32))
    }

    pub fn transfer_path(&mut self, path_box: PathBox) -> PathId {
        self.path_id_by_path
            .get(&path_box.payload())
            .copied()
            .unwrap_or_else(|| {
                let id_usize = self.paths.len();
                let id = PathId(id_usize as u32);
                self.paths.push(path_box);

                // Get unbounded ref to path payload.
                let path_ptr = self.paths[id_usize].payload() as *const Path;
                // SAFETY: The ref to the path is valid until the PathsIndex is destroyed,
                // as we never remove paths from the index.
                let unbounded_ref: &'a Path = unsafe { path_ptr.as_ref() }.unwrap();
                self.path_id_by_path.insert(unbounded_ref, id);
                id
            })
    }
}
