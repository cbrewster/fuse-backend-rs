// Copyright 2020 Ant Financial. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! A pseudo fs for path walking to other real filesystems
//!
//! There are several assumptions adopted when designing the PseudoFs:
//! - The PseudoFs is used to mount other filesystems, so it only supports directories.
//! - There won't be too much directories/sub-directories managed by a PseudoFs instance, so linear
//!   search is used when searching for child inodes.
//! - Inodes managed by the PseudoFs is readonly, even for the permission bits.

use arc_swap::ArcSwap;
use async_trait::async_trait;
use std::collections::HashMap;
use std::ffi::CStr;
use std::io::{Error, Result};
use std::ops::Deref;
use std::path::{Component, Path};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use crate::abi::fuse_abi::{stat64, Attr, CreateIn};
use crate::api::filesystem::*;

// ID 0 is reserved for invalid entry, and ID 1 is used for ROOT_ID.
const PSEUDOFS_NEXT_INODE: u64 = 2;
const PSEUDOFS_DEFAULT_ATTR_TIMEOUT: u64 = 1 << 32;
const PSEUDOFS_DEFAULT_ENTRY_TIMEOUT: u64 = PSEUDOFS_DEFAULT_ATTR_TIMEOUT;

type Inode = u64;
type Handle = u64;

struct PseudoInode {
    ino: u64,
    parent: u64,
    children: ArcSwap<Vec<Arc<PseudoInode>>>,
    name: String,
}

impl PseudoInode {
    fn new(ino: u64, parent: u64, name: String) -> Self {
        PseudoInode {
            ino,
            parent,
            children: ArcSwap::new(Arc::new(Vec::new())),
            name,
        }
    }

    // It's protected by Pseudofs.lock.
    fn insert_child(&self, child: Arc<PseudoInode>) {
        let mut children = self.children.load().deref().deref().clone();

        children.push(child);

        self.children.store(Arc::new(children));
    }

    fn remove_child(&self, child: Arc<PseudoInode>) {
        let mut children = self.children.load().deref().deref().clone();

        children
            .iter()
            .position(|x| x.name == child.name)
            .map(|pos| children.remove(pos))
            .unwrap();

        self.children.store(Arc::new(children));
    }
}

pub struct PseudoFs {
    next_inode: AtomicU64,
    root_inode: Arc<PseudoInode>,
    inodes: ArcSwap<HashMap<u64, Arc<PseudoInode>>>,
    lock: Mutex<()>, // Write protect PseudoFs.inodes and PseudoInode.children
}

impl PseudoFs {
    pub fn new() -> Self {
        let root_inode = Arc::new(PseudoInode::new(ROOT_ID, ROOT_ID, String::from("/")));
        let fs = PseudoFs {
            next_inode: AtomicU64::new(PSEUDOFS_NEXT_INODE),
            root_inode: root_inode.clone(),
            inodes: ArcSwap::new(Arc::new(HashMap::new())),
            lock: Mutex::new(()),
        };

        // Create the root inode. We have just created the lock, so it should be safe to unwrap().
        let _guard = fs.lock.lock().unwrap();
        fs.insert_inode(root_inode);
        drop(_guard);

        fs
    }

    // mount creates path walk nodes all the way from root
    // to @path, and returns pseudo fs inode number for the path
    pub fn mount(&self, mountpoint: &str) -> Result<u64> {
        let path = Path::new(mountpoint);
        if !path.has_root() {
            error!("pseudo fs mount failure: invalid mount path {}", mountpoint);
            return Err(Error::from_raw_os_error(libc::EINVAL));
        }

        let mut inodes = self.inodes.load();
        let mut inode = &self.root_inode;

        'outer: for component in path.components() {
            trace!("pseudo fs mount iterate {:?}", component.as_os_str());
            match component {
                Component::RootDir => continue,
                Component::CurDir => continue,
                Component::ParentDir => inode = inodes.get(&inode.parent).unwrap(),
                Component::Prefix(_) => {
                    error!("unsupported path: {}", mountpoint);
                    return Err(Error::from_raw_os_error(libc::EINVAL));
                }
                Component::Normal(path) => {
                    let name = path.to_str().unwrap();

                    // Optimistic check without lock.
                    for child in inode.children.load().iter() {
                        if child.name == name {
                            inode = inodes.get(&child.ino).unwrap();
                            continue 'outer;
                        }
                    }

                    // Double check with writer lock held.
                    let _guard = self.lock.lock();
                    for child in inode.children.load().iter() {
                        if child.name == name {
                            inode = inodes.get(&child.ino).unwrap();
                            continue 'outer;
                        }
                    }

                    let new_node = self.create_inode(name, inode);
                    inodes = self.inodes.load();
                    inode = inodes.get(&new_node.ino).unwrap();
                }
            }
        }

        // Now we have all path components exist, return the last one
        Ok(inode.ino)
    }

    pub fn path_walk(&self, mountpoint: &str) -> Result<Option<u64>> {
        let path = Path::new(mountpoint);
        if !path.has_root() {
            error!("pseudo fs walk failure: invalid path {}", mountpoint);
            return Err(Error::from_raw_os_error(libc::EINVAL));
        }

        let inodes = self.inodes.load();
        let mut inode = &self.root_inode;

        'outer: for component in path.components() {
            debug!("pseudo fs iterate {:?}", component.as_os_str());
            match component {
                Component::RootDir => continue,
                Component::CurDir => continue,
                Component::ParentDir => inode = inodes.get(&inode.parent).unwrap(),
                Component::Prefix(_) => {
                    error!("unsupported path: {}", mountpoint);
                    return Err(Error::from_raw_os_error(libc::EINVAL));
                }
                Component::Normal(path) => {
                    let name = path.to_str().ok_or_else(|| {
                        error!("Path {:?} can't be converted safely", path);
                        Error::from_raw_os_error(libc::EINVAL)
                    })?;

                    // Optimistic check without lock.
                    for child in inode.children.load().iter() {
                        if child.name == name {
                            inode = inodes.get(&child.ino).unwrap();
                            continue 'outer;
                        }
                    }

                    // Double check with writer lock held.
                    let _guard = self.lock.lock();
                    for child in inode.children.load().iter() {
                        if child.name == name {
                            inode = inodes.get(&child.ino).unwrap();
                            continue 'outer;
                        }
                    }

                    debug!("name {} is not found, path is {}", name, mountpoint);
                    return Ok(None);
                }
            }
        }

        // let _guard = self.lock.lock();
        // self.evict_inode(&inode);
        // Now we have all path components exist, return the last one
        Ok(Some(inode.ino))
    }

    fn new_inode(&self, parent: u64, name: &str) -> Arc<PseudoInode> {
        let ino = self.next_inode.fetch_add(1, Ordering::Relaxed);

        Arc::new(PseudoInode::new(ino, parent, name.to_owned()))
    }

    // Caller must hold PseudoFs.lock.
    fn insert_inode(&self, inode: Arc<PseudoInode>) {
        let mut hashmap = self.inodes.load().deref().deref().clone();

        hashmap.insert(inode.ino, inode);

        self.inodes.store(Arc::new(hashmap));
    }

    // Caller must hold PseudoFs.lock.
    fn create_inode(&self, name: &str, parent: &Arc<PseudoInode>) -> Arc<PseudoInode> {
        let inode = self.new_inode(parent.ino, name);

        self.insert_inode(inode.clone());
        parent.insert_child(inode.clone());

        inode
    }

    fn remove_inode(&self, inode: &Arc<PseudoInode>) {
        let mut hashmap = self.inodes.load().deref().deref().clone();

        hashmap.remove(&inode.ino);

        self.inodes.store(Arc::new(hashmap));
    }

    pub fn get_parent_inode(&self, ino: u64) -> Option<u64> {
        let _guard = self.lock.lock();
        let inodes = self.inodes.load();
        inodes.get(&ino).map(|o| o.parent)
    }

    #[allow(dead_code)]
    pub fn evict_inode(&self, ino: u64) {
        let _guard = self.lock.lock();
        let inodes = self.inodes.load();

        let inode = inodes.get(&ino).unwrap();
        // ino == inode.parent means it is pseudo fs root inode.
        // Do not evict it.
        if ino == inode.parent {
            return;
        }

        let parent = inodes.get(&inode.parent).unwrap();
        parent.remove_child(inode.clone());

        self.remove_inode(inode);
    }

    fn get_entry(&self, ino: u64) -> Entry {
        let mut attr = Attr {
            ..Default::default()
        };
        attr.ino = ino;
        #[cfg(target_os = "linux")]
        {
            attr.mode = libc::S_IFDIR | libc::S_IRWXU | libc::S_IRWXG | libc::S_IRWXO;
        }
        #[cfg(target_os = "macos")]
        {
            attr.mode = (libc::S_IFDIR | libc::S_IRWXU | libc::S_IRWXG | libc::S_IRWXO) as u32;
        }
        let now = SystemTime::now();
        attr.ctime = now
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        attr.mtime = attr.ctime;
        attr.atime = attr.ctime;
        attr.blksize = 4096;
        Entry {
            inode: ino,
            generation: 0,
            attr: attr.into(),
            attr_flags: 0,
            attr_timeout: Duration::from_secs(PSEUDOFS_DEFAULT_ATTR_TIMEOUT),
            entry_timeout: Duration::from_secs(PSEUDOFS_DEFAULT_ENTRY_TIMEOUT),
        }
    }

    fn do_readdir(
        &self,
        parent: u64,
        size: u32,
        offset: u64,
        add_entry: &mut dyn FnMut(DirEntry) -> Result<usize>,
    ) -> Result<()> {
        if size == 0 {
            return Ok(());
        }

        let inodes = self.inodes.load();
        let inode = inodes
            .get(&parent)
            .ok_or_else(|| Error::from_raw_os_error(libc::ENOENT))?;
        let mut next = offset + 1;
        let children = inode.children.load();

        if offset >= children.len() as u64 {
            return Ok(());
        }

        for child in children[offset as usize..].iter() {
            match add_entry(DirEntry {
                ino: child.ino,
                offset: next,
                type_: 0,
                name: child.name.clone().as_bytes(),
            }) {
                Ok(0) => break,
                Ok(_) => next += 1,
                Err(r) => return Err(r),
            }
        }

        Ok(())
    }
}

impl Default for PseudoFs {
    fn default() -> Self {
        Self::new()
    }
}

impl FileSystem for PseudoFs {
    type Inode = Inode;
    type Handle = Handle;

    fn lookup(&self, _: &Context, parent: u64, name: &CStr) -> Result<Entry> {
        let inodes = self.inodes.load();
        let pinode = inodes
            .get(&parent)
            .ok_or_else(|| Error::from_raw_os_error(libc::ENOENT))?;
        let child_name = name
            .to_str()
            .map_err(|_| Error::from_raw_os_error(libc::EINVAL))?;
        let mut ino: u64 = 0;
        if child_name == "." {
            ino = pinode.ino;
        } else if child_name == ".." {
            ino = pinode.parent;
        } else {
            for child in pinode.children.load().iter() {
                if child.name == child_name {
                    ino = child.ino;
                    break;
                }
            }
        }

        if ino == 0 {
            // not found
            Err(Error::from_raw_os_error(libc::ENOENT))
        } else {
            Ok(self.get_entry(ino))
        }
    }

    fn getattr(&self, _: &Context, inode: u64, _: Option<u64>) -> Result<(stat64, Duration)> {
        let ino = self
            .inodes
            .load()
            .get(&inode)
            .map(|inode| inode.ino)
            .ok_or_else(|| Error::from_raw_os_error(libc::ENOENT))?;
        let entry = self.get_entry(ino);

        Ok((entry.attr, entry.attr_timeout))
    }

    fn readdir(
        &self,
        _ctx: &Context,
        inode: u64,
        _: u64,
        size: u32,
        offset: u64,
        add_entry: &mut dyn FnMut(DirEntry) -> Result<usize>,
    ) -> Result<()> {
        self.do_readdir(inode, size, offset, add_entry)
    }

    fn readdirplus(
        &self,
        _ctx: &Context,
        inode: u64,
        _handle: u64,
        size: u32,
        offset: u64,
        add_entry: &mut dyn FnMut(DirEntry, Entry) -> Result<usize>,
    ) -> Result<()> {
        self.do_readdir(inode, size, offset, &mut |dir_entry| {
            let entry = self.get_entry(dir_entry.ino);
            add_entry(dir_entry, entry)
        })
    }

    fn access(&self, _ctx: &Context, _inode: u64, _mask: u32) -> Result<()> {
        Ok(())
    }
}

#[cfg(feature = "async-io")]
#[async_trait]
impl AsyncFileSystem for PseudoFs {
    async fn async_lookup(&self, ctx: &Context, parent: Self::Inode, name: &CStr) -> Result<Entry> {
        self.lookup(ctx, parent, name)
    }

    async fn async_getattr(
        &self,
        ctx: &Context,
        inode: Self::Inode,
        handle: Option<Self::Handle>,
    ) -> Result<(stat64, Duration)> {
        self.getattr(ctx, inode, handle)
    }

    async fn async_setattr(
        &self,
        ctx: &Context,
        inode: Self::Inode,
        attr: stat64,
        handle: Option<Self::Handle>,
        valid: SetattrValid,
    ) -> Result<(stat64, Duration)> {
        self.setattr(ctx, inode, attr, handle, valid)
    }

    async fn async_open(
        &self,
        ctx: &Context,
        inode: Self::Inode,
        flags: u32,
        fuse_flags: u32,
    ) -> Result<(Option<Self::Handle>, OpenOptions)> {
        let (handle, opts, _) = self.open(ctx, inode, flags, fuse_flags)?;
        Ok((handle, opts))
    }

    async fn async_create(
        &self,
        ctx: &Context,
        parent: Self::Inode,
        name: &CStr,
        args: CreateIn,
    ) -> Result<(Entry, Option<Self::Handle>, OpenOptions)> {
        let (entry, handle, opts, _) = self.create(ctx, parent, name, args)?;
        Ok((entry, handle, opts))
    }

    #[allow(clippy::too_many_arguments)]
    async fn async_read(
        &self,
        _ctx: &Context,
        _inode: Self::Inode,
        _handle: Self::Handle,
        _w: &mut (dyn AsyncZeroCopyWriter + Send),
        _size: u32,
        _offset: u64,
        _lock_owner: Option<u64>,
        _flags: u32,
    ) -> Result<usize> {
        Err(Error::from_raw_os_error(libc::ENOSYS))
    }

    #[allow(clippy::too_many_arguments)]
    async fn async_write(
        &self,
        _ctx: &Context,
        _inode: Self::Inode,
        _handle: Self::Handle,
        _r: &mut (dyn AsyncZeroCopyReader + Send),
        _size: u32,
        _offset: u64,
        _lock_owner: Option<u64>,
        _delayed_write: bool,
        _flags: u32,
        _fuse_flags: u32,
    ) -> Result<usize> {
        Err(Error::from_raw_os_error(libc::ENOSYS))
    }

    async fn async_fsync(
        &self,
        ctx: &Context,
        inode: Self::Inode,
        datasync: bool,
        handle: Self::Handle,
    ) -> Result<()> {
        self.fsync(ctx, inode, datasync, handle)
    }

    async fn async_fallocate(
        &self,
        ctx: &Context,
        inode: Self::Inode,
        handle: Self::Handle,
        mode: u32,
        offset: u64,
        length: u64,
    ) -> Result<()> {
        self.fallocate(ctx, inode, handle, mode, offset, length)
    }

    async fn async_fsyncdir(
        &self,
        ctx: &Context,
        inode: Self::Inode,
        datasync: bool,
        handle: Self::Handle,
    ) -> Result<()> {
        self.fsyncdir(ctx, inode, datasync, handle)
    }
}

/// Save and restore PseudoFs state.
#[cfg(feature = "persist")]
pub mod persist {
    use std::collections::HashMap;
    use std::io::{Error as IoError, ErrorKind, Result};
    use std::sync::atomic::Ordering;
    use std::sync::Arc;

    use dbs_snapshot::Snapshot;
    use versionize::{VersionMap, Versionize, VersionizeResult};
    use versionize_derive::Versionize;

    use super::{PseudoFs, PseudoInode};
    use crate::api::filesystem::ROOT_ID;

    #[derive(Versionize, PartialEq, Debug, Default, Clone)]
    struct PseudoInodeState {
        ino: u64,
        parent: u64,
        name: String,
    }

    #[derive(Versionize, PartialEq, Debug, Default)]
    pub struct PseudoFsState {
        next_inode: u64,
        inodes: Vec<PseudoInodeState>,
    }

    impl PseudoFs {
        fn get_version_map() -> VersionMap {
            let mut vm = VersionMap::new();
            vm.set_type_version(PseudoFsState::type_id(), 1);

            // more versions for the future

            vm
        }

        /// Saves part of the PseudoFs into a byte array.
        /// The upper layer caller can use this method to save
        /// and transfer metadata for the reloading in the future.
        pub fn save_to_bytes(&self) -> Result<Vec<u8>> {
            let mut inodes = Vec::new();
            let next_inode = self.next_inode.load(Ordering::Relaxed);

            let _guard = self.lock.lock().unwrap();
            for inode in self.inodes.load().values() {
                if inode.ino == ROOT_ID {
                    // no need to save the root inode
                    continue;
                }

                inodes.push(PseudoInodeState {
                    ino: inode.ino,
                    parent: inode.parent,
                    name: inode.name.clone(),
                });
            }
            let state = PseudoFsState { next_inode, inodes };

            let vm = PseudoFs::get_version_map();
            let target_version = vm.latest_version();
            let mut s = Snapshot::new(vm, target_version);
            let mut buf = Vec::new();
            s.save(&mut buf, &state).map_err(|e| {
                IoError::new(
                    ErrorKind::Other,
                    format!("Failed to save PseudoFs to bytes: {:?}", e),
                )
            })?;

            Ok(buf)
        }

        /// Restores the PseudoFs from a byte array.
        pub fn restore_from_bytes(&self, buf: &mut Vec<u8>) -> Result<()> {
            let state: PseudoFsState =
                Snapshot::load(&mut buf.as_slice(), buf.len(), PseudoFs::get_version_map())
                    .map_err(|e| {
                        IoError::new(
                            ErrorKind::Other,
                            format!("Failed to load PseudoFs from bytes: {:?}", e),
                        )
                    })?
                    .0;
            self.restore_from_state(&state)
        }

        fn restore_from_state(&self, state: &PseudoFsState) -> Result<()> {
            // first, reconstruct all the inodes
            let mut inode_map = HashMap::new();
            let mut state_inodes = state.inodes.clone();
            for inode in state_inodes.iter() {
                let inode = Arc::new(PseudoInode::new(
                    inode.ino,
                    inode.parent,
                    inode.name.clone(),
                ));
                inode_map.insert(inode.ino, inode);
            }

            // insert root inode to make sure the others inodes can find their parents
            inode_map.insert(self.root_inode.ino, self.root_inode.clone());

            // then, connect the inodes
            state_inodes.sort_by(|a, b| a.ino.cmp(&b.ino));
            for inode in state_inodes.iter() {
                let inode = inode_map
                    .get(&inode.ino)
                    .ok_or_else(|| {
                        IoError::new(
                            ErrorKind::InvalidData,
                            format!("invalid inode {}", inode.ino),
                        )
                    })?
                    .clone();
                let parent = inode_map.get_mut(&inode.parent).ok_or_else(|| {
                    IoError::new(
                        ErrorKind::InvalidData,
                        format!(
                            "invalid parent inode {} for inode {}",
                            inode.parent, inode.ino
                        ),
                    )
                })?;
                parent.insert_child(inode);
            }
            self.inodes.store(Arc::new(inode_map));

            // last, restore next_inode
            self.next_inode.store(state.next_inode, Ordering::Relaxed);

            Ok(())
        }
    }

    mod test {

        #[test]
        fn save_restore_test() {
            use crate::api::pseudo_fs::PseudoFs;

            let fs = &PseudoFs::new();
            let paths = vec!["/a", "/a/b", "/a/b/c", "/b", "/b/a/c", "/d"];

            for path in paths.iter() {
                fs.mount(path).unwrap();
            }

            // save fs
            let mut buf = fs.save_to_bytes().unwrap();

            // restore fs
            let restored_fs = &PseudoFs::new();
            restored_fs.restore_from_bytes(&mut buf).unwrap();

            // check fs and restored_fs
            let next_inode = fs.next_inode.load(std::sync::atomic::Ordering::Relaxed);
            let restored_next_inode = restored_fs
                .next_inode
                .load(std::sync::atomic::Ordering::Relaxed);
            assert_eq!(next_inode, restored_next_inode);

            for path in paths.iter() {
                let inode = fs.path_walk(path).unwrap();
                let restored_inode = restored_fs.path_walk(path).unwrap();
                assert_eq!(inode, restored_inode);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::CString;

    fn create_fuse_context() -> Context {
        Context::new()
    }

    #[test]
    fn test_pseudofs_new() {
        let fs = PseudoFs::new();

        assert_eq!(fs.next_inode.load(Ordering::Relaxed), 2);
        assert_eq!(fs.root_inode.ino, ROOT_ID);
        assert_eq!(fs.root_inode.children.load().len(), 0);
        assert_eq!(fs.inodes.load().len(), 1);
    }

    #[test]
    fn test_pseudofs_mount() {
        let fs = PseudoFs::new();

        assert_eq!(
            fs.mount("test").unwrap_err().raw_os_error().unwrap(),
            libc::EINVAL
        );

        let a1 = fs.mount("/a").unwrap();
        let a2 = fs.mount("/a").unwrap();
        assert_eq!(a1, a2);
        let a3 = fs.mount("/./a").unwrap();
        assert_eq!(a1, a3);
        let a4 = fs.mount("/../a").unwrap();
        assert_eq!(a1, a4);
        let a5 = fs.mount("/../../a").unwrap();
        assert_eq!(a1, a5);

        let c1 = fs.mount("/a/b/c").unwrap();
        let c1_i = fs.inodes.load().get(&c1).unwrap().clone();
        let b1 = fs.mount("/a/b").unwrap();
        assert_eq!(c1, c1_i.ino);
        assert_eq!(c1_i.parent, b1);

        let _e1 = fs.mount("/a/b/c/d/e").unwrap();
    }

    #[test]
    fn test_pseudofs_lookup() {
        let fs = PseudoFs::new();
        let a1 = fs.mount("/a").unwrap();
        let b1 = fs.mount("/a/b").unwrap();
        let c1 = fs.mount("/a/b/c").unwrap();

        assert!(fs
            .lookup(
                &create_fuse_context(),
                0x1000_0000,
                &CString::new(".").unwrap()
            )
            .is_err());
        assert_eq!(
            fs.lookup(
                &create_fuse_context(),
                ROOT_ID,
                &CString::new("..").unwrap()
            )
            .unwrap()
            .inode,
            ROOT_ID
        );
        assert_eq!(
            fs.lookup(&create_fuse_context(), ROOT_ID, &CString::new(".").unwrap())
                .unwrap()
                .inode,
            ROOT_ID
        );
        assert_eq!(
            fs.lookup(&create_fuse_context(), ROOT_ID, &CString::new("a").unwrap())
                .unwrap()
                .inode,
            a1
        );
        assert!(fs
            .lookup(
                &create_fuse_context(),
                ROOT_ID,
                &CString::new("a_no").unwrap()
            )
            .is_err());
        assert_eq!(
            fs.lookup(&create_fuse_context(), a1, &CString::new("b").unwrap())
                .unwrap()
                .inode,
            b1
        );
        assert!(fs
            .lookup(&create_fuse_context(), a1, &CString::new("b_no").unwrap())
            .is_err());
        assert_eq!(
            fs.lookup(&create_fuse_context(), b1, &CString::new("c").unwrap())
                .unwrap()
                .inode,
            c1
        );
        assert!(fs
            .lookup(&create_fuse_context(), b1, &CString::new("c_no").unwrap())
            .is_err());

        assert_eq!(fs.path_walk("/a").unwrap(), Some(a1));
        assert_eq!(fs.path_walk("/a/b").unwrap(), Some(b1));
        assert_eq!(fs.path_walk("/a/b/c").unwrap(), Some(c1));
        assert_eq!(fs.path_walk("/a/b/d").unwrap(), None);
        assert_eq!(fs.path_walk("/a/b/c/d").unwrap(), None);

        fs.evict_inode(b1);
        fs.evict_inode(a1);
    }

    #[test]
    fn test_pseudofs_getattr() {
        let fs = PseudoFs::new();
        let a1 = fs.mount("/a").unwrap();

        fs.getattr(&create_fuse_context(), ROOT_ID, None).unwrap();
        fs.getattr(&create_fuse_context(), a1, None).unwrap();
        assert!(fs.getattr(&create_fuse_context(), 0x1000, None).is_err());

        fs.evict_inode(a1);
        fs.evict_inode(ROOT_ID);
    }

    #[test]
    fn test_pseudofs_readdir() {
        let fs = PseudoFs::new();
        let _ = fs.mount("/a").unwrap();
        let _ = fs.mount("/b").unwrap();

        fs.readdir(&create_fuse_context(), ROOT_ID, 0, 0, 0, &mut |_| Ok(1))
            .unwrap();
        fs.readdir(&create_fuse_context(), ROOT_ID, 0, 1, 0, &mut |_| Ok(1))
            .unwrap();
        fs.readdir(&create_fuse_context(), ROOT_ID, 0, 1, 1, &mut |_| Ok(1))
            .unwrap();
        fs.readdir(&create_fuse_context(), ROOT_ID, 0, 2, 0, &mut |_| Ok(1))
            .unwrap();
        fs.readdir(&create_fuse_context(), ROOT_ID, 0, 3, 0, &mut |_| Ok(1))
            .unwrap();
        fs.readdir(&create_fuse_context(), ROOT_ID, 0, 3, 3, &mut |_| Ok(1))
            .unwrap();
        assert!(fs
            .readdir(&create_fuse_context(), 0x1000, 0, 3, 0, &mut |_| Ok(1))
            .is_err());
    }

    #[test]
    fn test_pseudofs_readdir_plus() {
        let fs = PseudoFs::new();
        let _ = fs.mount("/a").unwrap();
        let _ = fs.mount("/b").unwrap();

        fs.readdirplus(&create_fuse_context(), ROOT_ID, 0, 0, 0, &mut |_, _| Ok(1))
            .unwrap();
        fs.readdirplus(&create_fuse_context(), ROOT_ID, 0, 1, 0, &mut |_, _| Ok(1))
            .unwrap();
        fs.readdirplus(&create_fuse_context(), ROOT_ID, 0, 1, 1, &mut |_, _| Ok(1))
            .unwrap();
        fs.readdirplus(&create_fuse_context(), ROOT_ID, 0, 2, 0, &mut |_, _| Ok(1))
            .unwrap();
        fs.readdirplus(&create_fuse_context(), ROOT_ID, 0, 3, 0, &mut |_, _| Ok(1))
            .unwrap();
        fs.readdirplus(&create_fuse_context(), ROOT_ID, 0, 3, 3, &mut |_, _| Ok(1))
            .unwrap();
        assert!(fs
            .readdirplus(&create_fuse_context(), 0x1000, 0, 3, 0, &mut |_, _| Ok(1))
            .is_err());
    }

    #[test]
    fn test_pseudofs_access() {
        let fs = PseudoFs::new();
        let a1 = fs.mount("/a").unwrap();
        let ctx = create_fuse_context();

        fs.access(&ctx, a1, 0).unwrap();
    }
}
