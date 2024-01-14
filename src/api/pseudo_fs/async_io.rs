use std::{ffi::CStr, pin::Pin, time::Duration};

use async_stream::try_stream;
use async_trait::async_trait;
use futures::{stream::BoxStream, Stream, StreamExt};
use libc::stat64;
use std::io::{Error, Result};

use crate::{
    abi::fuse_abi::CreateIn,
    api::{
        filesystem::{
            AsyncFileSystem, AsyncZeroCopyReader, AsyncZeroCopyWriter, Context, Entry, FileSystem,
            OpenOptions, OwnedDirEntry, SetattrValid,
        },
        pseudo_fs::PseudoFs,
    },
};

#[async_trait(?Send)]
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

    fn async_readdir<'a, 'b, 'async_trait>(
        &'a self,
        _ctx: &'b Context,
        inode: Self::Inode,
        _handle: Self::Handle,
        size: u32,
        offset: u64,
    ) -> Pin<Box<dyn Stream<Item = Result<OwnedDirEntry>> + Send + 'async_trait>>
    where
        'a: 'async_trait,
        'b: 'async_trait,
        Self: 'async_trait,
    {
        self.async_do_readdir(inode, size, offset)
    }

    fn async_readdirplus<'a, 'b, 'async_trait>(
        &'a self,
        _ctx: &'b Context,
        inode: Self::Inode,
        _handle: Self::Handle,
        size: u32,
        offset: u64,
    ) -> Pin<Box<dyn Stream<Item = Result<(OwnedDirEntry, Entry)>> + Send + 'async_trait>>
    where
        'a: 'async_trait,
        'b: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(try_stream! {
            let mut stream = self.async_do_readdir(inode, size, offset);
            while let Some(dir_entry) = stream.next().await {
                let dir_entry = dir_entry?;
                let entry = self.get_entry(dir_entry.ino);
                yield (dir_entry, entry);
            }
        })
    }
}

impl PseudoFs {
    fn async_do_readdir<'a, 'b>(
        &'a self,
        parent: u64,
        size: u32,
        offset: u64,
    ) -> BoxStream<Result<OwnedDirEntry>>
    where
        'a: 'b,
        Self: 'b,
    {
        Box::pin(try_stream! {
            if size == 0 {
                return;
            }

            let inodes = self.inodes.load();
            let inode = inodes
                .get(&parent)
                .ok_or_else(|| Error::from_raw_os_error(libc::ENOENT))?;
            let children = inode.children.load();

            if offset >= children.len() as u64 {
                return;
            }

            let mut next = offset + 1;
            for child in children[offset as usize..].iter() {
                yield OwnedDirEntry {
                    ino: child.ino,
                    offset: next,
                    type_: 0,
                    name: child.name.clone().into(),
                };
                next += 1;
            }
        })
    }
}
