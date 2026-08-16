//! Windows physical-file safety helpers.
//!
//! The frozen sealer rules require final-open-handle FileId deduplication and
//! reparse-point rejection for the project root, root manifest, CAS root, and
//! every source path component. These helpers intentionally use the Windows
//! metadata identity rather than path strings or ordinary `canonicalize`.

use std::fs::Metadata;
use std::path::Path;

#[cfg(windows)]
use std::os::windows::fs::MetadataExt;
#[cfg(windows)]
use std::os::windows::io::AsRawHandle;
#[cfg(windows)]
use windows_sys::Win32::Storage::FileSystem::{
    GetFileInformationByHandle, BY_HANDLE_FILE_INFORMATION,
};

const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x0400;

/// Physical file identity on Windows: `(volume_serial_number, file_index)`.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct FileIdentity {
    pub volume_serial_number: u32,
    pub file_index: u64,
}

/// True when the metadata describes a symlink, junction, or other reparse
/// point.
pub fn is_reparse_point(metadata: &Metadata) -> bool {
    #[cfg(windows)]
    {
        metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0
    }
    #[cfg(not(windows))]
    {
        metadata.file_type().is_symlink()
    }
}

/// Read metadata without following symlinks. This is the correct check for
/// path components that may themselves be reparse points.
pub fn symlink_metadata(path: &Path) -> std::io::Result<Metadata> {
    std::fs::symlink_metadata(path)
}

/// Read identity from an already-open handle.
pub fn identity_from_open_file(file: &std::fs::File) -> std::io::Result<Option<FileIdentity>> {
    #[cfg(windows)]
    {
        let mut info = std::mem::MaybeUninit::<BY_HANDLE_FILE_INFORMATION>::uninit();
        // SAFETY: `info` is valid for this call and is initialized on success.
        let result =
            unsafe { GetFileInformationByHandle(file.as_raw_handle() as _, info.as_mut_ptr()) };
        if result == 0 {
            return Err(std::io::Error::last_os_error());
        }
        // SAFETY: the call returned success, so `info` is initialized.
        let info = unsafe { info.assume_init() };
        Ok(Some(FileIdentity {
            volume_serial_number: info.dwVolumeSerialNumber,
            file_index: ((info.nFileIndexHigh as u64) << 32) | info.nFileIndexLow as u64,
        }))
    }
    #[cfg(not(windows))]
    {
        let _ = file;
        Ok(None)
    }
}

/// Fail closed when `path` exists and is a reparse point.
pub fn ensure_no_reparse(path: &Path) -> std::io::Result<()> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) => {
            if is_reparse_point(&metadata) {
                return Err(std::io::Error::other(format!(
                    "reparse point forbidden: {}",
                    path.display()
                )));
            }
            Ok(())
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

/// Ensure every existing ancestor component from `project_root` (inclusive)
/// down to and including `path` is not a reparse point.
pub fn ensure_no_reparse_for_path(project_root: &Path, path: &Path) -> std::io::Result<()> {
    ensure_no_reparse(project_root)?;
    let relative = path.strip_prefix(project_root).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "path escapes the project root",
        )
    })?;
    let mut current = project_root.to_path_buf();
    for component in relative.components() {
        current.push(component.as_os_str());
        ensure_no_reparse(&current)?;
    }
    Ok(())
}
