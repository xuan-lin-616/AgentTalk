//! Windows physical-file safety helpers.
//!
//! The frozen sealer rules require final-open-handle FileId deduplication and
//! reparse-point rejection for the project root, root manifest, CAS root, and
//! every source path component. These helpers open with
//! `FILE_FLAG_OPEN_REPARSE_POINT | FILE_FLAG_BACKUP_SEMANTICS` and derive the
//! identity and reparse decision from the returned handle, never from a
//! pre-open path check.

use std::fs::{File, Metadata};
use std::io;
use std::path::{Path, PathBuf};

#[cfg(windows)]
use std::os::windows::ffi::OsStrExt;
#[cfg(windows)]
use std::os::windows::fs::MetadataExt;
#[cfg(windows)]
use std::os::windows::io::{AsRawHandle, FromRawHandle};
#[cfg(windows)]
use windows_sys::Win32::Foundation::INVALID_HANDLE_VALUE;
#[cfg(windows)]
use windows_sys::Win32::Storage::FileSystem::{
    CreateFileW, GetFileInformationByHandle, GetFinalPathNameByHandleW, BY_HANDLE_FILE_INFORMATION,
    FILE_FLAG_BACKUP_SEMANTICS, FILE_FLAG_OPEN_REPARSE_POINT, OPEN_EXISTING,
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

/// Open an existing path with reparse-point traversal disabled for the final
/// path element. Intermediate directory traversal is guarded separately by
/// opening each existing component with the same no-follow flag and by the
/// final-handle path check.
#[cfg(windows)]
pub fn open_no_follow(path: &Path) -> io::Result<File> {
    let wide = path
        .as_os_str()
        .encode_wide()
        .chain(Some(0))
        .collect::<Vec<_>>();
    let handle = unsafe {
        CreateFileW(
            wide.as_ptr(),
            windows_sys::Win32::Storage::FileSystem::FILE_GENERIC_READ,
            windows_sys::Win32::Storage::FileSystem::FILE_SHARE_READ
                | windows_sys::Win32::Storage::FileSystem::FILE_SHARE_WRITE
                | windows_sys::Win32::Storage::FileSystem::FILE_SHARE_DELETE,
            std::ptr::null_mut(),
            OPEN_EXISTING,
            FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT,
            std::ptr::null_mut(),
        )
    };
    if handle == INVALID_HANDLE_VALUE {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: the handle was newly created and is owned by the returned File.
    Ok(unsafe { File::from_raw_handle(handle as _) })
}

#[cfg(not(windows))]
pub fn open_no_follow(path: &Path) -> io::Result<File> {
    File::open(path)
}

/// Read identity from an already-open handle.
pub fn identity_from_open_file(file: &File) -> io::Result<Option<FileIdentity>> {
    #[cfg(windows)]
    {
        let mut info = std::mem::MaybeUninit::<BY_HANDLE_FILE_INFORMATION>::uninit();
        // SAFETY: `info` is valid for this call and is initialized on success.
        let result =
            unsafe { GetFileInformationByHandle(file.as_raw_handle() as _, info.as_mut_ptr()) };
        if result == 0 {
            return Err(io::Error::last_os_error());
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

/// Final path reported by the operating system for an open handle.
#[cfg(windows)]
pub fn final_path_from_handle(file: &File) -> io::Result<PathBuf> {
    let mut buffer = vec![0u16; 4096];
    let length = unsafe {
        GetFinalPathNameByHandleW(
            file.as_raw_handle() as _,
            buffer.as_mut_ptr(),
            buffer.len() as u32,
            0,
        )
    };
    if length == 0 {
        return Err(io::Error::last_os_error());
    }
    if length as usize > buffer.len() {
        return Err(io::Error::other("final path buffer too small"));
    }
    let path = String::from_utf16_lossy(&buffer[..length as usize]);
    Ok(PathBuf::from(path))
}

#[cfg(not(windows))]
pub fn final_path_from_handle(_file: &File) -> io::Result<PathBuf> {
    Ok(PathBuf::new())
}

/// Return `true` when the open handle itself is a reparse point.
pub fn handle_is_reparse(file: &File) -> io::Result<bool> {
    Ok(is_reparse_point(&file.metadata()?))
}

/// Ensure every existing component from `project_root` (inclusive) down to
/// and including `path` can be opened without following a final-component
/// reparse point. This is a component-level no-follow traversal guard used
/// immediately before the final handle open.
pub fn ensure_no_reparse_components(project_root: &Path, path: &Path) -> io::Result<()> {
    let _ = open_no_follow(project_root)?;
    let relative = path
        .strip_prefix(project_root)
        .map_err(|_| io::Error::other("path escapes the project root"))?;
    let mut current = project_root.to_path_buf();
    for component in relative.components() {
        current.push(component.as_os_str());
        let file = match open_no_follow(&current) {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(error),
        };
        if handle_is_reparse(&file)? {
            return Err(io::Error::other(format!(
                "reparse point forbidden: {}",
                current.display()
            )));
        }
    }
    Ok(())
}

/// Validate that an opened handle still resolves under the project root final
/// path. This is the final-handle evidence that no intermediate component
/// escaped the project root when the handle was opened.
pub fn ensure_handle_under_root(root_handle: &File, file_handle: &File) -> io::Result<()> {
    #[cfg(windows)]
    {
        let root_path = final_path_from_handle(root_handle)?;
        let file_path = final_path_from_handle(file_handle)?;
        let root_norm = normalize_final_path(&root_path);
        let file_norm = normalize_final_path(&file_path);
        if !file_norm.starts_with(&root_norm) {
            return Err(io::Error::other(format!(
                "opened handle escapes project root: {} is not under {}",
                file_path.display(),
                root_path.display()
            )));
        }
        Ok(())
    }
    #[cfg(not(windows))]
    {
        let _ = (root_handle, file_handle);
        Ok(())
    }
}

#[cfg(windows)]
fn normalize_final_path(path: &Path) -> String {
    path.to_string_lossy()
        .trim_start_matches("\\?\\")
        .trim_end_matches(char::from(92))
        .to_lowercase()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn open_no_follow_returns_handle_and_identity_for_regular_file() {
        let root = std::env::temp_dir().join(format!(
            "agenttalk-fsguard-test-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&root).unwrap();
        let file_path = root.join("file.txt");
        fs::write(&file_path, b"abc").unwrap();

        let file = open_no_follow(&file_path).unwrap();
        assert!(!handle_is_reparse(&file).unwrap());
        assert!(identity_from_open_file(&file).unwrap().is_some());
        fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn handle_under_root_rejects_escape_deterministically() {
        let root = std::env::temp_dir().join(format!(
            "agenttalk-fsguard-root-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let outside = std::env::temp_dir().join(format!(
            "agenttalk-fsguard-outside-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&root).unwrap();
        fs::create_dir_all(&outside).unwrap();
        fs::write(outside.join("outside.txt"), b"x").unwrap();

        let root_handle = open_no_follow(&root).unwrap();
        let outside_handle = open_no_follow(&outside.join("outside.txt")).unwrap();
        assert!(ensure_handle_under_root(&root_handle, &outside_handle).is_err());

        fs::remove_dir_all(&root).unwrap();
        fs::remove_dir_all(&outside).unwrap();
    }
}
