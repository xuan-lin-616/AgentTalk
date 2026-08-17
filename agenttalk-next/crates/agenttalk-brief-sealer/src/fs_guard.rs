//! Windows physical-file safety helpers.
//!
//! Component traversal uses `NtCreateFile` with a `RootDirectory` parent
//! handle. Every path component after the project root is opened relative to
//! the already-verified parent directory handle, with
//! `FILE_OPEN_REPARSE_POINT | FILE_OPEN_FOR_BACKUP_INTENT`, and the returned
//! handle is checked for `FILE_ATTRIBUTE_REPARSE_POINT`. File handles add
//! `FILE_SYNCHRONOUS_IO_NONALERT` so the exact same handle can be read with
//! `std::io::Read`. There is no check-then-reopen-by-absolute-path step.

use std::fs::File;
use std::io;
use std::mem::{size_of, zeroed};
use std::path::{Path, PathBuf};

#[cfg(windows)]
use std::os::windows::ffi::OsStrExt;
#[cfg(windows)]
use std::os::windows::fs::MetadataExt;
#[cfg(windows)]
use std::os::windows::io::{AsRawHandle, FromRawHandle};
#[cfg(windows)]
use windows_sys::Wdk::Foundation::OBJECT_ATTRIBUTES;
#[cfg(windows)]
use windows_sys::Wdk::Storage::FileSystem::{
    NtCreateFile, FILE_DIRECTORY_FILE, FILE_OPEN, FILE_OPEN_FOR_BACKUP_INTENT,
    FILE_OPEN_REPARSE_POINT as NT_FILE_OPEN_REPARSE_POINT,
};
#[cfg(windows)]
use windows_sys::Win32::Foundation::{INVALID_HANDLE_VALUE, UNICODE_STRING};
#[cfg(windows)]
use windows_sys::Win32::Storage::FileSystem::{
    CreateFileW, GetFileInformationByHandle, BY_HANDLE_FILE_INFORMATION,
    FILE_FLAG_BACKUP_SEMANTICS, FILE_FLAG_OPEN_REPARSE_POINT, OPEN_EXISTING,
};
#[cfg(windows)]
use windows_sys::Win32::System::IO::IO_STATUS_BLOCK;

const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x0400;
const OBJ_CASE_INSENSITIVE: u32 = 0x0000_0040;
const FILE_READ_DATA: u32 = 0x0001;
const FILE_READ_ATTRIBUTES: u32 = 0x0080;
const FILE_WRITE_ATTRIBUTES: u32 = 0x0100;
const FILE_LIST_DIRECTORY: u32 = 0x0001;
const SYNCHRONIZE: u32 = 0x0010_0000;
const FILE_SYNCHRONOUS_IO_NONALERT: u32 = 0x0020;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct FileIdentity {
    pub volume_serial_number: u32,
    pub file_index: u64,
}

pub fn is_reparse_point(metadata: &std::fs::Metadata) -> bool {
    #[cfg(windows)]
    {
        metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0
    }
    #[cfg(not(windows))]
    {
        metadata.file_type().is_symlink()
    }
}

#[cfg(windows)]
pub fn open_root_handle(path: &Path) -> io::Result<File> {
    let wide = path
        .as_os_str()
        .encode_wide()
        .chain(Some(0))
        .collect::<Vec<_>>();
    let handle = unsafe {
        CreateFileW(
            wide.as_ptr(),
            FILE_READ_DATA
                | FILE_READ_ATTRIBUTES
                | FILE_WRITE_ATTRIBUTES
                | FILE_LIST_DIRECTORY
                | SYNCHRONIZE,
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
    Ok(unsafe { File::from_raw_handle(handle as _) })
}

#[cfg(not(windows))]
pub fn open_root_handle(path: &Path) -> io::Result<File> {
    File::open(path)
}
#[cfg(windows)]
fn open_child_relative(parent: &File, component: &str, open_directory: bool) -> io::Result<File> {
    let mut wide: Vec<u16> = component.encode_utf16().collect();
    let mut uni = UNICODE_STRING {
        Length: (wide.len() * 2) as u16,
        MaximumLength: (wide.len() * 2) as u16,
        Buffer: wide.as_mut_ptr(),
    };
    let mut obj = OBJECT_ATTRIBUTES {
        Length: size_of::<OBJECT_ATTRIBUTES>() as u32,
        RootDirectory: parent.as_raw_handle() as _,
        ObjectName: &mut uni as *mut UNICODE_STRING,
        Attributes: OBJ_CASE_INSENSITIVE,
        SecurityDescriptor: std::ptr::null_mut(),
        SecurityQualityOfService: std::ptr::null_mut(),
    };
    let mut io_status: IO_STATUS_BLOCK = unsafe { zeroed() };
    let mut handle = INVALID_HANDLE_VALUE;
    let (desired_access, create_options) = if open_directory {
        (
            FILE_LIST_DIRECTORY | FILE_READ_ATTRIBUTES | FILE_WRITE_ATTRIBUTES | SYNCHRONIZE,
            NT_FILE_OPEN_REPARSE_POINT | FILE_OPEN_FOR_BACKUP_INTENT | FILE_DIRECTORY_FILE,
        )
    } else {
        (
            FILE_READ_DATA | FILE_READ_ATTRIBUTES | SYNCHRONIZE,
            NT_FILE_OPEN_REPARSE_POINT | FILE_OPEN_FOR_BACKUP_INTENT | FILE_SYNCHRONOUS_IO_NONALERT,
        )
    };
    let status = unsafe {
        NtCreateFile(
            &mut handle as *mut _,
            desired_access,
            &mut obj as *mut OBJECT_ATTRIBUTES,
            &mut io_status,
            std::ptr::null_mut(),
            0,
            windows_sys::Win32::Storage::FileSystem::FILE_SHARE_READ
                | windows_sys::Win32::Storage::FileSystem::FILE_SHARE_WRITE
                | windows_sys::Win32::Storage::FileSystem::FILE_SHARE_DELETE,
            FILE_OPEN,
            create_options,
            std::ptr::null_mut(),
            0,
        )
    };
    if status == 0xC000_0034u32 as i32 || status == 0xC000_003Au32 as i32 {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!("component not found: {component:?}"),
        ));
    }
    if status < 0 {
        return Err(io::Error::other(format!(
            "NtCreateFile failed for component {component:?}: NTSTATUS {status:#x}"
        )));
    }
    Ok(unsafe { File::from_raw_handle(handle as _) })
}

#[cfg(not(windows))]
fn open_child_relative(_parent: &File, component: &str, _open_directory: bool) -> io::Result<File> {
    File::open(component)
}

pub fn open_relative_components(
    root_handle: &File,
    relative: &Path,
    final_is_dir: bool,
) -> io::Result<File> {
    let components: Vec<String> = relative
        .components()
        .map(|component| component.as_os_str().to_string_lossy().into_owned())
        .collect();
    if components.is_empty() {
        return Err(io::Error::other("empty relative path"));
    }
    let mut current = None;
    let mut parent = root_handle;
    for (index, component) in components.iter().enumerate() {
        let is_last = index + 1 == components.len();
        let open_directory = if is_last { final_is_dir } else { true };
        let file = open_child_relative(parent, component, open_directory)?;
        if handle_is_reparse(&file)? {
            return Err(io::Error::other(format!(
                "reparse point forbidden: {component}"
            )));
        }
        current = Some(file);
        parent = current.as_ref().expect("current file set");
    }
    current.ok_or_else(|| io::Error::other("empty relative path"))
}

/// Open a final directory component with write access for a subsequent
/// `FlushFileBuffers`. Intermediate components use the read-only no-follow
/// traversal from `open_child_relative`.
#[cfg(windows)]
fn open_child_relative_write(parent: &File, component: &str) -> io::Result<File> {
    let mut wide: Vec<u16> = component.encode_utf16().collect();
    let mut uni = UNICODE_STRING {
        Length: (wide.len() * 2) as u16,
        MaximumLength: (wide.len() * 2) as u16,
        Buffer: wide.as_mut_ptr(),
    };
    let mut obj = OBJECT_ATTRIBUTES {
        Length: size_of::<OBJECT_ATTRIBUTES>() as u32,
        RootDirectory: parent.as_raw_handle() as _,
        ObjectName: &mut uni as *mut UNICODE_STRING,
        Attributes: OBJ_CASE_INSENSITIVE,
        SecurityDescriptor: std::ptr::null_mut(),
        SecurityQualityOfService: std::ptr::null_mut(),
    };
    let mut io_status: IO_STATUS_BLOCK = unsafe { zeroed() };
    let mut handle = INVALID_HANDLE_VALUE;
    let status = unsafe {
        NtCreateFile(
            &mut handle as *mut _,
            windows_sys::Win32::Storage::FileSystem::FILE_GENERIC_READ
                | windows_sys::Win32::Storage::FileSystem::FILE_GENERIC_WRITE,
            &mut obj as *mut OBJECT_ATTRIBUTES,
            &mut io_status,
            std::ptr::null_mut(),
            0,
            windows_sys::Win32::Storage::FileSystem::FILE_SHARE_READ
                | windows_sys::Win32::Storage::FileSystem::FILE_SHARE_WRITE
                | windows_sys::Win32::Storage::FileSystem::FILE_SHARE_DELETE,
            FILE_OPEN,
            NT_FILE_OPEN_REPARSE_POINT | FILE_OPEN_FOR_BACKUP_INTENT | FILE_DIRECTORY_FILE,
            std::ptr::null_mut(),
            0,
        )
    };
    if status < 0 {
        return Err(io::Error::other(format!(
            "NtCreateFile(write-directory) failed for component {component:?}: NTSTATUS {status:#x}"
        )));
    }
    Ok(unsafe { File::from_raw_handle(handle as _) })
}

/// Open `relative` directories and flush the final directory handle. This is
/// the durable metadata step for the CAS object directory entry.
#[cfg(windows)]
pub fn flush_directory_relative(root_handle: &File, relative: &Path) -> io::Result<()> {
    let components: Vec<String> = relative
        .components()
        .map(|component| component.as_os_str().to_string_lossy().into_owned())
        .collect();
    if components.is_empty() {
        return Err(io::Error::other("empty relative path"));
    }
    let mut parent = root_handle;
    let mut current = None;
    for (index, component) in components.iter().enumerate() {
        let is_last = index + 1 == components.len();
        let file = if is_last {
            open_child_relative_write(parent, component)?
        } else {
            open_child_relative(parent, component, true)?
        };
        if handle_is_reparse(&file)? {
            return Err(io::Error::other(format!(
                "reparse point forbidden: {component}"
            )));
        }
        current = Some(file);
        parent = current.as_ref().expect("current file set");
    }
    let final_file = current.ok_or_else(|| io::Error::other("empty relative path"))?;
    let ok = unsafe {
        windows_sys::Win32::Storage::FileSystem::FlushFileBuffers(final_file.as_raw_handle() as _)
    };
    if ok == 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(not(windows))]
pub fn flush_directory_relative(_root_handle: &File, _relative: &Path) -> io::Result<()> {
    Ok(())
}

pub fn identity_from_open_file(file: &File) -> io::Result<Option<FileIdentity>> {
    #[cfg(windows)]
    {
        let mut info = std::mem::MaybeUninit::<BY_HANDLE_FILE_INFORMATION>::uninit();
        let result =
            unsafe { GetFileInformationByHandle(file.as_raw_handle() as _, info.as_mut_ptr()) };
        if result == 0 {
            return Err(io::Error::last_os_error());
        }
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

pub fn handle_is_reparse(file: &File) -> io::Result<bool> {
    Ok(is_reparse_point(&file.metadata()?))
}

pub fn final_path_from_handle(file: &File) -> io::Result<PathBuf> {
    #[cfg(windows)]
    {
        let mut buffer = vec![0u16; 4096];
        let length = unsafe {
            windows_sys::Win32::Storage::FileSystem::GetFinalPathNameByHandleW(
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
    {
        let _ = file;
        Ok(PathBuf::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn open_relative_components_opens_and_reads_regular_file_from_parent_handle() {
        let root = std::env::temp_dir().join(format!(
            "agenttalk-fsguard-test-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&root).unwrap();
        fs::write(root.join("file.txt"), b"abc").unwrap();

        let root_handle = open_root_handle(&root).unwrap();
        let mut file =
            open_relative_components(&root_handle, Path::new("file.txt"), false).unwrap();
        assert!(!handle_is_reparse(&file).unwrap());
        assert!(identity_from_open_file(&file).unwrap().is_some());
        let mut bytes = Vec::new();
        std::io::Read::read_to_end(&mut file, &mut bytes).unwrap();
        assert_eq!(bytes, b"abc");
        fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn open_relative_components_opens_directory_component() {
        let root = std::env::temp_dir().join(format!(
            "agenttalk-fsguard-dir-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(root.join(".agenttalk").join("objects")).unwrap();
        let root_handle = open_root_handle(&root).unwrap();
        let dir = open_relative_components(&root_handle, Path::new(".agenttalk"), true).unwrap();
        assert!(!handle_is_reparse(&dir).unwrap());
        let objs = open_relative_components(&dir, Path::new("objects"), true).unwrap();
        assert!(!handle_is_reparse(&objs).unwrap());
        fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn open_relative_components_rejects_directory_component_from_file() {
        let root = std::env::temp_dir().join(format!(
            "agenttalk-fsguard-root-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&root).unwrap();
        fs::write(root.join("file.txt"), b"abc").unwrap();
        let root_handle = open_root_handle(&root).unwrap();
        assert!(open_relative_components(&root_handle, Path::new("file.txt"), true).is_err());
        fs::remove_dir_all(&root).unwrap();
    }
}
