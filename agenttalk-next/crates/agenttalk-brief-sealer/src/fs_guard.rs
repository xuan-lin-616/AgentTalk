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
    FileDispositionInformation, FileLinkInformation, NtCreateFile, NtSetInformationFile,
    FILE_DIRECTORY_FILE, FILE_DISPOSITION_INFORMATION, FILE_LINK_INFORMATION, FILE_OPEN,
    FILE_OPEN_FOR_BACKUP_INTENT, FILE_OPEN_REPARSE_POINT as NT_FILE_OPEN_REPARSE_POINT,
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
const FILE_DELETE_ACCESS: u32 = 0x0001_0000;
const FILE_ADD_SUBDIRECTORY: u32 = 0x0004;
const FILE_WRITE_DATA: u32 = 0x0002;
const STATUS_OBJECT_NAME_COLLISION: i32 = 0xC000_0035u32 as i32;
const FILE_LIST_DIRECTORY: u32 = 0x0001;
const SYNCHRONIZE: u32 = 0x0010_0000;
const FILE_SYNCHRONOUS_IO_NONALERT: u32 = 0x0020;

#[cfg(test)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct TraceEntry {
    pub parent_handle: usize,
    pub child_handle: usize,
    pub component: String,
    pub open_directory: bool,
    pub open_reparse_point: bool,
    pub reparse_checked: bool,
}

#[cfg(test)]
thread_local! {
    pub(crate) static OPEN_TRACE: std::cell::RefCell<Vec<TraceEntry>> =
        const { std::cell::RefCell::new(Vec::new()) };
}

pub(crate) fn reparse_io_error(component: &str) -> io::Error {
    io::Error::other(format!("reparse point forbidden: {component}"))
}

pub(crate) fn is_reparse_error(error: &io::Error) -> bool {
    error.to_string().contains("reparse point forbidden")
}

fn check_reparse(file: &File, component: &str) -> io::Result<()> {
    if handle_is_reparse(file)? {
        return Err(reparse_io_error(component));
    }
    #[cfg(test)]
    OPEN_TRACE.with(|trace| {
        if let Some(entry) = trace.borrow_mut().last_mut() {
            entry.reparse_checked = true;
        }
    });
    Ok(())
}

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
            FILE_LIST_DIRECTORY | FILE_READ_ATTRIBUTES | FILE_ADD_SUBDIRECTORY | SYNCHRONIZE,
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
    let file = unsafe { File::from_raw_handle(handle as _) };
    #[cfg(test)]
    OPEN_TRACE.with(|trace| {
        trace.borrow_mut().push(TraceEntry {
            parent_handle: parent.as_raw_handle() as usize,
            child_handle: file.as_raw_handle() as usize,
            component: component.to_owned(),
            open_directory,
            open_reparse_point: true,
            reparse_checked: false,
        });
    });
    Ok(file)
}

#[cfg(not(windows))]
fn open_child_relative(_parent: &File, component: &str, _open_directory: bool) -> io::Result<File> {
    File::open(component)
}

#[cfg(windows)]
pub fn open_or_create_directory_relative(parent: &File, component: &str) -> io::Result<File> {
    match open_child_relative(parent, component, true) {
        Ok(file) => Ok(file),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
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
                    FILE_LIST_DIRECTORY
                        | FILE_READ_ATTRIBUTES
                        | FILE_ADD_SUBDIRECTORY
                        | SYNCHRONIZE,
                    &mut obj as *mut OBJECT_ATTRIBUTES,
                    &mut io_status,
                    std::ptr::null_mut(),
                    0,
                    windows_sys::Win32::Storage::FileSystem::FILE_SHARE_READ
                        | windows_sys::Win32::Storage::FileSystem::FILE_SHARE_WRITE
                        | windows_sys::Win32::Storage::FileSystem::FILE_SHARE_DELETE,
                    windows_sys::Wdk::Storage::FileSystem::FILE_OPEN_IF,
                    NT_FILE_OPEN_REPARSE_POINT | FILE_OPEN_FOR_BACKUP_INTENT | FILE_DIRECTORY_FILE,
                    std::ptr::null_mut(),
                    0,
                )
            };
            if status < 0 {
                return Err(io::Error::other(format!(
                    "NtCreateFile(open-or-create-directory) failed for {component:?}: NTSTATUS {status:#x}"
                )));
            }
            Ok(unsafe { File::from_raw_handle(handle as _) })
        }
        Err(error) => Err(error),
    }
}

#[cfg(not(windows))]
pub fn open_or_create_directory_relative(_parent: &File, component: &str) -> io::Result<File> {
    std::fs::create_dir(component)?;
    File::open(component)
}

#[cfg(windows)]
pub fn create_file_relative_new(parent: &File, component: &str) -> io::Result<File> {
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
            FILE_WRITE_DATA | FILE_READ_ATTRIBUTES | SYNCHRONIZE | FILE_DELETE_ACCESS,
            &mut obj as *mut OBJECT_ATTRIBUTES,
            &mut io_status,
            std::ptr::null_mut(),
            0,
            windows_sys::Win32::Storage::FileSystem::FILE_SHARE_READ
                | windows_sys::Win32::Storage::FileSystem::FILE_SHARE_WRITE
                | windows_sys::Win32::Storage::FileSystem::FILE_SHARE_DELETE,
            windows_sys::Wdk::Storage::FileSystem::FILE_CREATE,
            NT_FILE_OPEN_REPARSE_POINT | FILE_OPEN_FOR_BACKUP_INTENT | FILE_SYNCHRONOUS_IO_NONALERT,
            std::ptr::null_mut(),
            0,
        )
    };
    if status < 0 {
        return Err(io::Error::other(format!(
            "NtCreateFile(create-new-file) failed for {component:?}: NTSTATUS {status:#x}"
        )));
    }
    Ok(unsafe { File::from_raw_handle(handle as _) })
}

#[cfg(not(windows))]
pub fn create_file_relative_new(_parent: &File, component: &str) -> io::Result<File> {
    std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(component)
}

#[cfg(windows)]
pub fn link_file_relative(objects_dir: &File, file_handle: &File, name: &str) -> io::Result<()> {
    let name_utf16: Vec<u16> = name.encode_utf16().collect();
    let mut link: Vec<u8> =
        vec![0; size_of::<FILE_LINK_INFORMATION>() + name_utf16.len().saturating_sub(1) * 2];
    let link_ptr = link.as_mut_ptr() as *mut FILE_LINK_INFORMATION;
    unsafe {
        (*link_ptr).Anonymous.ReplaceIfExists = 0;
        (*link_ptr).RootDirectory = objects_dir.as_raw_handle() as _;
        (*link_ptr).FileNameLength = (name_utf16.len() * 2) as u32;
        std::ptr::copy_nonoverlapping(
            name_utf16.as_ptr(),
            (*link_ptr).FileName.as_mut_ptr(),
            name_utf16.len(),
        );
    }
    let mut io_status: IO_STATUS_BLOCK = unsafe { zeroed() };
    let status = unsafe {
        NtSetInformationFile(
            file_handle.as_raw_handle() as _,
            &mut io_status,
            link_ptr as *const _,
            link.len() as u32,
            FileLinkInformation,
        )
    };
    if status == STATUS_OBJECT_NAME_COLLISION {
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            "object name collision",
        ));
    }
    if status < 0 {
        return Err(io::Error::other(format!(
            "NtSetInformationFile(FileLinkInformation) failed: {status:#x}"
        )));
    }
    Ok(())
}

#[cfg(not(windows))]
pub fn link_file_relative(_objects_dir: &File, _file_handle: &File, _name: &str) -> io::Result<()> {
    Err(io::Error::other(
        "handle-relative hard link unsupported on this platform",
    ))
}

#[cfg(windows)]
pub fn delete_file_by_handle(file: &File) -> io::Result<()> {
    let mut disposition = FILE_DISPOSITION_INFORMATION { DeleteFile: 1 };
    let mut io_status: IO_STATUS_BLOCK = unsafe { zeroed() };
    let status = unsafe {
        NtSetInformationFile(
            file.as_raw_handle() as _,
            &mut io_status,
            &mut disposition as *mut FILE_DISPOSITION_INFORMATION as *const _,
            size_of::<FILE_DISPOSITION_INFORMATION>() as u32,
            FileDispositionInformation,
        )
    };
    if status < 0 {
        return Err(io::Error::other(format!(
            "NtSetInformationFile(FileDispositionInformation) failed: {status:#x}"
        )));
    }
    Ok(())
}

#[cfg(not(windows))]
pub fn delete_file_by_handle(_file: &File) -> io::Result<()> {
    Ok(())
}

#[cfg(windows)]
pub fn flush_file_handle(file: &File) -> io::Result<()> {
    let ok = unsafe {
        windows_sys::Win32::Storage::FileSystem::FlushFileBuffers(file.as_raw_handle() as _)
    };
    if ok == 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

#[cfg(not(windows))]
pub fn flush_file_handle(_file: &File) -> io::Result<()> {
    Ok(())
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
        check_reparse(&file, component)?;
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

#[cfg(windows)]
pub fn flush_directory_handle(file: &File) -> io::Result<()> {
    let ok = unsafe {
        windows_sys::Win32::Storage::FileSystem::FlushFileBuffers(file.as_raw_handle() as _)
    };
    if ok == 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

#[cfg(not(windows))]
pub fn flush_directory_handle(_file: &File) -> io::Result<()> {
    Ok(())
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
    fn trace_proves_parent_handle_chain_and_no_follow_flags() {
        let root = std::env::temp_dir().join(format!(
            "agenttalk-fsguard-trace-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(root.join("plan")).unwrap();
        fs::write(root.join("plan/roadmap.md"), b"abc").unwrap();

        let root_handle = open_root_handle(&root).unwrap();
        OPEN_TRACE.with(|trace| trace.borrow_mut().clear());
        let mut file =
            open_relative_components(&root_handle, Path::new("plan/roadmap.md"), false).unwrap();
        let mut bytes = Vec::new();
        std::io::Read::read_to_end(&mut file, &mut bytes).unwrap();
        assert_eq!(bytes, b"abc");

        let trace = OPEN_TRACE.with(|trace| trace.borrow().clone());
        assert_eq!(trace.len(), 2);
        assert_eq!(trace[0].parent_handle, root_handle.as_raw_handle() as usize);
        assert_eq!(trace[0].component, "plan");
        assert!(trace[0].open_directory);
        assert!(trace[0].open_reparse_point);
        assert!(trace[0].reparse_checked);
        assert_eq!(trace[1].parent_handle, trace[0].child_handle);
        assert_eq!(trace[1].component, "roadmap.md");
        assert!(!trace[1].open_directory);
        assert!(trace[1].open_reparse_point);
        assert!(trace[1].reparse_checked);
        fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn reparse_error_mapping_branch_is_deterministic() {
        let error = reparse_io_error("x");
        assert!(is_reparse_error(&error));
        assert!(!is_reparse_error(&io::Error::other("ordinary io failure")));
    }

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
