use agenttalk_protocols::{decode_frame, encode_frame, FrameError, DEFAULT_MAX_MESSAGE_BYTES};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum TransportError {
    #[error(transparent)]
    Frame(#[from] FrameError),
    #[error("transport is unavailable on this platform")]
    Unsupported,
    #[error("named pipe is closed")]
    Closed,
    #[error("Windows named pipe error {0}")]
    Windows(u32),
}

pub trait FramedTransport {
    fn write_json<T: serde::Serialize>(&mut self, value: &T) -> Result<(), TransportError>;
    fn read_json(&mut self) -> Result<Vec<u8>, TransportError>;
}

/// The read side of a framed transport.
pub trait FramedReader {
    fn read_json(&mut self) -> Result<Vec<u8>, TransportError>;
    fn try_read_json(&mut self) -> Result<Option<Vec<u8>>, TransportError>;
}

/// The write side of a framed transport.
pub trait FramedWriter {
    fn write_json<T: serde::Serialize>(&mut self, value: &T) -> Result<(), TransportError>;
}

#[cfg(windows)]
mod windows_named_pipe {
    use super::*;
    use std::os::windows::ffi::OsStrExt;
    use std::ptr::null_mut;
    use std::{mem, slice};
    use windows_sys::Win32::Foundation::{
        CloseHandle, DuplicateHandle, GetLastError, LocalFree, DUPLICATE_SAME_ACCESS,
        ERROR_PIPE_CONNECTED, ERROR_SUCCESS, FALSE, HANDLE, HLOCAL, INVALID_HANDLE_VALUE, TRUE,
    };
    use windows_sys::Win32::Security::Authorization::{GetSecurityInfo, SE_KERNEL_OBJECT};
    use windows_sys::Win32::Security::{
        AddAccessAllowedAceEx, CreateWellKnownSid, GetAce, GetLengthSid,
        GetSecurityDescriptorControl, GetSecurityDescriptorDacl, GetTokenInformation,
        InitializeAcl, InitializeSecurityDescriptor, SetSecurityDescriptorControl,
        SetSecurityDescriptorDacl, TokenUser, WinLocalSystemSid, ACCESS_ALLOWED_ACE, ACL,
        ACL_REVISION, PSECURITY_DESCRIPTOR, PSID, SECURITY_ATTRIBUTES, SECURITY_DESCRIPTOR,
        SE_DACL_PROTECTED, TOKEN_QUERY, TOKEN_USER, WELL_KNOWN_SID_TYPE,
    };
    use windows_sys::Win32::Storage::FileSystem::{
        CreateFileW, ReadFile, WriteFile, FILE_GENERIC_READ, FILE_GENERIC_WRITE, FILE_SHARE_READ,
        FILE_SHARE_WRITE, OPEN_EXISTING, PIPE_ACCESS_DUPLEX,
    };
    use windows_sys::Win32::System::Pipes::{
        ConnectNamedPipe, CreateNamedPipeW, PeekNamedPipe, WaitNamedPipeW, PIPE_READMODE_BYTE,
        PIPE_TYPE_BYTE, PIPE_UNLIMITED_INSTANCES, PIPE_WAIT,
    };
    use windows_sys::Win32::System::SystemServices::{
        ACCESS_ALLOWED_ACE_TYPE, SECURITY_DESCRIPTOR_REVISION,
    };
    use windows_sys::Win32::System::Threading::{GetCurrentProcess, OpenProcessToken};

    const PIPE_BUFFER_BYTES: u32 = DEFAULT_MAX_MESSAGE_BYTES as u32;
    const PIPE_ACL_ACCESS_MASK: u32 = FILE_GENERIC_READ | FILE_GENERIC_WRITE;

    // These Win32 error values are deliberately kept local so the error mapping
    // does not depend on optional windows-sys constant exports. They all mean
    // that the peer or this handle can no longer carry a pipe frame.
    const ERROR_HANDLE_EOF_CODE: u32 = 38;
    const ERROR_INVALID_HANDLE_CODE: u32 = 6;
    const ERROR_BROKEN_PIPE_CODE: u32 = 109;
    const ERROR_NO_DATA_CODE: u32 = 232;
    const ERROR_PIPE_NOT_CONNECTED_CODE: u32 = 233;
    const ERROR_OPERATION_ABORTED_CODE: u32 = 995;

    fn wide(value: &str) -> Vec<u16> {
        std::ffi::OsStr::new(value)
            .encode_wide()
            .chain(std::iter::once(0))
            .collect()
    }

    fn check_handle(handle: HANDLE) -> Result<HANDLE, TransportError> {
        if handle == INVALID_HANDLE_VALUE || handle.is_null() {
            Err(map_win32_error(unsafe { GetLastError() }))
        } else {
            Ok(handle)
        }
    }

    fn win32_error() -> TransportError {
        TransportError::Windows(unsafe { GetLastError() })
    }

    fn map_win32_error(error: u32) -> TransportError {
        match error {
            ERROR_HANDLE_EOF_CODE
            | ERROR_INVALID_HANDLE_CODE
            | ERROR_BROKEN_PIPE_CODE
            | ERROR_NO_DATA_CODE
            | ERROR_PIPE_NOT_CONNECTED_CODE
            | ERROR_OPERATION_ABORTED_CODE => TransportError::Closed,
            other => TransportError::Windows(other),
        }
    }

    struct OwnedHandle(HANDLE);

    impl OwnedHandle {
        fn new(handle: HANDLE) -> Result<Self, TransportError> {
            if handle.is_null() || handle == INVALID_HANDLE_VALUE {
                Err(win32_error())
            } else {
                Ok(Self(handle))
            }
        }

        fn get(&self) -> HANDLE {
            self.0
        }

        fn duplicate(&self) -> Result<Self, TransportError> {
            let process = unsafe { GetCurrentProcess() };
            let mut duplicate = null_mut();
            let success = unsafe {
                DuplicateHandle(
                    process,
                    self.get(),
                    process,
                    &mut duplicate,
                    0,
                    FALSE,
                    DUPLICATE_SAME_ACCESS,
                )
            };
            if success == 0 {
                return Err(map_win32_error(unsafe { GetLastError() }));
            }
            Self::new(duplicate)
        }
    }

    // A HANDLE is a process-local kernel reference. This wrapper transfers
    // unique ownership between threads; it is never copied or shared by Rust.
    unsafe impl Send for OwnedHandle {}

    impl Drop for OwnedHandle {
        fn drop(&mut self) {
            if !self.0.is_null() && self.0 != INVALID_HANDLE_VALUE {
                unsafe {
                    CloseHandle(self.0);
                }
            }
        }
    }

    struct PipeSecurity {
        descriptor: SECURITY_DESCRIPTOR,
        dacl: Vec<u8>,
        user_sid: Vec<u8>,
        system_sid: Vec<u8>,
    }

    impl PipeSecurity {
        fn for_current_user() -> Result<Self, TransportError> {
            let user_sid = current_user_sid()?;
            let system_sid = well_known_sid(WinLocalSystemSid)?;
            let user_sid_length = unsafe { GetLengthSid(user_sid.as_ptr() as PSID) } as usize;
            let system_sid_length = unsafe { GetLengthSid(system_sid.as_ptr() as PSID) } as usize;
            if user_sid_length == 0 || system_sid_length == 0 {
                return Err(win32_error());
            }

            let acl_length = mem::size_of::<ACL>()
                .checked_add(
                    (mem::size_of::<ACCESS_ALLOWED_ACE>() - mem::size_of::<u32>())
                        .checked_add(user_sid_length)
                        .ok_or_else(win32_error)?,
                )
                .and_then(|length| {
                    length.checked_add(
                        (mem::size_of::<ACCESS_ALLOWED_ACE>() - mem::size_of::<u32>())
                            .checked_add(system_sid_length)?,
                    )
                })
                .ok_or_else(win32_error)?;
            let acl_length = u32::try_from(acl_length).map_err(|_| win32_error())?;
            let mut dacl = vec![0u8; acl_length as usize];
            let dacl_ptr = dacl.as_mut_ptr() as *mut ACL;
            if unsafe { InitializeAcl(dacl_ptr, acl_length, ACL_REVISION) } == 0 {
                return Err(win32_error());
            }
            if unsafe {
                AddAccessAllowedAceEx(
                    dacl_ptr,
                    ACL_REVISION,
                    0,
                    PIPE_ACL_ACCESS_MASK,
                    user_sid.as_ptr() as PSID,
                )
            } == 0
            {
                return Err(win32_error());
            }
            if unsafe {
                AddAccessAllowedAceEx(
                    dacl_ptr,
                    ACL_REVISION,
                    0,
                    PIPE_ACL_ACCESS_MASK,
                    system_sid.as_ptr() as PSID,
                )
            } == 0
            {
                return Err(win32_error());
            }

            let mut descriptor = unsafe { mem::zeroed::<SECURITY_DESCRIPTOR>() };
            let descriptor_ptr =
                &mut descriptor as *mut SECURITY_DESCRIPTOR as PSECURITY_DESCRIPTOR;
            if unsafe { InitializeSecurityDescriptor(descriptor_ptr, SECURITY_DESCRIPTOR_REVISION) }
                == 0
            {
                return Err(win32_error());
            }
            if unsafe { SetSecurityDescriptorDacl(descriptor_ptr, TRUE, dacl_ptr, FALSE) } == 0 {
                return Err(win32_error());
            }
            if unsafe {
                SetSecurityDescriptorControl(descriptor_ptr, SE_DACL_PROTECTED, SE_DACL_PROTECTED)
            } == 0
            {
                return Err(win32_error());
            }

            Ok(Self {
                descriptor,
                dacl,
                user_sid,
                system_sid,
            })
        }

        fn attributes(&self) -> SECURITY_ATTRIBUTES {
            SECURITY_ATTRIBUTES {
                nLength: mem::size_of::<SECURITY_ATTRIBUTES>() as u32,
                lpSecurityDescriptor: &self.descriptor as *const SECURITY_DESCRIPTOR as *mut _,
                bInheritHandle: FALSE,
            }
        }

        fn verify_kernel_dacl(&self, handle: HANDLE) -> Result<(), TransportError> {
            let mut descriptor: PSECURITY_DESCRIPTOR = null_mut();
            let result = unsafe {
                GetSecurityInfo(
                    handle,
                    SE_KERNEL_OBJECT,
                    windows_sys::Win32::Security::DACL_SECURITY_INFORMATION,
                    null_mut(),
                    null_mut(),
                    null_mut(),
                    null_mut(),
                    &mut descriptor,
                )
            };
            if result != ERROR_SUCCESS {
                if !descriptor.is_null() {
                    unsafe {
                        LocalFree(descriptor as HLOCAL);
                    }
                }
                return Err(TransportError::Windows(result));
            }

            let verification =
                unsafe { verify_descriptor(descriptor, &self.user_sid, &self.system_sid) };
            if !descriptor.is_null() {
                unsafe {
                    LocalFree(descriptor as HLOCAL);
                }
            }
            verification
        }
    }

    impl Drop for PipeSecurity {
        fn drop(&mut self) {
            self.descriptor.Dacl = null_mut();
            self.dacl.fill(0);
        }
    }

    fn current_user_sid() -> Result<Vec<u8>, TransportError> {
        let mut token_handle = null_mut();
        if unsafe { OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &mut token_handle) } == 0 {
            return Err(win32_error());
        }
        let token = OwnedHandle::new(token_handle)?;
        let mut required = 0u32;
        unsafe {
            GetTokenInformation(token.get(), TokenUser, null_mut(), 0, &mut required);
        }
        if required == 0 {
            return Err(win32_error());
        }
        let mut buffer = vec![0u8; required as usize];
        if unsafe {
            GetTokenInformation(
                token.get(),
                TokenUser,
                buffer.as_mut_ptr() as *mut _,
                required,
                &mut required,
            )
        } == 0
        {
            return Err(win32_error());
        }

        let token_user = unsafe { std::ptr::read_unaligned(buffer.as_ptr() as *const TOKEN_USER) };
        let sid = token_user.User.Sid;
        if sid.is_null() {
            return Err(win32_error());
        }
        let sid_length = unsafe { GetLengthSid(sid) } as usize;
        if sid_length == 0 {
            return Err(win32_error());
        }
        Ok(unsafe { slice::from_raw_parts(sid as *const u8, sid_length) }.to_vec())
    }

    fn well_known_sid(sid_type: WELL_KNOWN_SID_TYPE) -> Result<Vec<u8>, TransportError> {
        let mut required = 0u32;
        unsafe {
            CreateWellKnownSid(sid_type, null_mut(), null_mut(), &mut required);
        }
        if required == 0 {
            return Err(win32_error());
        }
        let mut buffer = vec![0u8; required as usize];
        if unsafe {
            CreateWellKnownSid(
                sid_type,
                null_mut(),
                buffer.as_mut_ptr() as PSID,
                &mut required,
            )
        } == 0
        {
            return Err(win32_error());
        }
        buffer.truncate(required as usize);
        Ok(buffer)
    }

    unsafe fn verify_descriptor(
        descriptor: PSECURITY_DESCRIPTOR,
        user_sid: &[u8],
        system_sid: &[u8],
    ) -> Result<(), TransportError> {
        if descriptor.is_null() {
            return Err(TransportError::Windows(0));
        }

        let mut control = 0u16;
        let mut _revision = 0u32;
        if GetSecurityDescriptorControl(descriptor, &mut control, &mut _revision) == 0
            || control & SE_DACL_PROTECTED == 0
        {
            return Err(win32_error());
        }

        let mut present = FALSE;
        let mut dacl: *mut ACL = null_mut();
        let mut defaulted = FALSE;
        if GetSecurityDescriptorDacl(descriptor, &mut present, &mut dacl, &mut defaulted) == 0
            || present == 0
            || dacl.is_null()
            || defaulted != 0
            || (*dacl).AceCount != 2
        {
            return Err(win32_error());
        }

        let mut user_found = false;
        let mut system_found = false;
        for index in 0..(*dacl).AceCount as u32 {
            let mut raw_ace = null_mut();
            if GetAce(dacl, index, &mut raw_ace) == 0 || raw_ace.is_null() {
                return Err(win32_error());
            }
            let ace = &*(raw_ace as *const ACCESS_ALLOWED_ACE);
            if ace.Header.AceType as u32 != ACCESS_ALLOWED_ACE_TYPE
                || ace.Header.AceFlags != 0
                || ace.Mask != PIPE_ACL_ACCESS_MASK
            {
                return Err(win32_error());
            }
            let ace_sid = (&ace.SidStart as *const u32) as PSID;
            if windows_sys::Win32::Security::EqualSid(ace_sid, user_sid.as_ptr() as PSID) != 0 {
                user_found = true;
            }
            if windows_sys::Win32::Security::EqualSid(ace_sid, system_sid.as_ptr() as PSID) != 0 {
                system_found = true;
            }
            if !user_found && !system_found {
                return Err(win32_error());
            }
        }
        if !user_found || !system_found {
            return Err(win32_error());
        }
        Ok(())
    }

    fn create_server_handle(name: &str) -> Result<HANDLE, TransportError> {
        let name = wide(name);
        let security = PipeSecurity::for_current_user()?;
        let attributes = security.attributes();
        let handle = unsafe {
            CreateNamedPipeW(
                name.as_ptr(),
                PIPE_ACCESS_DUPLEX,
                PIPE_TYPE_BYTE | PIPE_READMODE_BYTE | PIPE_WAIT,
                PIPE_UNLIMITED_INSTANCES,
                PIPE_BUFFER_BYTES,
                PIPE_BUFFER_BYTES,
                0,
                &attributes,
            )
        };
        let handle = check_handle(handle)?;
        if let Err(error) = security.verify_kernel_dacl(handle) {
            unsafe {
                CloseHandle(handle);
            }
            return Err(error);
        }
        Ok(handle)
    }

    pub struct NamedPipeListener {
        name: String,
        handle: HANDLE,
    }

    impl NamedPipeListener {
        pub fn bind(name: impl Into<String>) -> Result<Self, TransportError> {
            let name = name.into();
            Ok(Self {
                handle: create_server_handle(&name)?,
                name,
            })
        }

        pub fn accept(&mut self) -> Result<NamedPipeConnection, TransportError> {
            let connected = unsafe { ConnectNamedPipe(self.handle, null_mut()) };
            if connected == 0 {
                let error = unsafe { GetLastError() };
                if error != ERROR_PIPE_CONNECTED {
                    return Err(map_win32_error(error));
                }
            }
            let connection_handle = self.handle;
            let next_handle = match create_server_handle(&self.name) {
                Ok(handle) => handle,
                Err(error) => {
                    unsafe {
                        CloseHandle(connection_handle);
                    }
                    self.handle = null_mut();
                    return Err(error);
                }
            };
            self.handle = next_handle;
            Ok(NamedPipeConnection {
                handle: OwnedHandle::new(connection_handle)?,
                maximum_bytes: DEFAULT_MAX_MESSAGE_BYTES,
            })
        }
    }

    impl Drop for NamedPipeListener {
        fn drop(&mut self) {
            if !self.handle.is_null() && self.handle != INVALID_HANDLE_VALUE {
                unsafe {
                    CloseHandle(self.handle);
                }
            }
        }
    }

    pub struct NamedPipeClient;

    impl NamedPipeClient {
        pub fn connect(name: &str) -> Result<NamedPipeConnection, TransportError> {
            let name = wide(name);
            let ready = unsafe { WaitNamedPipeW(name.as_ptr(), 5000) };
            if ready == 0 {
                return Err(map_win32_error(unsafe { GetLastError() }));
            }
            let handle = unsafe {
                CreateFileW(
                    name.as_ptr(),
                    FILE_GENERIC_READ | FILE_GENERIC_WRITE,
                    FILE_SHARE_READ | FILE_SHARE_WRITE,
                    null_mut(),
                    OPEN_EXISTING,
                    0,
                    0 as HANDLE,
                )
            };
            Ok(NamedPipeConnection {
                handle: OwnedHandle::new(check_handle(handle)?)?,
                maximum_bytes: DEFAULT_MAX_MESSAGE_BYTES,
            })
        }
    }

    pub struct NamedPipeConnection {
        handle: OwnedHandle,
        maximum_bytes: usize,
    }

    impl NamedPipeConnection {
        /// Duplicate the underlying pipe handle into a new independently owned
        /// connection. Each value closes only its own kernel handle.
        pub fn try_clone_handle(&self) -> Result<Self, TransportError> {
            Ok(Self {
                handle: self.handle.duplicate()?,
                maximum_bytes: self.maximum_bytes,
            })
        }

        /// Alias for [`Self::try_clone_handle`] for callers that prefer an
        /// operation-named API over the standard `try_clone` spelling.
        pub fn clone_handle(&self) -> Result<Self, TransportError> {
            self.try_clone_handle()
        }

        /// Standard clone spelling for a fallible OS-handle duplication.
        pub fn try_clone(&self) -> Result<Self, TransportError> {
            self.try_clone_handle()
        }

        /// Split a duplex pipe into independently owned read and write halves.
        ///
        /// The original handle is moved into the reader. The writer receives a
        /// `DuplicateHandle` copy, so dropping either half never closes the
        /// other half's HANDLE and no raw HANDLE is exposed to callers.
        pub fn into_split(self) -> Result<(NamedPipeReader, NamedPipeWriter), TransportError> {
            let Self {
                handle,
                maximum_bytes,
            } = self;
            let writer_handle = handle.duplicate()?;
            Ok((
                NamedPipeReader {
                    handle,
                    maximum_bytes,
                },
                NamedPipeWriter {
                    handle: writer_handle,
                    maximum_bytes,
                },
            ))
        }

        pub fn try_read_json(&mut self) -> Result<Option<Vec<u8>>, TransportError> {
            try_read_json_from_handle(self.handle.get(), self.maximum_bytes)
        }
    }

    fn read_exact_from_handle(handle: HANDLE, bytes: &mut [u8]) -> Result<(), TransportError> {
        let mut offset = 0;
        while offset < bytes.len() {
            let mut read = 0u32;
            let requested = u32::try_from(bytes.len() - offset).map_err(|_| {
                TransportError::Frame(FrameError::TooLarge {
                    actual: bytes.len() - offset,
                    maximum: u32::MAX as usize,
                })
            })?;
            let success = unsafe {
                ReadFile(
                    handle,
                    bytes[offset..].as_mut_ptr(),
                    requested,
                    &mut read,
                    null_mut(),
                )
            };
            if success == 0 {
                return Err(map_win32_error(unsafe { GetLastError() }));
            }
            if read == 0 {
                return Err(TransportError::Closed);
            }
            offset += read as usize;
        }
        Ok(())
    }

    fn write_all_to_handle(handle: HANDLE, bytes: &[u8]) -> Result<(), TransportError> {
        let mut offset = 0;
        while offset < bytes.len() {
            let mut written = 0u32;
            let requested = u32::try_from(bytes.len() - offset).map_err(|_| {
                TransportError::Frame(FrameError::TooLarge {
                    actual: bytes.len() - offset,
                    maximum: u32::MAX as usize,
                })
            })?;
            let success = unsafe {
                WriteFile(
                    handle,
                    bytes[offset..].as_ptr(),
                    requested,
                    &mut written,
                    null_mut(),
                )
            };
            if success == 0 {
                return Err(map_win32_error(unsafe { GetLastError() }));
            }
            if written == 0 {
                return Err(TransportError::Closed);
            }
            offset += written as usize;
        }
        Ok(())
    }

    fn read_json_from_handle(
        handle: HANDLE,
        maximum_bytes: usize,
    ) -> Result<Vec<u8>, TransportError> {
        let mut prefix = [0u8; 4];
        read_exact_from_handle(handle, &mut prefix)?;
        let declared = u32::from_be_bytes(prefix) as usize;
        if declared > maximum_bytes {
            return Err(TransportError::Frame(FrameError::TooLarge {
                actual: declared,
                maximum: maximum_bytes,
            }));
        }
        let mut payload = vec![0u8; declared];
        read_exact_from_handle(handle, &mut payload)?;
        let mut frame = Vec::with_capacity(4 + declared);
        frame.extend_from_slice(&prefix);
        frame.extend_from_slice(&payload);
        Ok(decode_frame(&frame, maximum_bytes)?)
    }

    fn try_read_json_from_handle(
        handle: HANDLE,
        maximum_bytes: usize,
    ) -> Result<Option<Vec<u8>>, TransportError> {
        let mut available = 0u32;
        let success = unsafe {
            PeekNamedPipe(
                handle,
                null_mut(),
                0,
                null_mut(),
                &mut available,
                null_mut(),
            )
        };
        if success == 0 {
            return Err(map_win32_error(unsafe { GetLastError() }));
        }
        if available < 4 {
            return Ok(None);
        }
        let mut prefix = [0u8; 4];
        let mut peeked = 0u32;
        let success = unsafe {
            PeekNamedPipe(
                handle,
                prefix.as_mut_ptr() as *mut _,
                prefix.len() as u32,
                &mut peeked,
                null_mut(),
                null_mut(),
            )
        };
        if success == 0 {
            return Err(map_win32_error(unsafe { GetLastError() }));
        }
        if peeked < 4 {
            return Ok(None);
        }
        let declared = u32::from_be_bytes(prefix) as usize;
        if declared > maximum_bytes {
            return Err(TransportError::Frame(FrameError::TooLarge {
                actual: declared,
                maximum: maximum_bytes,
            }));
        }
        let frame_bytes = 4usize.checked_add(declared).ok_or({
            TransportError::Frame(FrameError::TooLarge {
                actual: declared,
                maximum: maximum_bytes,
            })
        })?;
        if u64::from(available) < frame_bytes as u64 {
            return Ok(None);
        }
        read_json_from_handle(handle, maximum_bytes).map(Some)
    }

    fn write_json_to_handle<T: serde::Serialize>(
        handle: HANDLE,
        maximum_bytes: usize,
        value: &T,
    ) -> Result<(), TransportError> {
        write_all_to_handle(handle, &encode_frame(value, maximum_bytes)?)
    }

    pub struct NamedPipeReader {
        handle: OwnedHandle,
        maximum_bytes: usize,
    }

    impl NamedPipeReader {
        pub fn try_clone_handle(&self) -> Result<Self, TransportError> {
            Ok(Self {
                handle: self.handle.duplicate()?,
                maximum_bytes: self.maximum_bytes,
            })
        }

        pub fn read_json(&mut self) -> Result<Vec<u8>, TransportError> {
            read_json_from_handle(self.handle.get(), self.maximum_bytes)
        }

        pub fn try_read_json(&mut self) -> Result<Option<Vec<u8>>, TransportError> {
            try_read_json_from_handle(self.handle.get(), self.maximum_bytes)
        }
    }

    impl FramedReader for NamedPipeReader {
        fn read_json(&mut self) -> Result<Vec<u8>, TransportError> {
            NamedPipeReader::read_json(self)
        }

        fn try_read_json(&mut self) -> Result<Option<Vec<u8>>, TransportError> {
            NamedPipeReader::try_read_json(self)
        }
    }

    pub struct NamedPipeWriter {
        handle: OwnedHandle,
        maximum_bytes: usize,
    }

    impl NamedPipeWriter {
        pub fn try_clone_handle(&self) -> Result<Self, TransportError> {
            Ok(Self {
                handle: self.handle.duplicate()?,
                maximum_bytes: self.maximum_bytes,
            })
        }

        pub fn write_json<T: serde::Serialize>(&mut self, value: &T) -> Result<(), TransportError> {
            write_json_to_handle(self.handle.get(), self.maximum_bytes, value)
        }
    }

    impl FramedWriter for NamedPipeWriter {
        fn write_json<T: serde::Serialize>(&mut self, value: &T) -> Result<(), TransportError> {
            NamedPipeWriter::write_json(self, value)
        }
    }

    impl FramedTransport for NamedPipeConnection {
        fn write_json<T: serde::Serialize>(&mut self, value: &T) -> Result<(), TransportError> {
            write_json_to_handle(self.handle.get(), self.maximum_bytes, value)
        }

        fn read_json(&mut self) -> Result<Vec<u8>, TransportError> {
            read_json_from_handle(self.handle.get(), self.maximum_bytes)
        }
    }

    pub use {
        NamedPipeClient as Client, NamedPipeConnection as Connection, NamedPipeListener as Listener,
    };

    pub type ReadHalf = NamedPipeReader;
    pub type WriteHalf = NamedPipeWriter;

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn named_pipe_kernel_acl_contains_only_current_user_and_system() {
            let name = format!("\\\\.\\pipe\\agenttalk-ipc-acl-{}", std::process::id());
            let listener = NamedPipeListener::bind(name).unwrap();
            let expected = PipeSecurity::for_current_user().unwrap();
            expected.verify_kernel_dacl(listener.handle).unwrap();
        }
    }
}

#[cfg(windows)]
pub use windows_named_pipe::{
    Client as NamedPipeClient, Connection as NamedPipeConnection, Listener as NamedPipeListener,
    NamedPipeReader, NamedPipeWriter, ReadHalf as NamedPipeReadHalf,
    WriteHalf as NamedPipeWriteHalf,
};

#[cfg(windows)]
#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn named_pipe_transport_round_trips_a_framed_payload() {
        let name = format!("\\\\.\\pipe\\agenttalk-ipc-test-{}", std::process::id());
        let server_name = name.clone();
        let server = thread::spawn(move || {
            let mut listener = NamedPipeListener::bind(server_name).unwrap();
            let mut connection = listener.accept().unwrap();
            let request = connection.read_json().unwrap();
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(&request).unwrap()["kind"],
                "ping"
            );
            connection.write_json(&json!({"kind":"pong"})).unwrap();
        });
        thread::sleep(Duration::from_millis(20));
        let mut client = NamedPipeClient::connect(&name).unwrap();
        client.write_json(&json!({"kind":"ping"})).unwrap();
        let response = client.read_json().unwrap();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&response).unwrap()["kind"],
            "pong"
        );
        server.join().unwrap();
    }

    #[test]
    fn named_pipe_split_supports_concurrent_read_and_write() {
        let name = format!("\\\\.\\pipe\\agenttalk-ipc-split-{}", std::process::id());
        let server_name = name.clone();
        let server = thread::spawn(move || {
            let mut listener = NamedPipeListener::bind(server_name).unwrap();
            let connection = listener.accept().unwrap();
            let (mut reader, mut writer) = connection.into_split().unwrap();

            thread::scope(|scope| {
                let read = scope.spawn(move || reader.read_json());
                let write = scope.spawn(move || writer.write_json(&json!({"kind": "pong"})));

                let request = read.join().unwrap().unwrap();
                assert_eq!(
                    serde_json::from_slice::<serde_json::Value>(&request).unwrap()["kind"],
                    "ping"
                );
                write.join().unwrap().unwrap();
            });
        });

        thread::sleep(Duration::from_millis(20));
        let connection = NamedPipeClient::connect(&name).unwrap();
        let (mut reader, mut writer) = connection.into_split().unwrap();
        writer.write_json(&json!({"kind": "ping"})).unwrap();
        let response = reader.read_json().unwrap();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&response).unwrap()["kind"],
            "pong"
        );
        server.join().unwrap();
    }

    #[test]
    fn named_pipe_handle_clone_outlives_original_without_double_close() {
        let name = format!("\\\\.\\pipe\\agenttalk-ipc-clone-{}", std::process::id());
        let server_name = name.clone();
        let server = thread::spawn(move || {
            let mut listener = NamedPipeListener::bind(server_name).unwrap();
            let mut connection = listener.accept().unwrap();
            let request = connection.read_json().unwrap();
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(&request).unwrap()["kind"],
                "clone"
            );
        });

        thread::sleep(Duration::from_millis(20));
        let connection = NamedPipeClient::connect(&name).unwrap();
        let mut clone = connection.try_clone_handle().unwrap();
        drop(connection);
        clone.write_json(&json!({"kind": "clone"})).unwrap();
        server.join().unwrap();
    }

    #[test]
    fn named_pipe_split_peer_close_is_reported_as_closed() {
        let name = format!("\\\\.\\pipe\\agenttalk-ipc-close-{}", std::process::id());
        let server_name = name.clone();
        let server = thread::spawn(move || {
            let mut listener = NamedPipeListener::bind(server_name).unwrap();
            let connection = listener.accept().unwrap();
            let (mut reader, _writer) = connection.into_split().unwrap();
            reader.read_json()
        });

        thread::sleep(Duration::from_millis(20));
        let connection = NamedPipeClient::connect(&name).unwrap();
        let (reader, writer) = connection.into_split().unwrap();
        drop(reader);
        drop(writer);

        assert!(matches!(
            server.join().unwrap(),
            Err(TransportError::Closed)
        ));
    }
}

#[cfg(not(windows))]
pub struct NamedPipeListener;

#[cfg(not(windows))]
impl NamedPipeListener {
    pub fn bind(_: impl Into<String>) -> Result<Self, TransportError> {
        Err(TransportError::Unsupported)
    }

    pub fn accept(&mut self) -> Result<NamedPipeConnection, TransportError> {
        Err(TransportError::Unsupported)
    }
}

#[cfg(not(windows))]
pub struct NamedPipeClient;

#[cfg(not(windows))]
impl NamedPipeClient {
    pub fn connect(_: &str) -> Result<NamedPipeConnection, TransportError> {
        Err(TransportError::Unsupported)
    }
}

#[cfg(not(windows))]
pub struct NamedPipeConnection;

#[cfg(not(windows))]
impl NamedPipeConnection {
    pub fn try_clone_handle(&self) -> Result<Self, TransportError> {
        Err(TransportError::Unsupported)
    }

    pub fn clone_handle(&self) -> Result<Self, TransportError> {
        Err(TransportError::Unsupported)
    }

    pub fn try_clone(&self) -> Result<Self, TransportError> {
        Err(TransportError::Unsupported)
    }

    pub fn into_split(self) -> Result<(NamedPipeReader, NamedPipeWriter), TransportError> {
        Err(TransportError::Unsupported)
    }

    pub fn try_read_json(&mut self) -> Result<Option<Vec<u8>>, TransportError> {
        Err(TransportError::Unsupported)
    }
}

#[cfg(not(windows))]
impl FramedTransport for NamedPipeConnection {
    fn write_json<T: serde::Serialize>(&mut self, _: &T) -> Result<(), TransportError> {
        Err(TransportError::Unsupported)
    }

    fn read_json(&mut self) -> Result<Vec<u8>, TransportError> {
        Err(TransportError::Unsupported)
    }
}

#[cfg(not(windows))]
pub struct NamedPipeReader;

#[cfg(not(windows))]
impl NamedPipeReader {
    pub fn try_clone_handle(&self) -> Result<Self, TransportError> {
        Err(TransportError::Unsupported)
    }

    pub fn read_json(&mut self) -> Result<Vec<u8>, TransportError> {
        Err(TransportError::Unsupported)
    }

    pub fn try_read_json(&mut self) -> Result<Option<Vec<u8>>, TransportError> {
        Err(TransportError::Unsupported)
    }
}

#[cfg(not(windows))]
impl FramedReader for NamedPipeReader {
    fn read_json(&mut self) -> Result<Vec<u8>, TransportError> {
        NamedPipeReader::read_json(self)
    }

    fn try_read_json(&mut self) -> Result<Option<Vec<u8>>, TransportError> {
        NamedPipeReader::try_read_json(self)
    }
}

#[cfg(not(windows))]
pub struct NamedPipeWriter;

#[cfg(not(windows))]
impl NamedPipeWriter {
    pub fn try_clone_handle(&self) -> Result<Self, TransportError> {
        Err(TransportError::Unsupported)
    }

    pub fn write_json<T: serde::Serialize>(&mut self, _: &T) -> Result<(), TransportError> {
        Err(TransportError::Unsupported)
    }
}

#[cfg(not(windows))]
impl FramedWriter for NamedPipeWriter {
    fn write_json<T: serde::Serialize>(&mut self, value: &T) -> Result<(), TransportError> {
        NamedPipeWriter::write_json(self, value)
    }
}

#[cfg(not(windows))]
pub type NamedPipeReadHalf = NamedPipeReader;

#[cfg(not(windows))]
pub type NamedPipeWriteHalf = NamedPipeWriter;
