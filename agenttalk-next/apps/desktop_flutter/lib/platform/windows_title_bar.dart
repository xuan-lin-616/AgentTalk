import 'dart:ffi';
import 'dart:io';

import 'package:ffi/ffi.dart';

/// Best-effort native title-bar darkening on Windows.
///
/// The Flutter app cannot self-draw the native window caption without
/// modifying the Windows runner (which is out of scope for the UI work). This
/// instead asks DWM to render the active window's caption with the immersive
/// dark attribute (Windows 10 20H1+) and the dark caption color attribute
/// (Windows 11). Every call is best-effort and failure is ignored; on
/// non-Windows this is a no-op.
Future<void> applyDarkWindowsTitleBar() async {
  if (!Platform.isWindows) return;
  try {
    final user32 = DynamicLibrary.open('user32.dll');
    final dwmapi = DynamicLibrary.open('dwmapi.dll');
    final getActiveWindow = user32
        .lookupFunction<IntPtr Function(), int Function()>('GetActiveWindow');
    final dwmSetWindowAttribute = dwmapi
        .lookupFunction<
          Int32 Function(IntPtr, Uint32, Pointer<Void>, Uint32),
          int Function(int, int, Pointer<Void>, int)
        >('DwmSetWindowAttribute');

    final hwnd = getActiveWindow();
    if (hwnd == 0) return;

    // DWMWA_USE_IMMERSIVE_DARK_MODE = 20.
    final immersiveValue = calloc<Uint32>(1)..value = 1;
    dwmSetWindowAttribute(
      hwnd,
      20,
      immersiveValue.cast<Void>(),
      sizeOf<Uint32>(),
    );
    calloc.free(immersiveValue);

    // DWMWA_CAPTION_COLOR = 35 (Windows 11). COLORREF is 0x00BBGGRR;
    // #0D0F12 -> R 0x0D, G 0x0F, B 0x12.
    final captionColor = calloc<Uint32>(1)..value = 0x00120F0D;
    dwmSetWindowAttribute(
      hwnd,
      35,
      captionColor.cast<Void>(),
      sizeOf<Uint32>(),
    );
    calloc.free(captionColor);
  } on Object {
    // Missing DWM API or non-eligible window: keep the default caption.
  }
}
