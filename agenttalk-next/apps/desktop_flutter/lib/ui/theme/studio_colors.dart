import 'package:flutter/material.dart';

/// Design tokens for the AgentTalk multi-agent studio dark UI.
///
/// These constants come from the visual blueprint: bgRoot #0D0F12,
/// bgSurface #13161C, bgCard #191D24, primary #2563EB, plus node accent and
/// status colors. They are colors only; no component may read IPC data from
/// here and no hardcoded fake UI data belongs in this file.
abstract final class StudioColors {
  // Background layers (dark hierarchy)
  static const Color bgRoot = Color(0xFF0D0F14); // L0 lowest black
  static const Color bgSurface = Color(0xFF131720); // L1 panel base
  static const Color bgCard = Color(0xFF1B202D); // L2 floating/card base
  static const Color bgHover = Color(0xFF262E40); // L3 hover/focus

  // Borders and dividers
  static const Color borderSubtle = Color(0xFF242A3A);
  static const Color borderStrong = Color(0xFF2E3648);

  // Brand and status colors
  static const Color primary = Color(0xFF2563EB);
  static const Color primaryHover = Color(0xFF3B82F6);
  static const Color success = Color(0xFF10B981);
  static const Color warning = Color(0xFFF59E0B);
  static const Color danger = Color(0xFFEF4444);
  static const Color inactive = Color(0xFF6B7280);

  // Node accent colors
  static const Color nodeStart = Color(0xFF059669);
  static const Color nodeEnd = Color(0xFF10B981);
  static const Color nodeCollector = Color(0xFF3B82F6);
  static const Color nodeAnalyzer = Color(0xFF8B5CF6);
  static const Color nodeReport = Color(0xFFF59E0B);

  // Text hierarchy
  static const Color textPrimary = Color(0xFFF3F4F6);
  static const Color textSecondary = Color(0xFF9CA3AF);
  static const Color textTertiary = Color(0xFF8A93A5);

  static const Color transparent = Color(0x00000000);
}

/// Studio dark scheme: every Material surface maps onto the blueprint's dark
/// layer hierarchy, with the studio primary blue as the seed.
final ColorScheme studioDarkColorScheme =
    ColorScheme.fromSeed(
      seedColor: StudioColors.primary,
      brightness: Brightness.dark,
    ).copyWith(
      primary: StudioColors.primary,
      onPrimary: Colors.white,
      primaryContainer: const Color(0xFF1E3A8A),
      onPrimaryContainer: const Color(0xFFDBEAFE),
      secondary: const Color(0xFF94A3B8),
      onSecondary: StudioColors.bgRoot,
      secondaryContainer: const Color(0xFF1E293B),
      onSecondaryContainer: StudioColors.textPrimary,
      tertiary: StudioColors.primaryHover,
      onTertiary: Colors.white,
      tertiaryContainer: const Color(0xFF1E3A5F),
      onTertiaryContainer: const Color(0xFFDBEAFE),
      error: StudioColors.danger,
      onError: Colors.white,
      errorContainer: const Color(0xFF7F1D1D),
      onErrorContainer: const Color(0xFFFEE2E2),
      surface: StudioColors.bgSurface,
      onSurface: StudioColors.textPrimary,
      onSurfaceVariant: StudioColors.textSecondary,
      surfaceContainerLowest: StudioColors.bgRoot,
      surfaceContainerLow: StudioColors.bgSurface,
      surfaceContainer: StudioColors.bgCard,
      surfaceContainerHigh: StudioColors.bgHover,
      surfaceContainerHighest: StudioColors.borderStrong,
      outline: StudioColors.borderStrong,
      outlineVariant: StudioColors.borderSubtle,
      shadow: Colors.black,
      scrim: Colors.black,
    );

/// Light counterpart used by the theme toggle. It shares the studio primary
/// but stays readable on light backgrounds.
final ColorScheme studioLightColorScheme =
    ColorScheme.fromSeed(
      seedColor: StudioColors.primary,
      brightness: Brightness.light,
    ).copyWith(
      primary: StudioColors.primary,
      onPrimary: Colors.white,
      primaryContainer: const Color(0xFFDBEAFE),
      onPrimaryContainer: const Color(0xFF1E3A8A),
      secondary: const Color(0xFF475569),
      onSecondary: Colors.white,
      secondaryContainer: const Color(0xFFE2E8F0),
      onSecondaryContainer: const Color(0xFF1E293B),
      tertiary: const Color(0xFF7C3AED),
      onTertiary: Colors.white,
      tertiaryContainer: const Color(0xFFEDE9FE),
      onTertiaryContainer: const Color(0xFF4C1D95),
      error: const Color(0xFFDC2626),
      onError: Colors.white,
      errorContainer: const Color(0xFFFEE2E2),
      onErrorContainer: const Color(0xFF7F1D1D),
      surface: const Color(0xFFF8FAFC),
      onSurface: const Color(0xFF0F172A),
      onSurfaceVariant: const Color(0xFF475569),
      surfaceContainerLowest: const Color(0xFFFBFCFE),
      surfaceContainerLow: const Color(0xFFF1F5F9),
      surfaceContainer: const Color(0xFFE2E8F0),
      surfaceContainerHigh: const Color(0xFFCBD5E1),
      surfaceContainerHighest: const Color(0xFF94A3B8),
      outline: const Color(0xFF64748B),
      outlineVariant: const Color(0xFFCBD5E1),
      shadow: Colors.black,
      scrim: Colors.black,
    );

/// Shared component theme for the studio shell.
ThemeData buildStudioTheme(ColorScheme scheme) {
  final baseTextTheme = scheme.brightness == Brightness.dark
      ? ThemeData.dark().textTheme
      : ThemeData.light().textTheme;
  const double bodyHeight = 1.4;
  const double bodyLetterSpacing = 0.2;
  final textTheme = baseTextTheme
      .apply(
        bodyColor: scheme.onSurface,
        displayColor: scheme.onSurface,
        fontFamily: 'Segoe UI',
      )
      .copyWith(
        headlineSmall: baseTextTheme.headlineSmall?.copyWith(
          fontSize: 18,
          fontWeight: FontWeight.w700,
          color: scheme.onSurface,
          letterSpacing: bodyLetterSpacing,
          height: bodyHeight,
        ),
        titleLarge: baseTextTheme.titleLarge?.copyWith(
          fontSize: 16,
          fontWeight: FontWeight.w700,
          color: scheme.onSurface,
          letterSpacing: bodyLetterSpacing,
          height: bodyHeight,
        ),
        titleMedium: baseTextTheme.titleMedium?.copyWith(
          fontSize: 13,
          fontWeight: FontWeight.w600,
          color: scheme.onSurface,
          letterSpacing: bodyLetterSpacing,
          height: bodyHeight,
        ),
        titleSmall: baseTextTheme.titleSmall?.copyWith(
          fontSize: 12,
          fontWeight: FontWeight.w600,
          color: scheme.onSurface,
          letterSpacing: bodyLetterSpacing,
          height: bodyHeight,
        ),
        bodyLarge: baseTextTheme.bodyLarge?.copyWith(
          fontSize: 13,
          color: scheme.onSurface,
          letterSpacing: bodyLetterSpacing,
          height: bodyHeight,
        ),
        bodyMedium: baseTextTheme.bodyMedium?.copyWith(
          fontSize: 12,
          color: scheme.onSurface,
          letterSpacing: bodyLetterSpacing,
          height: bodyHeight,
        ),
        bodySmall: baseTextTheme.bodySmall?.copyWith(
          fontSize: 11,
          color: scheme.onSurfaceVariant,
          letterSpacing: bodyLetterSpacing,
          height: bodyHeight,
        ),
        labelLarge: baseTextTheme.labelLarge?.copyWith(
          fontSize: 12,
          fontWeight: FontWeight.w600,
          color: scheme.onSurface,
          letterSpacing: bodyLetterSpacing,
          height: bodyHeight,
        ),
        labelMedium: baseTextTheme.labelMedium?.copyWith(
          fontSize: 11,
          color: scheme.onSurfaceVariant,
          letterSpacing: bodyLetterSpacing,
          height: bodyHeight,
        ),
        labelSmall: baseTextTheme.labelSmall?.copyWith(
          fontSize: 10,
          color: scheme.onSurfaceVariant,
          letterSpacing: bodyLetterSpacing,
          height: bodyHeight,
        ),
      );
  return ThemeData(
    useMaterial3: true,
    colorScheme: scheme,
    textTheme: textTheme,
    fontFamily: 'Segoe UI',
    fontFamilyFallback: const [
      'Microsoft YaHei UI',
      'PingFang SC',
      'Noto Sans SC',
      'sans-serif',
    ],
    scaffoldBackgroundColor: scheme.brightness == Brightness.dark
        ? StudioColors.bgRoot
        : scheme.surface,
    cardTheme: CardThemeData(
      elevation: 0,
      color: scheme.surfaceContainerLow,
      shape: RoundedRectangleBorder(
        borderRadius: BorderRadius.circular(12),
        side: BorderSide(color: scheme.outlineVariant),
      ),
    ),
    dialogTheme: DialogThemeData(
      elevation: 6,
      backgroundColor: scheme.surface,
      shape: RoundedRectangleBorder(
        borderRadius: BorderRadius.circular(14),
        side: BorderSide(color: scheme.outlineVariant),
      ),
    ),
    inputDecorationTheme: InputDecorationTheme(
      filled: true,
      fillColor: scheme.surfaceContainerLowest,
      border: OutlineInputBorder(borderRadius: BorderRadius.circular(8)),
      enabledBorder: OutlineInputBorder(
        borderRadius: BorderRadius.circular(8),
        borderSide: BorderSide(color: scheme.outlineVariant),
      ),
      focusedBorder: OutlineInputBorder(
        borderRadius: BorderRadius.circular(8),
        borderSide: BorderSide(color: scheme.primary),
      ),
      disabledBorder: OutlineInputBorder(
        borderRadius: BorderRadius.circular(8),
        borderSide: BorderSide(color: scheme.outlineVariant),
      ),
    ),
  );
}
