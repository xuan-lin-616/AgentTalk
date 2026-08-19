import 'package:agenttalk_desktop/ui/theme/studio_colors.dart';
import 'package:flutter/material.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  test('studio design tokens match the blueprint palette', () {
    expect(StudioColors.bgRoot, const Color(0xFF0D0F14));
    expect(StudioColors.bgSurface, const Color(0xFF131720));
    expect(StudioColors.bgCard, const Color(0xFF1B202D));
    expect(StudioColors.primary, const Color(0xFF2563EB));
    expect(StudioColors.success, const Color(0xFF10B981));
    expect(StudioColors.warning, const Color(0xFFF59E0B));
    expect(StudioColors.danger, const Color(0xFFEF4444));
    expect(StudioColors.nodeAnalyzer, const Color(0xFF8B5CF6));
    expect(StudioColors.textPrimary, const Color(0xFFF3F4F6));
    expect(StudioColors.textSecondary, const Color(0xFF9CA3AF));
    expect(StudioColors.textTertiary, const Color(0xFF8A93A5));
  });

  test('dark scheme maps Material surfaces onto the dark layers', () {
    expect(studioDarkColorScheme.brightness, Brightness.dark);
    expect(studioDarkColorScheme.primary, StudioColors.primary);
    expect(studioDarkColorScheme.surface, StudioColors.bgSurface);
    expect(studioDarkColorScheme.surfaceContainerLowest, StudioColors.bgRoot);
    expect(studioDarkColorScheme.surfaceContainerLow, StudioColors.bgSurface);
    expect(studioDarkColorScheme.surfaceContainer, StudioColors.bgCard);
    expect(studioDarkColorScheme.surfaceContainerHigh, StudioColors.bgHover);
    expect(
      studioDarkColorScheme.surfaceContainerHighest,
      StudioColors.borderStrong,
    );
    expect(studioDarkColorScheme.outline, StudioColors.borderStrong);
    expect(studioDarkColorScheme.outlineVariant, StudioColors.borderSubtle);
    expect(studioDarkColorScheme.onSurface, StudioColors.textPrimary);
    expect(studioDarkColorScheme.onSurfaceVariant, StudioColors.textSecondary);
    expect(studioDarkColorScheme.error, StudioColors.danger);
  });

  test('studio theme uses the root black as the dark scaffold', () {
    final theme = buildStudioTheme(studioDarkColorScheme);
    expect(theme.colorScheme.brightness, Brightness.dark);
    expect(theme.scaffoldBackgroundColor, StudioColors.bgRoot);
    expect(theme.useMaterial3, isTrue);
  });
}
