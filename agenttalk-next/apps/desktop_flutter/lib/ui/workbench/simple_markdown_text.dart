import 'package:flutter/material.dart';

import '../theme/studio_colors.dart';
import 'studio_event_log.dart';

/// Minimal dark-flavored Markdown renderer used by the chat stream.
///
/// It supports fenced code blocks, inline code, and `**bold**`. It is a
/// renderer only; all text is passed through [studioSafeText] first so no
/// path/PID/port/credential reaches the widget tree.
class SimpleMarkdownText extends StatelessWidget {
  const SimpleMarkdownText({
    super.key,
    required this.text,
    this.baseStyle,
    this.codeBackground,
  });

  final String text;
  final TextStyle? baseStyle;
  final Color? codeBackground;

  @override
  Widget build(BuildContext context) {
    final safe = studioSafeText(text);
    final blocks = _parseBlocks(safe);
    return Column(
      crossAxisAlignment: CrossAxisAlignment.start,
      mainAxisSize: MainAxisSize.min,
      children: [for (final block in blocks) _buildBlock(context, block)],
    );
  }

  Widget _buildBlock(BuildContext context, _MarkdownBlock block) {
    final base =
        baseStyle ??
        const TextStyle(
          color: StudioColors.textPrimary,
          fontSize: 11,
          height: 1.4,
        );
    switch (block.kind) {
      case _MarkdownBlockKind.code:
        return Container(
          width: double.infinity,
          margin: const EdgeInsets.symmetric(vertical: 4),
          padding: const EdgeInsets.all(8),
          decoration: BoxDecoration(
            color: codeBackground ?? StudioColors.bgRoot,
            borderRadius: BorderRadius.circular(6),
            border: Border.all(color: StudioColors.borderSubtle),
          ),
          child: Text(
            block.text,
            style: const TextStyle(
              color: StudioColors.textSecondary,
              fontSize: 10,
              fontFamily: 'monospace',
              height: 1.4,
            ),
          ),
        );
      case _MarkdownBlockKind.paragraph:
        return Padding(
          padding: const EdgeInsets.symmetric(vertical: 2),
          child: Text.rich(
            TextSpan(children: _inlineSpans(block.text, base)),
            style: base,
          ),
        );
    }
  }

  List<TextSpan> _inlineSpans(String text, TextStyle base) {
    final spans = <TextSpan>[];
    final boldPattern = RegExp(r'\*\*(.+?)\*\*|`([^`]+)`');
    var cursor = 0;
    for (final match in boldPattern.allMatches(text)) {
      if (match.start > cursor) {
        spans.add(TextSpan(text: text.substring(cursor, match.start)));
      }
      if (match.group(1) != null) {
        spans.add(
          TextSpan(
            text: match.group(1),
            style: base.copyWith(fontWeight: FontWeight.w700),
          ),
        );
      } else if (match.group(2) != null) {
        spans.add(
          TextSpan(
            text: match.group(2),
            style: base.copyWith(
              fontFamily: 'monospace',
              color: StudioColors.primaryHover,
            ),
          ),
        );
      }
      cursor = match.end;
    }
    if (cursor < text.length) {
      spans.add(TextSpan(text: text.substring(cursor)));
    }
    return spans;
  }
}

enum _MarkdownBlockKind { paragraph, code }

class _MarkdownBlock {
  const _MarkdownBlock(this.kind, this.text);

  final _MarkdownBlockKind kind;
  final String text;
}

List<_MarkdownBlock> _parseBlocks(String text) {
  final blocks = <_MarkdownBlock>[];
  final lines = text.split('\n');
  var inCode = false;
  var codeBuffer = StringBuffer();
  var paragraph = StringBuffer();

  void flushParagraph() {
    if (paragraph.isNotEmpty) {
      blocks.add(
        _MarkdownBlock(_MarkdownBlockKind.paragraph, paragraph.toString()),
      );
      paragraph.clear();
    }
  }

  for (final line in lines) {
    if (line.trimLeft().startsWith('```')) {
      if (inCode) {
        blocks.add(
          _MarkdownBlock(_MarkdownBlockKind.code, codeBuffer.toString()),
        );
        codeBuffer = StringBuffer();
        inCode = false;
      } else {
        flushParagraph();
        inCode = true;
      }
      continue;
    }
    if (inCode) {
      codeBuffer.writeln(line);
      continue;
    }
    if (line.trim().isEmpty) {
      flushParagraph();
      continue;
    }
    if (paragraph.isNotEmpty) paragraph.write('\n');
    paragraph.write(line);
  }
  if (inCode && codeBuffer.isNotEmpty) {
    blocks.add(_MarkdownBlock(_MarkdownBlockKind.code, codeBuffer.toString()));
  }
  flushParagraph();
  return blocks;
}
