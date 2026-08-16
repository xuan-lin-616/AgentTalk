import 'package:flutter/material.dart';

import '../gen/l10n.dart';

typedef MessageSearch =
    Future<List<Map<String, dynamic>>> Function(String query);

class MessageSearchDialog extends StatefulWidget {
  const MessageSearchDialog({super.key, required this.search});

  final MessageSearch search;

  @override
  State<MessageSearchDialog> createState() => _MessageSearchDialogState();
}

class _MessageSearchDialogState extends State<MessageSearchDialog> {
  final TextEditingController _query = TextEditingController();
  List<Map<String, dynamic>> _results = const <Map<String, dynamic>>[];
  String? _error;
  bool _loading = false;

  @override
  void dispose() {
    _query.dispose();
    super.dispose();
  }

  Future<void> _submit() async {
    final query = _query.text.trim();
    if (query.isEmpty || _loading) return;
    setState(() {
      _loading = true;
      _error = null;
    });
    try {
      final results = await widget.search(query);
      if (!mounted) return;
      setState(() => _results = results);
    } on Object catch (error) {
      if (!mounted) return;
      setState(() {
        _results = const <Map<String, dynamic>>[];
        _error = error.toString();
      });
    } finally {
      if (mounted) setState(() => _loading = false);
    }
  }

  @override
  Widget build(BuildContext context) {
    final l10n = AppLocalizations.of(context)!;
    final theme = Theme.of(context);
    return AlertDialog(
      title: Text(l10n.searchMessages),
      content: SizedBox(
        width: 560,
        height: 420,
        child: Column(
          children: [
            TextField(
              controller: _query,
              autofocus: true,
              onSubmitted: (_) => _submit(),
              decoration: InputDecoration(
                hintText: l10n.searchMessagesHint,
                suffixIcon: IconButton(
                  tooltip: l10n.searchMessages,
                  onPressed: _loading ? null : _submit,
                  icon: _loading
                      ? const SizedBox(
                          width: 18,
                          height: 18,
                          child: CircularProgressIndicator(strokeWidth: 2),
                        )
                      : const Icon(Icons.search),
                ),
                border: const OutlineInputBorder(),
              ),
            ),
            const SizedBox(height: 12),
            if (_error != null)
              Align(
                alignment: Alignment.centerLeft,
                child: Text(
                  '${l10n.searchMessagesFailed}$_error',
                  style: TextStyle(color: theme.colorScheme.error),
                ),
              )
            else if (_results.isEmpty && !_loading)
              Expanded(child: Center(child: Text(l10n.searchMessagesEmpty)))
            else
              Expanded(
                child: ListView.separated(
                  itemCount: _results.length,
                  separatorBuilder: (_, _) => const Divider(height: 1),
                  itemBuilder: (context, index) {
                    final result = _results[index];
                    return ListTile(
                      dense: true,
                      leading: const Icon(Icons.chat_bubble_outline),
                      title: Text(result['content']?.toString() ?? ''),
                      subtitle: Text(
                        result['conversationId']?.toString() ?? '',
                      ),
                    );
                  },
                ),
              ),
          ],
        ),
      ),
      actions: [
        TextButton(
          onPressed: () => Navigator.of(context).pop(),
          child: const Text('关闭'),
        ),
      ],
    );
  }
}
