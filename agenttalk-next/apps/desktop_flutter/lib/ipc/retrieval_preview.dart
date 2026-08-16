/// Typed request and response models for the versioned retrieval preview query.
///
/// This slice is deliberately read-only: it carries bounded snippets and
/// metadata only. It never carries a prompt or source body.
class RetrievalPreviewRequest {
  const RetrievalPreviewRequest({
    required this.project,
    required this.conversation,
    required this.agent,
    required this.query,
    required this.scope,
    required this.sourceTypes,
    required this.limit,
    this.mode = 'exact',
  });

  final String? project;
  final String? conversation;
  final String? agent;
  final String query;
  final String scope;
  final List<String> sourceTypes;
  final int limit;
  final String mode;

  Map<String, dynamic> toJson() {
    _requireNonEmpty('query', query);
    if (scope != 'project' && scope != 'conversation') {
      throw FormatException('retrieval.preview scope is invalid');
    }
    final scopedId = scope == 'conversation' ? conversation : project;
    if (scopedId == null || scopedId.trim().isEmpty) {
      throw FormatException(
        'retrieval.preview requires an explicit $scope scope',
      );
    }
    if (limit <= 0 || limit > 100) {
      throw FormatException('retrieval.preview limit is invalid');
    }
    if (mode != 'exact' && mode != 'vector_fixture') {
      throw FormatException('retrieval.preview mode is invalid');
    }
    if (sourceTypes.any((type) => type.trim().isEmpty)) {
      throw FormatException('retrieval.preview sourceTypes are invalid');
    }
    return <String, dynamic>{
      'project': project,
      'conversation': conversation,
      'agent': agent,
      'query': query.trim(),
      'scope': scope,
      'sourceTypes': List<String>.unmodifiable(sourceTypes),
      'limit': limit,
      'mode': mode,
    };
  }
}

class RetrievalPreviewResult {
  const RetrievalPreviewResult({
    required this.retrievalVersion,
    required this.queryHash,
    required this.capabilities,
    required this.hits,
  });

  final String retrievalVersion;
  final String queryHash;
  final Map<String, dynamic> capabilities;
  final List<RetrievalPreviewHit> hits;

  factory RetrievalPreviewResult.fromResponse(Map<String, dynamic> response) {
    final payload = response['payload'];
    if (payload is! Map<String, dynamic>) {
      throw const FormatException(
        'retrieval.preview response payload is invalid',
      );
    }
    final retrievalVersion = _requiredString(
      payload,
      'retrievalVersion',
      'retrieval.preview retrievalVersion is invalid',
    );
    final queryHash = _requiredString(
      payload,
      'queryHash',
      'retrieval.preview queryHash is invalid',
    );
    final capabilities = payload['capabilities'];
    final rawHits = payload['hits'];
    if (capabilities is! Map || rawHits is! List) {
      throw const FormatException('retrieval.preview result is invalid');
    }
    final typedCapabilities = <String, dynamic>{};
    capabilities.forEach((key, value) {
      if (key is String) typedCapabilities[key] = value;
    });
    if (typedCapabilities.length != capabilities.length) {
      throw const FormatException('retrieval.preview capabilities are invalid');
    }
    return RetrievalPreviewResult(
      retrievalVersion: retrievalVersion,
      queryHash: queryHash,
      capabilities: Map<String, dynamic>.unmodifiable(typedCapabilities),
      hits: List<RetrievalPreviewHit>.unmodifiable(
        rawHits.map((hit) {
          if (hit is! Map) {
            throw const FormatException('retrieval.preview hit is invalid');
          }
          final json = <String, dynamic>{};
          hit.forEach((key, value) {
            if (key is String) json[key] = value;
          });
          if (json.length != hit.length) {
            throw const FormatException(
              'retrieval.preview hit keys are invalid',
            );
          }
          return RetrievalPreviewHit.fromJson(json);
        }),
      ),
    );
  }
}

class RetrievalPreviewHit {
  const RetrievalPreviewHit({
    required this.hitId,
    required this.sourceType,
    required this.sourceObjectId,
    required this.snippet,
    required this.matchReason,
    required this.score,
    required this.estimatedTokens,
    required this.permissionDecision,
  });

  final String hitId;
  final String sourceType;
  final String sourceObjectId;
  final String snippet;
  final String matchReason;
  final double score;
  final int estimatedTokens;
  final String permissionDecision;

  factory RetrievalPreviewHit.fromJson(Map<String, dynamic> json) {
    final score = json['score'];
    final estimatedTokens = json['estimatedTokens'];
    if (score is! num || estimatedTokens is! num) {
      throw const FormatException('retrieval.preview hit numbers are invalid');
    }
    final integerTokens = estimatedTokens.toInt();
    if (estimatedTokens != integerTokens || integerTokens < 0) {
      throw const FormatException(
        'retrieval.preview estimatedTokens are invalid',
      );
    }
    return RetrievalPreviewHit(
      hitId: _requiredString(
        json,
        'hitId',
        'retrieval.preview hitId is invalid',
      ),
      sourceType: _requiredString(
        json,
        'sourceType',
        'retrieval.preview sourceType is invalid',
      ),
      sourceObjectId: _requiredString(
        json,
        'sourceObjectId',
        'retrieval.preview sourceObjectId is invalid',
      ),
      snippet: _requiredString(
        json,
        'snippet',
        'retrieval.preview snippet is invalid',
      ),
      matchReason: _requiredString(
        json,
        'matchReason',
        'retrieval.preview matchReason is invalid',
      ),
      score: score.toDouble(),
      estimatedTokens: integerTokens,
      permissionDecision: _requiredString(
        json,
        'permissionDecision',
        'retrieval.preview permissionDecision is invalid',
      ),
    );
  }

  String boundedSnippet([int maxCharacters = 280]) {
    if (maxCharacters <= 1 || snippet.length <= maxCharacters) return snippet;
    return '${snippet.substring(0, maxCharacters - 1)}…';
  }
}

String _requiredString(Map<String, dynamic> json, String key, String message) {
  final value = json[key];
  if (value is! String || value.trim().isEmpty) throw FormatException(message);
  return value;
}

void _requireNonEmpty(String field, String value) {
  if (value.trim().isEmpty) {
    throw FormatException('retrieval.preview $field is required');
  }
}
