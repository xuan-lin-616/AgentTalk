import json, hashlib, pathlib, jcs

REPO = pathlib.Path(__file__).resolve().parent

def sha(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()

def jc(obj):
    return jcs.canonicalize(obj)

def jcs_sha(obj) -> str:
    return sha(jc(obj))

def wb(rel, data: bytes):
    p = REPO / rel
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_bytes(data)

def wt(rel, text: str):
    wb(rel, text.encode('utf-8'))

def wj(rel, obj):
    wt(rel, json.dumps(obj, ensure_ascii=False, separators=(',', ':')) + "\n")

def wcanon(rel, obj):
    wb(rel, jc(obj))

# ---------------------------------------------------------------- JCS core
jcs_cases = [
    ("jcs/utf16-key-order", {"\U00010000": 1, "\uE000": 2, "a": 3}),
    ("jcs/unicode-escapes-nfc", {
        "escapedAscii": "\u0041",
        "escapedLatin": "\u00e9",
        "rawEmoji": "😀",
        "escapedCjk": "\u4f60\u597d",
        "slash": "/",
        "control": "\u0000",
    }),
    ("jcs/safe-integer-boundary", {
        "max": 9007199254740991,
        "oneDotZero": 1.0,
        "oneE0": 1e0,
        "zeroE0": 0e0,
        "arrayToSortInDigest": [3, 1, 2],
    }),
    ("jcs/array-order", [3, 1, 2, {"b": 2, "a": 1}]),
]
for rel, obj in jcs_cases:
    raw = json.dumps(obj, ensure_ascii=True, separators=(',', ':')).encode('utf-8') + b"\n"
    wb(f"{rel}.input.json", raw)
    wt(f"{rel}.expected.sha256.txt", sha(raw) + "\n")
    wb(f"{rel}.expected.canonical.json", jc(obj))
    wt(f"{rel}.expected.sha256-jcs.txt", sha(jc(obj)) + "\n")

wt("jcs/unsafe-integer.input.json", '{"n":9007199254740992}\n')
wt("jcs/negative-integer.input.json", '{"n":-1}\n')
wt("jcs/fractional.input.json", '{"n":1.5}\n')
wt("jcs/non-nfc.input.json", '{"value":"e\u0301"}\n')
wt("jcs/duplicate-key.input.json", '{"a":1,"a":2}\n')

# Hand-written raw JSON so the actual `1.0`, `1e0`, and `0e0` tokens are
# preserved verbatim (json.dumps would normalize `1e0` to `1.0`).
raw_number_literals = b'{"max":9007199254740991,"oneDotZero":1.0,"oneE0":1e0,"zeroE0":0e0}\n'
wb("jcs/raw-number-literals.input.json", raw_number_literals)
raw_number_literals_value = json.loads(raw_number_literals)
wb("jcs/raw-number-literals.expected.canonical.json", jc(raw_number_literals_value))
wt("jcs/raw-number-literals.expected.sha256.txt", sha(raw_number_literals) + "\n")
wt("jcs/raw-number-literals.expected.sha256-jcs.txt", jcs_sha(raw_number_literals_value) + "\n")

# ---------------------------------------------------------------- Brief
roles = [
    {"roleId": "architect", "displayName": "Architect"},
    {"roleId": "pm", "displayName": "PM"},
]
roadmap = b"# Roadmap\n\nGolden fixture roadmap.\n"
env_example = b"EXAMPLE_TOKEN=replace-me\n"
notes = b"Free-form notes for the fixture.\n"

def manifest_with_files(files):
    return {
        "schemaVersion": "agenttalk.brief.manifest.v1",
        "projectId": "golden-brief",
        "title": "Golden Brief",
        "roles": roles,
        "files": files,
    }

def file_obj(path, kind, fmt, schema_ref, required, content, ctx, owner):
    return {
        "path": path,
        "kind": kind,
        "format": fmt,
        "contentSchemaRef": schema_ref,
        "required": required,
        "sha256": sha(content),
        "size": len(content),
        "context": ctx,
        "declaredOwnerRoleId": owner,
    }

min_files = [
    file_obj("plan/roadmap.md", "plan", "markdown", None, True, roadmap,
             {"layer": "shared", "roleIds": ["architect", "pm"], "retention": "run", "workspaceAccess": "read_only"}, "pm"),
    file_obj("plan/.env.example", "plan", "text", None, False, env_example,
             {"layer": "role", "roleIds": ["pm"], "retention": "project", "workspaceAccess": "none"}, "pm"),
    file_obj("design/notes.txt", "design", "text", None, True, notes,
             {"layer": "persistent", "roleIds": ["architect"], "retention": "run", "workspaceAccess": "workspace_write"}, "pm"),
]
minimal = manifest_with_files(min_files)
raw_min = json.dumps(minimal, ensure_ascii=False, separators=(',', ':')).encode('utf-8') + b"\n"
wb("brief/valid-minimal/input.json", raw_min)
wt("brief/valid-minimal/expected.sha256.txt", sha(raw_min) + "\n")
wb("brief/valid-minimal/expected.canonical.json", jc(minimal))
wt("brief/valid-minimal/expected.sha256-jcs.txt", jcs_sha(minimal) + "\n")
wb("brief/valid-minimal/bytes/plan/roadmap.md", roadmap)
wb("brief/valid-minimal/bytes/plan/.env.example", env_example)
wb("brief/valid-minimal/bytes/design/notes.txt", notes)

def tree_record(m):
    roles_sorted = sorted(m["roles"], key=lambda r: r["roleId"].encode("utf-16-be"))
    def one_file(f):
        c = f["context"]
        return {
            "path": f["path"], "kind": f["kind"], "format": f["format"],
            "contentSchemaRef": f["contentSchemaRef"], "required": f["required"],
            "rawSha256": f["sha256"], "size": f["size"],
            "context": {
                "layer": c["layer"],
                "roleIds": sorted(c["roleIds"], key=lambda x: x.encode("utf-16-be")),
                "retention": c["retention"], "workspaceAccess": c["workspaceAccess"],
            },
            "declaredOwnerRoleId": f["declaredOwnerRoleId"],
        }
    return {
        "schemaVersion": "agenttalk.brief.tree.v1",
        "manifestSchemaVersion": "agenttalk.brief.manifest.v1",
        "projectId": m["projectId"], "title": m["title"],
        "roles": roles_sorted,
        "files": sorted((one_file(f) for f in m["files"]), key=lambda f: f["path"].encode("utf-16-be")),
    }

tree = tree_record(minimal)
wb("brief/valid-minimal/expected.tree-record.canonical.json", jc(tree))
wt("brief/valid-minimal/expected.brief-tree-digest.txt", sha(jc(tree)) + "\n")

# Brief schema registry valid case (markdown-null, json-schema, acceptance-json-schema).
design_schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "additionalProperties": False,
    "required": ["ok"],
    "properties": {"ok": {"type": "boolean"}},
}
acceptance_schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "additionalProperties": False,
    "required": ["accepted"],
    "properties": {"accepted": {"type": "boolean"}},
}
design_digest = jcs_sha(design_schema)
acceptance_digest = jcs_sha(acceptance_schema)
wb("brief/valid-schema-registry/registry/design-spec.json", jc(design_schema))
wb("brief/valid-schema-registry/registry/acceptance.json", jc(acceptance_schema))
wt("brief/valid-schema-registry/registry/design-spec.digest.txt", design_digest + "\n")
wt("brief/valid-schema-registry/registry/acceptance.digest.txt", acceptance_digest + "\n")

design_bytes = b'{"ok":true}\n'
acceptance_bytes = b'{"accepted":true}\n'
reg_files = [
    file_obj("plan/spec.json", "plan", "json",
             {"id": "agenttalk.design.spec.v1", "version": "1", "digest": design_digest},
             True, design_bytes,
             {"layer": "shared", "roleIds": ["architect"], "retention": "run", "workspaceAccess": "read_only"}, "pm"),
    file_obj("acceptance/checklist.json", "acceptance", "json",
             {"id": "agenttalk.acceptance.v1", "version": "1", "digest": acceptance_digest},
             True, acceptance_bytes,
             {"layer": "shared", "roleIds": ["architect", "pm"], "retention": "run", "workspaceAccess": "read_only"}, "pm"),
]
reg_manifest = manifest_with_files(reg_files)
raw_reg = json.dumps(reg_manifest, ensure_ascii=False, separators=(',', ':')).encode('utf-8') + b"\n"
wb("brief/valid-schema-registry/input.json", raw_reg)
wt("brief/valid-schema-registry/expected.sha256.txt", sha(raw_reg) + "\n")
wb("brief/valid-schema-registry/expected.canonical.json", jc(reg_manifest))
wt("brief/valid-schema-registry/expected.sha256-jcs.txt", jcs_sha(reg_manifest) + "\n")
wb("brief/valid-schema-registry/bytes/plan/spec.json", design_bytes)
wb("brief/valid-schema-registry/bytes/acceptance/checklist.json", acceptance_bytes)
reg_tree = tree_record(reg_manifest)
wb("brief/valid-schema-registry/expected.tree-record.canonical.json", jc(reg_tree))
wt("brief/valid-schema-registry/expected.brief-tree-digest.txt", sha(jc(reg_tree)) + "\n")

# ---------------------------------------------------------------- Brief tree digest literal vectors
minimal_bytes = {
    "plan/roadmap.md": roadmap,
    "plan/.env.example": env_example,
    "design/notes.txt": notes,
}

def clone_manifest():
    return json.loads(json.dumps(minimal))

def write_tree_vector(name, manifest, bytes_map=None):
    raw = json.dumps(manifest, ensure_ascii=False, separators=(',', ':')).encode('utf-8') + b"\n"
    wb(f"brief/tree-digest-vectors/{name}/input.json", raw)
    wt(f"brief/tree-digest-vectors/{name}/expected.brief-tree-digest.txt",
       sha(jc(tree_record(manifest))) + "\n")
    for rel, data in (bytes_map or minimal_bytes).items():
        wb(f"brief/tree-digest-vectors/{name}/bytes/{rel}", data)

# Object keys deliberately written in a different order. The parsed value is
# identical to valid-minimal, so the tree digest must match its literal.
key_order_manifest = {
    "files": minimal["files"],
    "roles": minimal["roles"],
    "title": minimal["title"],
    "projectId": minimal["projectId"],
    "schemaVersion": minimal["schemaVersion"],
}
write_tree_vector("object-key-order-shuffled", key_order_manifest)

# Roles, files, and context roleIds are set arrays; reversing them must not
# change the semantic tree record.
semantic_order = clone_manifest()
semantic_order["roles"].reverse()
semantic_order["files"].reverse()
for file in semantic_order["files"]:
    file["context"]["roleIds"].reverse()
write_tree_vector("semantic-order-shuffled", semantic_order)

changed_project = clone_manifest(); changed_project["projectId"] = "golden-brief-project"
write_tree_vector("project-id-change", changed_project)

changed_title = clone_manifest(); changed_title["title"] = "Golden Brief title changed"
write_tree_vector("title-change", changed_title)

changed_display = clone_manifest(); changed_display["roles"][0]["displayName"] = "Architect display changed"
write_tree_vector("role-display-name-change", changed_display)

changed_role_id = clone_manifest()
old_role = changed_role_id["roles"][0]["roleId"]
new_role = "lead-architect"
changed_role_id["roles"][0]["roleId"] = new_role
for file in changed_role_id["files"]:
    if old_role in file["context"]["roleIds"]:
        file["context"]["roleIds"] = [new_role if item == old_role else item for item in file["context"]["roleIds"]]
write_tree_vector("role-id-change", changed_role_id)

changed_path = clone_manifest()
changed_path["files"][0]["path"] = "plan/roadmap-changed.md"
path_bytes = dict(minimal_bytes)
path_bytes["plan/roadmap-changed.md"] = roadmap
write_tree_vector("file-path-change", changed_path, path_bytes)

changed_kind = clone_manifest(); changed_kind["files"][0]["kind"] = "design"
write_tree_vector("file-kind-change", changed_kind)

changed_format = clone_manifest(); changed_format["files"][0]["format"] = "text"
write_tree_vector("file-format-change", changed_format)

changed_required = clone_manifest(); changed_required["files"][0]["required"] = False
write_tree_vector("file-required-change", changed_required)

changed_content = clone_manifest()
changed_content_bytes = b"# Roadmap changed for literal tree digest\n"
changed_content["files"][0]["sha256"] = sha(changed_content_bytes)
changed_content["files"][0]["size"] = len(changed_content_bytes)
content_bytes = dict(minimal_bytes)
content_bytes["plan/roadmap.md"] = changed_content_bytes
write_tree_vector("file-raw-sha256-size-change", changed_content, content_bytes)

changed_layer = clone_manifest(); changed_layer["files"][0]["context"]["layer"] = "role"
write_tree_vector("context-layer-change", changed_layer)

changed_audience = clone_manifest(); changed_audience["files"][0]["context"]["roleIds"] = ["pm"]
write_tree_vector("audience-role-ids-change", changed_audience)

changed_retention = clone_manifest(); changed_retention["files"][0]["context"]["retention"] = "project"
write_tree_vector("context-retention-change", changed_retention)

changed_access = clone_manifest(); changed_access["files"][0]["context"]["workspaceAccess"] = "none"
write_tree_vector("workspace-access-change", changed_access)

changed_owner = clone_manifest(); changed_owner["files"][0]["declaredOwnerRoleId"] = "architect"
write_tree_vector("declared-owner-role-id-change", changed_owner)

changed_schema_ref = clone_manifest()
changed_schema_ref["files"][0]["contentSchemaRef"] = {
    "id": "agenttalk.design.spec.v1", "version": "1", "digest": design_digest,
}
write_tree_vector("content-schema-ref-change", changed_schema_ref)

# ---------------------------------------------------------------- Brief negatives
def write_brief_neg(name, obj, code, bytes_map=None):
    raw = json.dumps(obj, ensure_ascii=False, separators=(',', ':')).encode('utf-8') + b"\n"
    wb(f"brief/negative/{name}/input.json", raw)
    wt(f"brief/negative/{name}/expected.txt", code + "\n")
    for rel, data in (bytes_map or {}).items():
        wb(f"brief/negative/{name}/bytes/{rel}", data)

def base_files(extra_files=None):
    return min_files + (extra_files or [])

write_brief_neg("duplicate-key", None, "BRIEF_DUPLICATE_KEY")
wb("brief/negative/duplicate-key/input.json", b'{"schemaVersion":"agenttalk.brief.manifest.v1","schemaVersion":"agenttalk.brief.manifest.v1"}\n')

m = manifest_with_files(base_files())
m["unknownTop"] = True
write_brief_neg("unknown-field", m, "BRIEF_SCHEMA_VIOLATION")

m = manifest_with_files(base_files())
m["roles"] = []
write_brief_neg("roles-empty", m, "BRIEF_SCHEMA_VIOLATION")

m = manifest_with_files(base_files())
m["roles"] = [dict(roles[0]), dict(roles[0])]
write_brief_neg("roles-duplicate-full", m, "BRIEF_SCHEMA_VIOLATION")

m = manifest_with_files(base_files())
m["roles"] = [dict(roles[0]), {"roleId": "architect", "displayName": "Architect 2"}]
write_brief_neg("role-id-duplicate", m, "BRIEF_SCHEMA_VIOLATION")

lex_cases = {
    "path-absolute": "/plan/roadmap.md",
    "path-drive": "C:/plan/roadmap.md",
    "path-unc": "//server/plan/roadmap.md",
    "path-dotdot": "plan/../roadmap.md",
    "path-backslash": "plan\roadmap.md",
    "path-empty-segment": "plan//roadmap.md",
    "path-trailing-dot": "plan/roadmap.",
    "path-trailing-space": "plan/roadmap ",
    "path-ads": "plan/roadmap.md:stream",
    "path-device-name": "plan/CON.md",
    "path-non-nfc": "plan/cafe\u0301.md",
    "path-cas-case-variant": "plan/.AgentTalk/x.md",
    "path-root-manifest": "agenttalk-brief.json",
    "path-outside-authoring-tree": "notes/roadmap.md",
    "path-empty": "",
}
for name, bad_path in lex_cases.items():
    files = base_files()
    files[0] = dict(files[0], path=bad_path)
    m = manifest_with_files(files)
    code = "BRIEF_CAS_REFERENCE" if ".agenttalk" in bad_path.lower() else "BRIEF_PATH_LEXICAL_INVALID"
    write_brief_neg(name, m, code)

files = base_files()
files[0] = dict(files[0], path="plan/Roadmap.md")
files.append(dict(files[0], path="plan/roadmap.md"))
m = manifest_with_files(files)
write_brief_neg("path-case-alias", m, "BRIEF_PATH_ALIAS")

files = base_files()
files[0] = dict(files[0], path="plan/dup.md")
files.append(dict(files[0], path="plan/dup.md"))
write_brief_neg("path-duplicate", manifest_with_files(files), "BRIEF_DUPLICATE_PATH")

for token in [".git", ".ssh", ".aws", ".azure", ".kube", ".gnupg"]:
    files = base_files()
    files[0] = dict(files[0], path=f"plan/{token}/roadmap.md")
    write_brief_neg(f"sensitive-component-{token.strip('.')}", manifest_with_files(files), "BRIEF_SENSITIVE_SOURCE_FORBIDDEN")

for token in [".env", ".env.local", ".envrc", ".npmrc", ".pypirc", ".netrc", "id_rsa", "id_ecdsa", "id_ed25519", "id_dsa", "credentials", "credentials.json", "secrets.json", "service-account.json"]:
    files = base_files()
    files[0] = dict(files[0], path=f"plan/{token}")
    safe = token.replace(".", "-").replace("_", "-")
    write_brief_neg(f"sensitive-basename-{safe}", manifest_with_files(files), "BRIEF_SENSITIVE_SOURCE_FORBIDDEN")

for ext in [".pem", ".key", ".p8", ".p12", ".pfx", ".jks", ".keystore", ".kdbx"]:
    files = base_files()
    files[0] = dict(files[0], path=f"plan/key{ext}")
    write_brief_neg(f"sensitive-extension-{ext.strip('.')}", manifest_with_files(files), "BRIEF_SENSITIVE_SOURCE_FORBIDDEN")

files = base_files()
files[0] = dict(files[0], path="plan/.AGENTTALK/roadmap.md")
write_brief_neg("path-cas-uppercase", manifest_with_files(files), "BRIEF_CAS_REFERENCE")

files = base_files()
files[0] = dict(files[0], format="json", contentSchemaRef=None, sha256=sha(b'{"x":1}\n'), size=8)
write_brief_neg("schema-ref-json-null", manifest_with_files(files), "BRIEF_SCHEMA_VIOLATION")

files = base_files()
files[0] = dict(files[0], kind="acceptance", format="markdown", contentSchemaRef={"id":"agenttalk.acceptance.v1","version":"1","digest":"00"*32})
write_brief_neg("acceptance-not-json", manifest_with_files(files), "BRIEF_SCHEMA_VIOLATION")

files = base_files()
files[0] = dict(files[0], kind="acceptance", format="json", contentSchemaRef=None)
write_brief_neg("acceptance-json-null", manifest_with_files(files), "BRIEF_SCHEMA_VIOLATION")

files = base_files()
files[0] = dict(files[0]); files[0]["context"] = dict(files[0]["context"], layer="global")
write_brief_neg("layer-enum", manifest_with_files(files), "BRIEF_ENUM_INVALID")

files = base_files()
files[0] = dict(files[0]); files[0]["context"] = dict(files[0]["context"], retention="attempt")
write_brief_neg("retention-enum", manifest_with_files(files), "BRIEF_ENUM_INVALID")

files = base_files()
files[0] = dict(files[0]); files[0]["context"] = dict(files[0]["context"], workspaceAccess="read")
write_brief_neg("workspace-access-enum", manifest_with_files(files), "BRIEF_ENUM_INVALID")

files = base_files()
files[0] = dict(files[0], kind="asset")
write_brief_neg("kind-enum", manifest_with_files(files), "BRIEF_ENUM_INVALID")

files = base_files()
files[0] = dict(files[0], format="xml")
write_brief_neg("format-enum", manifest_with_files(files), "BRIEF_ENUM_INVALID")

files = base_files()
files[0] = dict(files[0], size=9007199254740992)
write_brief_neg("size-unsafe", manifest_with_files(files), "BRIEF_SCHEMA_VIOLATION")

m = manifest_with_files(base_files())
m["title"] = "Café"
write_brief_neg("title-non-nfc", m, "BRIEF_CANONICAL_ENCODING")

files = base_files()
files[0] = dict(files[0]); files[0]["context"] = dict(files[0]["context"], roleIds=["missing"])
write_brief_neg("context-unknown-role", manifest_with_files(files), "BRIEF_UNKNOWN_ROLE")

files = base_files()
files[0] = dict(files[0], declaredOwnerRoleId="missing")
write_brief_neg("owner-unknown-role", manifest_with_files(files), "BRIEF_UNKNOWN_ROLE")

files = base_files()
files[0] = dict(files[0], sha256="00"*32)
write_brief_neg("hash-mismatch", manifest_with_files(files), "BRIEF_HASH_MISMATCH", {"plan/roadmap.md": roadmap})

files = base_files()
files[0] = dict(files[0], size=len(roadmap)+1)
write_brief_neg("size-mismatch", manifest_with_files(files), "BRIEF_SIZE_MISMATCH", {"plan/roadmap.md": roadmap})

files = base_files()
write_brief_neg("declared-file-missing", manifest_with_files(files), "BRIEF_DECLARED_FILE_MISSING", {})

files = base_files()
files[0] = dict(files[0], contentSchemaRef={"id":"agenttalk.unknown.v1","version":"1","digest":"00"*32})
write_brief_neg("schema-ref-unresolved", manifest_with_files(files), "BRIEF_SCHEMA_REF_UNRESOLVED", {"plan/roadmap.md": roadmap})
# ---------------------------------------------------------------- Handoff
spec_blob = b"# Architecture spec\n\nUse handoff envelopes.\n"
notes_blob = b"Reviewer notes placeholder\n"
contract_blob = b'{"contract":"fixture"}\n'
evidence_blob = b'{"evidence":"fixture"}\n'
spec_sha = sha(spec_blob)
notes_sha = sha(notes_blob)
contract_sha = sha(contract_blob)
evidence_sha = sha(evidence_blob)

schema_spec = {"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object","additionalProperties":False}
schema_notes = {"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object","additionalProperties":False}
spec_schema_digest = jcs_sha(schema_spec)
notes_schema_digest = jcs_sha(schema_notes)

def write_handoff_context(prefix):
    wb(f"handoff/{prefix}/cas/{spec_sha}.blob", spec_blob)
    wb(f"handoff/{prefix}/cas/{notes_sha}.blob", notes_blob)
    wb(f"handoff/{prefix}/cas/{contract_sha}.blob", contract_blob)
    wb(f"handoff/{prefix}/cas/{evidence_sha}.blob", evidence_blob)
    wb(f"handoff/{prefix}/registry/spec-schema.json", jc(schema_spec))
    wb(f"handoff/{prefix}/registry/notes-schema.json", jc(schema_notes))
    wt(f"handoff/{prefix}/registry/spec-schema.digest.txt", spec_schema_digest + "\n")
    wt(f"handoff/{prefix}/registry/notes-schema.digest.txt", notes_schema_digest + "\n")

def build_declaration(staging_spec="staging-spec", staging_notes="staging-notes"):
    return {
        "schemaVersion": "agenttalk.handoff.delivery-declaration.v1",
        "projectRunId": "run-0001",
        "edgeId": "edge-0001",
        "fromTaskNodeId": "node-architect",
        "fromAttemptId": "1",
        "fromExecutionRunId": "er-0001",
        "leaseEpoch": 3,
        "outputs": [
            {"sourceOutputPortId": "zeta", "stagingObjectId": staging_notes,
             "declaredContentType": None, "declaredContentSchemaRef": None},
            {"sourceOutputPortId": "alpha", "stagingObjectId": staging_spec,
             "declaredContentType": "text/markdown",
             "declaredContentSchemaRef": {"id":"agenttalk.design.spec.v1","version":"1","digest":spec_schema_digest}},
        ],
    }

def declaration_record(d):
    outs = sorted(d["outputs"], key=lambda o: o["sourceOutputPortId"].encode("utf-16-be"))
    return {
        "schemaVersion": "agenttalk.handoff.delivery-declaration.v1",
        "projectRunId": d["projectRunId"], "edgeId": d["edgeId"],
        "fromTaskNodeId": d["fromTaskNodeId"], "fromAttemptId": d["fromAttemptId"],
        "fromExecutionRunId": d["fromExecutionRunId"], "leaseEpoch": d["leaseEpoch"],
        "outputs": outs,
    }

def build_envelope(producer_agent="agent-architect", allowed_agent="agent-developer"):
    return {
        "schemaVersion":"agenttalk.handoff.envelope.v1",
        "handoffId":"handoff-0001",
        "projectRunId":"run-0001",
        "edgeId":"edge-0001",
        "from":{"taskNodeId":"node-architect","attemptId":"1","executionRunId":"er-0001"},
        "to":{"taskNodeId":"node-developer","roleId":"developer","agentId":allowed_agent},
        "ownerBinding":{"taskNodeId":"node-architect","roleId":"architect"},
        "artifactBindings":[
            {"sourceOutput":{"portId":"zeta"}, "targetInput":{"portId":"zeta-in"},
             "artifactRef":{
                "objectRef":f"sha256:{notes_sha}", "sha256":notes_sha, "size":len(notes_blob),
                "contentSchemaRef":{"id":"agenttalk.notes.v1","version":"1","digest":notes_schema_digest},
                "normalizedContentType":"text/plain", "normalizedContentTypePolicyVersion":"1"}},
            {"sourceOutput":{"portId":"alpha"}, "targetInput":{"portId":"alpha-in"},
             "artifactRef":{
                "objectRef":f"sha256:{spec_sha}", "sha256":spec_sha, "size":len(spec_blob),
                "contentSchemaRef":{"id":"agenttalk.design.spec.v1","version":"1","digest":spec_schema_digest},
                "normalizedContentType":"text/markdown", "normalizedContentTypePolicyVersion":"1"}},
        ],
        "producer":{"agentId":producer_agent,"roleId":"architect"},
        "acceptance":{
            "contractRef":f"sha256:{contract_sha}","contractDigest":contract_sha,
            "evidenceRef":f"sha256:{evidence_sha}","evidenceDigest":evidence_sha,
            "validator":"agenttalk.acceptance.validator.v1","validatorVersion":"1"},
        "allowedConsumers":[{"taskNodeId":"node-developer","roleId":"developer","agentId":allowed_agent}],
        "producerContextManifestDigest":"33"*32,
        "dagSnapshotDigest":"11"*32,
        "roleBindingSnapshotDigest":"22"*32,
        "leaseEpoch":3,
        "declarationDigest":"",
        "artifactTransferSetDigest":"",
        "idempotencyKey":"",
        "deliveryPayloadDigest":"",
        "envelopeSha256":"",
    }

def sorted_bindings(envelope):
    return sorted(envelope["artifactBindings"],
                  key=lambda x: (x["sourceOutput"]["portId"].encode("utf-16-be"),
                                 x["targetInput"]["portId"].encode("utf-16-be")))

def transfer_record(envelope):
    bindings = sorted_bindings(envelope)
    return {
        "schemaVersion":"agenttalk.handoff.artifact-transfer-set.v1",
        "bindings":[{
            "sourceOutputPortId":b["sourceOutput"]["portId"],
            "targetInputPortId":b["targetInput"]["portId"],
            "artifactRef":b["artifactRef"],
        } for b in bindings],
    }

def envelope_hash_preimage(envelope):
    preimage = {k: v for k, v in envelope.items() if k != "envelopeSha256"}
    preimage["artifactBindings"] = sorted_bindings(envelope)
    return preimage

def idem_record(envelope):
    return {
        "schemaVersion":"agenttalk.handoff.delivery-identity.v1",
        "projectRunId":envelope["projectRunId"],
        "edgeId":envelope["edgeId"],
        "fromTaskNodeId":envelope["from"]["taskNodeId"],
        "fromAttemptId":envelope["from"]["attemptId"],
        "fromExecutionRunId":envelope["from"]["executionRunId"],
        "toTaskNodeId":envelope["to"]["taskNodeId"],
        "leaseEpoch":envelope["leaseEpoch"],
    }

def finalize_envelope(envelope, declaration):
    dd = sha(jc(declaration_record(declaration)))
    td = sha(jc(transfer_record(envelope)))
    ik = sha(jc(idem_record(envelope)))
    pd = sha(jc({
        "schemaVersion":"agenttalk.handoff.delivery-payload.v1",
        "declarationDigest":dd,
        "artifactTransferSetDigest":td,
        "acceptanceContractDigest":envelope["acceptance"]["contractDigest"],
        "acceptanceEvidenceDigest":envelope["acceptance"]["evidenceDigest"],
        "producerContextManifestDigest":envelope["producerContextManifestDigest"],
        "dagSnapshotDigest":envelope["dagSnapshotDigest"],
        "roleBindingSnapshotDigest":envelope["roleBindingSnapshotDigest"],
    }))
    env = dict(envelope)
    env.update({"declarationDigest":dd, "artifactTransferSetDigest":td,
                "idempotencyKey":ik, "deliveryPayloadDigest":pd})
    env["envelopeSha256"] = sha(jc(envelope_hash_preimage(env)))
    return env

def write_handoff_valid(prefix, producer_agent="agent-architect", declaration=None, envelope=None):
    declaration = declaration or build_declaration()
    envelope = finalize_envelope(envelope or build_envelope(producer_agent=producer_agent), declaration)
    raw = json.dumps(envelope, ensure_ascii=False, separators=(',', ':')).encode('utf-8') + b"\n"
    wb(f"handoff/{prefix}/envelope.input.json", raw)
    wt(f"handoff/{prefix}/expected.sha256.txt", sha(raw) + "\n")
    wb(f"handoff/{prefix}/expected.canonical.json", jc(envelope))
    wt(f"handoff/{prefix}/expected.sha256-jcs.txt", jcs_sha(envelope) + "\n")
    wj(f"handoff/{prefix}/declaration.input.json", declaration)
    wt(f"handoff/{prefix}/expected.declaration-digest.txt", envelope["declarationDigest"] + "\n")
    wt(f"handoff/{prefix}/expected.artifact-transfer-set-digest.txt", envelope["artifactTransferSetDigest"] + "\n")
    wt(f"handoff/{prefix}/expected.idempotency-key.txt", envelope["idempotencyKey"] + "\n")
    wt(f"handoff/{prefix}/expected.delivery-payload-digest.txt", envelope["deliveryPayloadDigest"] + "\n")
    wt(f"handoff/{prefix}/expected.envelope-sha256.txt", envelope["envelopeSha256"] + "\n")
    write_handoff_context(prefix)

write_handoff_valid("valid-minimal")
write_handoff_valid("wrong-producer-valid", producer_agent="agent-impostor")

# Only the artifactBindings order differs. All semantic digests, including
# envelopeSha256, must equal the valid-minimal literals.
reversed_envelope = build_envelope()
reversed_envelope["artifactBindings"] = list(reversed(reversed_envelope["artifactBindings"]))
write_handoff_valid("binding-order-reversed-valid", envelope=reversed_envelope)

# Handoff negatives reuse valid-minimal context.
valid_env = json.loads(REPO.joinpath("handoff/valid-minimal/envelope.input.json").read_text())

def recompute_binding_dependent_digests(env):
    td = sha(jc(transfer_record(env)))
    env["artifactTransferSetDigest"] = td
    env["deliveryPayloadDigest"] = sha(jc({
        "schemaVersion":"agenttalk.handoff.delivery-payload.v1",
        "declarationDigest":env["declarationDigest"],
        "artifactTransferSetDigest":td,
        "acceptanceContractDigest":env["acceptance"]["contractDigest"],
        "acceptanceEvidenceDigest":env["acceptance"]["evidenceDigest"],
        "producerContextManifestDigest":env["producerContextManifestDigest"],
        "dagSnapshotDigest":env["dagSnapshotDigest"],
        "roleBindingSnapshotDigest":env["roleBindingSnapshotDigest"],
    }))
    env["envelopeSha256"] = sha(jc(envelope_hash_preimage(env)))
    return env

def write_handoff_neg(name, env, code, decl=None, raw_bytes=None):
    if raw_bytes is not None:
        wb(f"handoff/negative/{name}/envelope.input.json", raw_bytes)
    else:
        wj(f"handoff/negative/{name}/envelope.input.json", env)
    wt(f"handoff/negative/{name}/expected.txt", code + "\n")
    if decl is not None:
        wj(f"handoff/negative/{name}/declaration.input.json", decl)

write_handoff_neg("duplicate-key", None, "HANDOFF_DUPLICATE_KEY", raw_bytes=b'{"schemaVersion":"agenttalk.handoff.envelope.v1","schemaVersion":"agenttalk.handoff.envelope.v1"}\n')

env = json.loads(json.dumps(valid_env)); env["unknownTop"] = True
write_handoff_neg("unknown-field", env, "HANDOFF_SCHEMA_VIOLATION")

env = json.loads(json.dumps(valid_env)); env["artifactBindings"][0]["artifactRef"]["contentSchemaRef"] = None
write_handoff_neg("artifact-schema-ref-null", env, "HANDOFF_SCHEMA_VIOLATION")

env = json.loads(json.dumps(valid_env)); env["artifactBindings"][0]["origin"] = {"taskNodeId":"x"}
write_handoff_neg("binding-origin-forbidden", env, "HANDOFF_SCHEMA_VIOLATION")

env = json.loads(json.dumps(valid_env)); env["priorEnvelope"] = "sha256:" + "00"*32
write_handoff_neg("prior-envelope-forbidden", env, "HANDOFF_SCHEMA_VIOLATION")

env = json.loads(json.dumps(valid_env)); env["producer"]["executionRunId"] = "er-double"
write_handoff_neg("producer-execution-run-id-forbidden", env, "HANDOFF_SCHEMA_VIOLATION")

env = json.loads(json.dumps(valid_env)); del env["ownerBinding"]
write_handoff_neg("owner-binding-missing", env, "HANDOFF_SCHEMA_VIOLATION")

env = json.loads(json.dumps(valid_env)); del env["to"]["roleId"]
write_handoff_neg("to-missing-role-id", env, "HANDOFF_SCHEMA_VIOLATION")

env = json.loads(json.dumps(valid_env)); env["allowedConsumers"][0]["agentId"] = "agent-other"
write_handoff_neg("allowed-consumers-mismatch", env, "HANDOFF_SCHEMA_VIOLATION")

env = json.loads(json.dumps(valid_env)); env["allowedConsumers"].append(dict(env["allowedConsumers"][0]))
write_handoff_neg("allowed-consumers-two", env, "HANDOFF_SCHEMA_VIOLATION")

env = json.loads(json.dumps(valid_env)); env["artifactBindings"].append(json.loads(json.dumps(env["artifactBindings"][0])))
write_handoff_neg("duplicate-binding", env, "HANDOFF_DUPLICATE_BINDING")

env = json.loads(json.dumps(valid_env)); env["leaseEpoch"] = 9007199254740992
write_handoff_neg("lease-epoch-unsafe", env, "HANDOFF_SCHEMA_VIOLATION")

env = json.loads(json.dumps(valid_env)); env["artifactBindings"][0]["artifactRef"]["sha256"] = "00"*32
write_handoff_neg("object-ref-mismatch", env, "HANDOFF_OBJECT_REF_MISMATCH")

env = json.loads(json.dumps(valid_env)); env["artifactBindings"][0]["artifactRef"]["size"] += 1
write_handoff_neg("artifact-size-mismatch", env, "HANDOFF_DIGEST_MISMATCH")

env = json.loads(json.dumps(valid_env)); env["artifactBindings"][0]["artifactRef"]["objectRef"] = "sha256:" + "00"*32
env["artifactBindings"][0]["artifactRef"]["sha256"] = "00"*32
write_handoff_neg("object-unknown", env, "HANDOFF_OBJECT_UNKNOWN")

env = json.loads(json.dumps(valid_env)); env["artifactBindings"][0]["artifactRef"]["normalizedContentType"] = "café"
write_handoff_neg("non-nfc-string", env, "HANDOFF_CANONICAL_ENCODING")

env = json.loads(json.dumps(valid_env)); env["acceptance"]["contractRef"] = "sha256:" + "00"*32
write_handoff_neg("acceptance-contract-ref-mismatch", env, "HANDOFF_DIGEST_MISMATCH")

env = json.loads(json.dumps(valid_env)); env["acceptance"]["evidenceRef"] = "sha256:" + "00"*32
write_handoff_neg("acceptance-evidence-ref-mismatch", env, "HANDOFF_DIGEST_MISMATCH")

env = json.loads(json.dumps(valid_env)); env["producer"]["agentId"] = "agent-impostor"
write_handoff_neg("envelope-hash-mismatch", env, "HANDOFF_ENVELOPE_HASH_MISMATCH")

env = json.loads(json.dumps(valid_env)); env["artifactBindings"][0]["artifactRef"]["normalizedContentType"] = "application/octet-stream"
write_handoff_neg("transfer-digest-mismatch", env, "HANDOFF_DIGEST_MISMATCH")

env = json.loads(json.dumps(valid_env)); env["to"]["taskNodeId"] = "node-other"; env["allowedConsumers"][0]["taskNodeId"] = "node-other"
write_handoff_neg("idempotency-key-mismatch", env, "HANDOFF_IDEMPOTENCY_INVALID")

env = json.loads(json.dumps(valid_env)); env["dagSnapshotDigest"] = "44"*32
write_handoff_neg("delivery-payload-mismatch", env, "HANDOFF_IDEMPOTENCY_INVALID")

decl_bad = build_declaration(staging_spec="staging-tampered")
write_handoff_neg("declaration-digest-mismatch", json.loads(json.dumps(valid_env)), "HANDOFF_IDEMPOTENCY_INVALID", decl=decl_bad)

# All binding-dependent digests are recomputed; only the source-port set
# closure between declaration and envelope is intentionally broken.
env = json.loads(json.dumps(valid_env))
for binding in env["artifactBindings"]:
    if binding["sourceOutput"]["portId"] == "alpha":
        binding["sourceOutput"]["portId"] = "alpha-tampered"
recompute_binding_dependent_digests(env)
write_handoff_neg("source-port-closure-mismatch", env, "HANDOFF_IDEMPOTENCY_INVALID")

env = json.loads(json.dumps(valid_env)); env["artifactBindings"][0]["artifactRef"]["contentSchemaRef"]["digest"] = "00"*32
write_handoff_neg("schema-ref-unresolved", env, "HANDOFF_SCHEMA_REF_UNRESOLVED")

print("generated")
print("spec_schema_digest", spec_schema_digest)
print("notes_schema_digest", notes_schema_digest)
print("minimal tree digest", sha(jc(tree)))
print("registry tree digest", sha(jc(reg_tree)))
print("handoff digests", {k: valid_env[k] for k in ["declarationDigest","artifactTransferSetDigest","idempotencyKey","deliveryPayloadDigest","envelopeSha256"]})
