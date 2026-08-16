# Golden vectors: independent pre-computation record

All `expected.*` files in this tree are literal values generated with:

- Python 3.12.4
- `jcs` package 0.2.1 (`pip install jcs==0.2.1`), an independent RFC 8785
  canonicalizer that sorts object keys by UTF-16 BE code units
- Python `hashlib.sha256` for raw and canonical digests
- Python `json.dumps(..., separators=(",", ":"))` only to author input files,
  except `jcs/raw-number-literals.input.json`, which is hand-written raw JSON
  and preserves the literal `1.0`, `1e0`, and `0e0` tokens.

`reference-generator.py` is the exact generator script retained for audit. It
locates the fixture root with `Path(__file__).resolve().parent`, does not
import or invoke the Rust crate, and the Rust golden tests never execute it.
Tests only read the literal `expected.*` files and compare them against the
crate implementation.

Reference outputs (also stored as literal files):

- brief valid-minimal briefTreeDigest:
  `b67114b2d0c0da53e21e34e688b4a340cf315aeb1a5e2a208393f131cc8f8768`
- brief valid-schema-registry briefTreeDigest:
  `cb13f66d4c08ad4dde431970aa7f1c7cebe05f32b9aad9a48ba73877f0ef5d6a`
- `brief/tree-digest-vectors/*` contains one literal briefTreeDigest per
  frozen tree-record field, plus object-key-order and set-array-order
  stability cases.
- handoff valid-minimal digests:
  - declarationDigest: `0286f580615d4a573754279f904aab6443a7c26d0d00dcb2b4346712603de563`
  - artifactTransferSetDigest: `72b860950d63852c3c65d75401cf22e363325ffba85bdc71559f55735156733c`
  - idempotencyKey: `0dece97dfdf769c592529c2f5984a791769e98aaa96c85396deafe597dc299b9`
  - deliveryPayloadDigest: `ae77a7a4f22639ba01a69f3625f60fa4e11c9d28da3bf423dace0fffa1eba1c3`
  - envelopeSha256: `23daffb83368e04bbcf7b61300b70cbc91d7fa7647ad677b61ca7b940cc8efa4`

- `handoff/binding-order-reversed-valid` has the same semantic digests as
  `handoff/valid-minimal`; only the raw artifactBindings array order differs.
- `handoff/negative/source-port-closure-mismatch` is a fully digest-consistent
  envelope whose declaration and binding source ports differ; the expected
  error is `HANDOFF_IDEMPOTENCY_INVALID`.

Negative fixtures carry `expected.txt` with the frozen error code; no hash is
expected for a rejected document.
