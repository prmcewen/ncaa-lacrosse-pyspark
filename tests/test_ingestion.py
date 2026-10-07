import json
from pathlib import Path

from src.ingestion.ingest import compute_payload_hash, get_latest_stored_hash


def test_compute_payload_hash_deterministic():
    payload_a = {"contestId": "6599996", "teams": [{"id": 1}, {"id": 2}]}
    payload_b = {"teams": [{"id": 1}, {"id": 2}], "contestId": "6599996"}
    # Key ordering difference should not change canonical hash
    hash_a = compute_payload_hash(payload_a)
    hash_b = compute_payload_hash(payload_b)
    assert hash_a == hash_b
    assert len(hash_a) == 64


def test_get_latest_stored_hash(tmp_path: Path):
    manifest = tmp_path / "manifest.jsonl"
    entry1 = {"contest_id": 123, "sha256": "hash_v1", "status": "STORED_NEW_VERSION"}
    entry2 = {"contest_id": 123, "sha256": "hash_v2", "status": "STORED_NEW_VERSION"}
    entry3 = {"contest_id": 999, "sha256": "hash_other", "status": "STORED_NEW_VERSION"}

    with manifest.open("w") as f:
        f.write(json.dumps(entry1) + "\n")
        f.write(json.dumps(entry2) + "\n")
        f.write(json.dumps(entry3) + "\n")

    latest_123 = get_latest_stored_hash(manifest, 123)
    assert latest_123 == "hash_v2"

    latest_999 = get_latest_stored_hash(manifest, 999)
    assert latest_999 == "hash_other"

    latest_none = get_latest_stored_hash(manifest, 456)
    assert latest_none is None
