import argparse
import hashlib
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional
import httpx

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

NCAA_BASE_URL = "https://sdataprod.ncaa.com/"
PERSISTED_QUERY_HASH = "57f922d56d60d88326b62202b3d88e8cd3cfb6687931bc0b5b3dfab089b84faa"

DEFAULT_DATA_DIR = Path(__file__).resolve().parents[2] / "data" / "bronze"


def get_latest_stored_hash(manifest_path: Path, contest_id: int) -> Optional[str]:
    """Look up the most recent stored SHA-256 hash for this contest from the manifest."""
    if not manifest_path.exists():
        return None
    latest_hash = None
    with manifest_path.open("r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            try:
                record = json.loads(line)
                if record.get("contest_id") == contest_id and record.get("status") == "STORED_NEW_VERSION":
                    latest_hash = record.get("sha256")
            except json.JSONDecodeError:
                continue
    return latest_hash


def compute_payload_hash(payload: Dict[str, Any]) -> str:
    """Compute deterministic SHA-256 hash of canonical JSON data."""
    canonical_json = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical_json.encode("utf-8")).hexdigest()


def fetch_contest_data(contest_id: int) -> Dict[str, Any]:
    """Fetch raw gamecenter play-by-play data from NCAA API using canonical URL query formatting."""
    ext = json.dumps({"persistedQuery": {"version": 1, "sha256Hash": PERSISTED_QUERY_HASH}}, separators=(",", ":"))
    var = json.dumps({"contestId": str(contest_id), "staticTestEnv": None}, separators=(",", ":"))
    url = f"{NCAA_BASE_URL}?meta=NCAA_GetGamecenterPbpGenericById_web&extensions={ext}&variables={var}"

    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko)",
        "Accept": "application/json",
    }
    with httpx.Client(timeout=30.0) as client:
        resp = client.get(url, headers=headers)
        resp.raise_for_status()
        return resp.json()


def ingest_contest(contest_id: int, base_dir: Optional[Path] = None) -> Dict[str, Any]:
    """Fetch, validate, compare hash, and store versioned raw JSON snapshot."""
    bronze_dir = base_dir or DEFAULT_DATA_DIR
    manifest_path = bronze_dir / "ingest_manifest.jsonl"
    bronze_dir.mkdir(parents=True, exist_ok=True)

    logger.info(f"Fetching play-by-play data for contest {contest_id}...")
    raw_data = fetch_contest_data(contest_id)

    # Basic structural validation
    pbp = raw_data.get("data", {}).get("playbyplay")
    if not pbp or "periods" not in pbp or "teams" not in pbp:
        raise ValueError(f"Invalid payload received for contest {contest_id}: missing playbyplay data")

    sha256_hash = compute_payload_hash(raw_data)
    latest_hash = get_latest_stored_hash(manifest_path, contest_id)

    if latest_hash == sha256_hash:
        logger.info(
            f"[UNCHANGED] Contest {contest_id} payload hash matches latest stored version ({sha256_hash[:8]}...). Skipping write."
        )
        return {
            "contest_id": contest_id,
            "status": "UNCHANGED",
            "sha256": sha256_hash,
            "message": "Payload matches previous version exactly",
        }

    # New version detected (live game update, stat correction, or initial ingest)
    timestamp_str = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    contest_partition_dir = bronze_dir / f"contest_id={contest_id}"
    contest_partition_dir.mkdir(parents=True, exist_ok=True)

    file_name = f"ingest_timestamp={timestamp_str}.json"
    file_path = contest_partition_dir / file_name

    with file_path.open("w", encoding="utf-8") as f:
        json.dump(raw_data, f, indent=2)

    game_status = pbp.get("status", "UNKNOWN")
    manifest_entry = {
        "contest_id": contest_id,
        "ingest_timestamp": timestamp_str,
        "sha256": sha256_hash,
        "file_path": str(file_path.relative_to(bronze_dir.parent.parent)),
        "status": "STORED_NEW_VERSION",
        "game_status": game_status,
        "periods_count": len(pbp.get("periods", [])),
    }

    with manifest_path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(manifest_entry) + "\n")

    logger.info(
        f"[STORED_NEW_VERSION] Contest {contest_id} stored to {file_path.name} (hash: {sha256_hash[:8]}...)."
    )
    return {
        "contest_id": contest_id,
        "status": "STORED_NEW_VERSION",
        "sha256": sha256_hash,
        "file_path": str(file_path),
        "ingest_timestamp": timestamp_str,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest NCAA Lacrosse play-by-play data")
    parser.add_argument("--contest-id", type=int, default=6599996, help="NCAA Contest ID to ingest")
    args = parser.parse_args()
    ingest_contest(args.contest_id)
