"""Deterministic, resumable NCAA-shaped Bronze load data."""
from __future__ import annotations

import copy
import hashlib
import json
import math
import random
import re
import subprocess
from collections import Counter
from pathlib import Path
from typing import Any

REPO = Path(__file__).resolve().parents[1]
FIXTURES = REPO / "data" / "bronze"
FIRST_CONTEST_ID = 1_000_000_000
FIRST_TEAM_ID = 9_000_000
TIMESTAMP = "20260928T000000Z"
FIRST_NAMES = ("Alex", "Blake", "Cameron", "Drew", "Evan", "Finn", "Gray", "Hayden",
               "Jordan", "Kai", "Logan", "Morgan", "Noah", "Owen", "Parker", "Quinn")
LAST_NAMES = ("Adams", "Bennett", "Carter", "Davis", "Ellis", "Foster", "Garcia", "Hayes",
              "Irwin", "Jones", "King", "Lewis", "Miller", "Nolan", "Ortiz", "Price",
              "Reed", "Stone", "Turner", "Vaughn", "Walker", "Young")
PLAYER_PATTERNS = (
    re.compile(r"^Faceoff (.+?) vs (.+?) won by "),
    re.compile(r"\(caused by ([^)]+)\)"),
    re.compile(r", Assist by (.+?)(?:,|\.|$)"),
    re.compile(r", SAVE (?:by )?(.+?)(?:\.|$)"),
    re.compile(r"^(.+?) at goalie for "),
)
TEAM_CODE_PATTERNS = (
    re.compile(r"^(?:GOAL|Shot|Turnover|Clear attempt|Timeout|Ground ball pickup) by ([A-Z][A-Z0-9.]+(?: [A-Z][A-Z0-9.]+)?)"),
    re.compile(r"^Penalty on ([A-Z][A-Z0-9.]+(?: [A-Z][A-Z0-9.]+)?)"),
    re.compile(r"won by ([A-Z][A-Z0-9.]+(?: [A-Z][A-Z0-9.]+)?)"),
    re.compile(r"at goalie for ([A-Z][A-Z0-9.]+(?: [A-Z][A-Z0-9.]+)?)"),
)



def _iter_plays(payload: dict[str, Any]):
    for period in payload["data"]["playbyplay"]["periods"]:
        for stat in period.get("playbyplayStats") or []:
            for play in stat.get("plays") or []:
                yield period, stat, play


def valid_count(payload: dict[str, Any]) -> int:
    return sum(bool((play.get("playText") or "").strip()) for _, _, play in _iter_plays(payload))


def _event(text: str) -> str:
    for prefix, event in (("GOAL by", "GOAL"), ("Shot by", "SHOT"),
                          ("Turnover by", "TURNOVER"), ("Faceoff", "FACEOFF"),
                          ("Ground ball", "GROUND_BALL"), ("Penalty on", "PENALTY"),
                          ("Clear attempt by", "CLEAR"), ("Timeout by", "TIMEOUT")):
        if text.startswith(prefix):
            return event
    if "at goalie for" in text:
        return "GOALIE_CHANGE"
    if "End-of-period" in text:
        return "PERIOD_END"
    return "UNKNOWN"


def load_templates(fixtures: Path = FIXTURES) -> list[dict[str, Any]]:
    templates = []
    for path in sorted(fixtures.glob("contest_id=*/*.json")):
        with path.open(encoding="utf-8") as stream:
            data = json.load(stream)
        if valid_count(data) >= 200:
            templates.append(data)
    if not templates:
        raise ValueError(f"No usable Bronze fixtures in {fixtures}")
    return templates


def contest_counts(target: int, seed: int) -> list[int]:
    if target < 200:
        raise ValueError("target must be at least 200 valid plays")
    rng = random.Random(seed)
    games = max(math.ceil(target / 300), round(target / 243))
    while games * 200 > target:
        games -= 1
    counts = [max(200, min(300, round(rng.gauss(243, 19)))) for _ in range(games)]
    difference = target - sum(counts)
    while difference:
        progress = False
        for index in rng.sample(range(games), games):
            if difference > 0 and counts[index] < 300:
                counts[index] += 1
                difference -= 1
                progress = True
            elif difference < 0 and counts[index] > 200:
                counts[index] -= 1
                difference += 1
                progress = True
            if difference == 0:
                break
        if not progress:
            raise ValueError("cannot distribute target within normal-size bounds")
    return counts


def _resize(payload: dict[str, Any], target: int, rng: random.Random) -> None:
    current = valid_count(payload)
    if current > target:
        removable = []
        for period in payload["data"]["playbyplay"]["periods"]:
            for stat in period.get("playbyplayStats") or []:
                for play in stat.get("plays") or []:
                    event = _event(play.get("playText") or "")
                    if (play.get("playText") or "").strip() and event in {"SHOT", "GROUND_BALL", "TURNOVER", "CLEAR", "UNKNOWN"}:
                        removable.append((stat, play))
        for stat, play in rng.sample(removable, current - target):
            stat["plays"].remove(play)
    elif current < target:
        candidates = []
        for period in payload["data"]["playbyplay"]["periods"]:
            for index, stat in enumerate(period.get("playbyplayStats") or []):
                for play in stat.get("plays") or []:
                    if _event(play.get("playText") or "") in {"SHOT", "GROUND_BALL", "TURNOVER", "CLEAR"}:
                        candidates.append((period, index, stat, play))
        additions: dict[int, list[dict[str, Any]]] = {}
        for _ in range(target - current):
            period, index, stat, play = rng.choice(candidates)
            clone = copy.deepcopy(stat)
            clone["plays"] = [copy.deepcopy(play)]
            clone["plays"][0]["homeScore"] = None
            clone["plays"][0]["visitorScore"] = None
            additions.setdefault(id(period), []).append((index, clone))
        for period in payload["data"]["playbyplay"]["periods"]:
            for index, clone in sorted(additions.get(id(period), []), key=lambda item: item[0], reverse=True):
                period["playbyplayStats"].insert(index + 1, clone)
    if valid_count(payload) != target:
        raise AssertionError("resize did not produce exact valid-play count")


def _names_in_text(text: str, aliases: dict[str, str]) -> set[str]:
    names = set()
    for pattern in PLAYER_PATTERNS:
        match = pattern.search(text)
        if match:
            for value in match.groups():
                value = value.strip(" .,")
                if " " in value and len(value) < 60:
                    names.add(value)
    match = re.match(
        r"^(?:GOAL|Shot|Turnover|Clear attempt|Timeout|Ground ball pickup) by (.+)$",
        text,
    )
    if match:
        remainder = match.group(1)
        for alias in sorted(aliases, key=len, reverse=True):
            if remainder.upper().startswith(alias) and (
                len(remainder) == len(alias) or remainder[len(alias)] in " .,"
            ):
                candidate = remainder[len(alias):].strip()
                candidate = re.split(
                    r"\s*\(|, SAVE|, Assist by|, goal number| HIGH| WIDE| HIT POST| HIT CROSSBAR| BLOCKED",
                    candidate, maxsplit=1,
                )[0].strip(" .,")
                if " " in candidate and len(candidate) < 60:
                    names.add(candidate)
                break
    return names


def _rename(payload: dict[str, Any], contest_id: int, rng: random.Random) -> None:
    pbp = payload["data"]["playbyplay"]
    old_teams = pbp["teams"]
    numbers = rng.sample(range(1024), 2)
    team_id_map = {}
    aliases = {}
    for team, number in zip(old_teams, numbers):
        old_id = int(team["teamId"])
        team_id_map[old_id] = FIRST_TEAM_ID + number
        code = f"T{number:03d}"
        for old in (team.get("name6Char"), team.get("nameShort"), team.get("nameFull")):
            if old:
                aliases[old.upper()] = code
        team.update(teamId=str(FIRST_TEAM_ID + number), name6Char=code,
                    nameShort=code, nameFull=f"Synthetic Team {number:03d}",
                    seoname=f"synthetic-team-{number:03d}",
                    color=f"#{(number * 918439 + 0x303030) & 0xFFFFFF:06X}")
    # Fixture play text sometimes uses aliases absent from team metadata.
    votes: dict[str, Counter[int]] = {}
    for _, stat, play in _iter_plays(payload):
        old_id = int(stat["teamId"]) if stat.get("teamId") is not None else None
        if old_id not in team_id_map:
            continue
        text = play.get("playText") or ""
        for pattern in TEAM_CODE_PATTERNS:
            match = pattern.search(text)
            if match:
                votes.setdefault(match.group(1).upper(), Counter())[old_id] += 1
    for alias, counts in votes.items():
        old_id = counts.most_common(1)[0][0]
        aliases[alias] = next(team["name6Char"] for team in old_teams
                              if int(team["teamId"]) == team_id_map[old_id])
    pbp["contestId"] = contest_id
    pbp["title"] = f"Synthetic contest {contest_id}"
    texts = [play.get("playText") or "" for _, _, play in _iter_plays(payload)]
    source_names = sorted(set().union(*(_names_in_text(text, aliases) for text in texts)), key=len, reverse=True)
    player_map = {}
    for index, name in enumerate(source_names):
        first = FIRST_NAMES[(index + contest_id) % len(FIRST_NAMES)]
        last = LAST_NAMES[(index // len(FIRST_NAMES) + contest_id) % len(LAST_NAMES)]
        player_map[name] = f"{first} {last}"
    player_regex = re.compile("|".join(re.escape(name) for name in source_names)) if source_names else None
    alias_regex = re.compile(r"(?<![A-Za-z])(" + "|".join(
        re.escape(alias) for alias in sorted(aliases, key=len, reverse=True)) + r")(?![A-Za-z])",
        re.IGNORECASE) if aliases else None
    for period in pbp["periods"]:
        for stat in period.get("playbyplayStats") or []:
            if stat.get("teamId") is not None:
                stat["teamId"] = team_id_map.get(int(stat["teamId"]), stat["teamId"])
    for _, stat, play in _iter_plays(payload):
        text = play.get("playText") or ""
        if player_regex:
            text = player_regex.sub(lambda match: player_map[match.group()], text)
        if alias_regex:
            text = alias_regex.sub(lambda match: aliases[match.group().upper()], text)
        play["playText"] = text


def _manifest_record(run_root: Path, path: Path, contest_id: int, payload_bytes: bytes,
                     periods: int) -> dict[str, Any]:
    return {
        "contest_id": contest_id, "ingest_timestamp": TIMESTAMP,
        "sha256": hashlib.sha256(payload_bytes).hexdigest(),
        "file_path": str(path.relative_to(run_root)),
        "status": "STORED_NEW_VERSION", "game_status": "final",
        "periods_count": periods,
    }


def generate(run_root: Path, target: int, seed: int = 1729,
             fixtures: Path = FIXTURES) -> dict[str, Any]:
    run_root = run_root.resolve()
    bronze = run_root / "data" / "bronze"
    bronze.mkdir(parents=True, exist_ok=True)
    manifest = bronze / "ingest_manifest.jsonl"
    summary_path = run_root / "generation_summary.json"
    if summary_path.exists():
        prior = json.loads(summary_path.read_text(encoding="utf-8"))
        if prior["seed"] != seed or prior["target_valid_plays"] != target:
            raise ValueError("run root already contains a different seed or target")
    existing = set()
    if manifest.exists():
        with manifest.open(encoding="utf-8") as stream:
            for line in stream:
                if line.strip():
                    record = json.loads(line)
                    if record.get("status") == "STORED_NEW_VERSION" and (run_root / record["file_path"]).exists():
                        existing.add(int(record["contest_id"]))
    templates = load_templates(fixtures)
    template_counts = [valid_count(template) for template in templates]
    counts = contest_counts(target, seed)
    events = Counter()
    raw_bytes = 0
    with manifest.open("a", encoding="utf-8") as output:
        for index, count in enumerate(counts):
            contest_id = FIRST_CONTEST_ID + index
            path = bronze / f"contest_id={contest_id}" / f"ingest_timestamp={TIMESTAMP}.json"
            if contest_id in existing:
                raw = path.read_bytes()
                raw_bytes += len(raw)
                prior_payload = json.loads(raw)
                events.update(_event(play.get("playText") or "") for _, _, play in _iter_plays(prior_payload)
                              if (play.get("playText") or "").strip())
                continue
            rng = random.Random(seed * 1_000_003 + contest_id)
            ranked = sorted(range(len(templates)), key=lambda n: abs(template_counts[n] - count))
            template = templates[rng.choice(ranked[:min(3, len(ranked))])]
            payload = copy.deepcopy(template)
            _resize(payload, count, rng)
            _rename(payload, contest_id, rng)
            encoded = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
            path.parent.mkdir(parents=True, exist_ok=True)
            temporary = path.with_suffix(".json.tmp")
            temporary.write_bytes(encoded)
            temporary.replace(path)
            record = _manifest_record(run_root, path, contest_id, encoded,
                                      len(payload["data"]["playbyplay"]["periods"]))
            output.write(json.dumps(record, separators=(",", ":")) + "\n")
            output.flush()
            raw_bytes += len(encoded)
            events.update(_event(play.get("playText") or "") for _, _, play in _iter_plays(payload)
                          if (play.get("playText") or "").strip())
    revision = subprocess.run(["git", "rev-parse", "HEAD"], cwd=REPO,
                              capture_output=True, text=True, check=False).stdout.strip()
    summary = {"seed": seed, "target_valid_plays": target, "valid_plays": sum(counts),
               "contests": len(counts), "bronze_bytes": raw_bytes, "bronze_files": len(counts),
               "event_counts": dict(events), "timestamp": TIMESTAMP,
               "git_revision": revision, "fixture_count": len(templates)}
    summary_path.write_text(json.dumps(summary, indent=2) + "\n")
    return summary


def validate(run_root: Path, sample: int | None = None) -> dict[str, Any]:
    run_root = run_root.resolve()
    manifest = run_root / "data" / "bronze" / "ingest_manifest.jsonl"
    entries = [json.loads(line) for line in manifest.read_text(encoding="utf-8").splitlines() if line.strip()]
    if sample is not None:
        entries = entries[:sample]
    total = 0
    events = Counter()
    for entry in entries:
        path = run_root / entry["file_path"]
        raw = path.read_bytes()
        if hashlib.sha256(raw).hexdigest() != entry["sha256"]:
            raise ValueError(f"hash mismatch: {path}")
        payload = json.loads(raw)
        pbp = payload["data"]["playbyplay"]
        if pbp["contestId"] != entry["contest_id"] or len(pbp["teams"]) != 2:
            raise ValueError(f"invalid contest/team metadata: {path}")
        team_ids = {int(team["teamId"]) for team in pbp["teams"]}
        for _, stat, play in _iter_plays(payload):
            if stat.get("teamId") is not None and int(stat["teamId"]) not in team_ids:
                raise ValueError(f"unknown teamId: {path}")
            if (play.get("playText") or "").strip():
                total += 1
                events[_event(play["playText"])] += 1
    summary = {"validated_contests": len(entries), "validated_plays": total,
               "event_counts": dict(events)}
    if sample is None:
        expected = json.loads((run_root / "generation_summary.json").read_text())["target_valid_plays"]
        if total != expected:
            raise ValueError(f"expected {expected} valid plays, found {total}")
    return summary
