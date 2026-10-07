"""Prepare and run isolated load-test data; large runs are opt-in."""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import resource
import socket
import subprocess
import sys
import time
from pathlib import Path

from benchmarks.load_data import FIRST_CONTEST_ID, generate, validate
from src.etl.storage_layout import resolve_table_path

REPO = Path(__file__).resolve().parents[1]


def _root(value: str) -> Path:
    root = Path(value).resolve()
    if root == REPO or root.is_relative_to((REPO / "data").resolve()):
        raise ValueError("choose an isolated run root, not the repository data directory")
    return root


def _record(root: Path, name: str, data: dict) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    output = root / f"{name}.json"
    output.write_text(json.dumps(data, indent=2, default=str) + "\n", encoding="utf-8")
    return output


def _environment(args, root: Path) -> dict[str, str]:
    env = os.environ.copy()
    env["LAXPXP_DATA_DIR"] = str(root / "data")
    env["LAXPXP_SPARK_MASTER"] = f"local[{args.workers}]"
    env["LAXPXP_SPARK_DRIVER_MEMORY"] = args.driver_memory
    env["LAXPXP_SPARK_DEFAULT_PARALLELISM"] = str(args.workers)
    env["LAXPXP_SPARK_SHUFFLE_PARTITIONS"] = str(args.shuffle_partitions)
    env["LAXPXP_DELTA_WRITE_PARTITIONS"] = str(args.write_partitions)
    env["LAXPXP_SPARK_EVENT_LOG_DIR"] = str(root / "spark-events")
    env["LAXPXP_BENCH_METRICS"] = str(root / f"{args.command}-stages.json")
    env["LAXPXP_PLAN_OUTPUT"] = str(root / f"{args.command}-spark-plan.md")
    if args.command == "run-silver-gold":
        gold = root / "gold-rebuild"
        if gold.exists() and any(gold.iterdir()):
            raise ValueError(f"fresh Gold output required: {gold}")
        env["LAXPXP_GOLD_DIR"] = str(gold)
    return env


def _run_etl(args) -> dict:
    root = _root(args.run_root)
    if not (root / "generation_summary.json").exists():
        raise ValueError("generate the isolated dataset first")
    if args.command == "run-silver-gold" and not resolve_table_path(root / "data/silver/silver_plays").exists():
        raise ValueError("Silver plays do not exist; run full ETL first")
    env = _environment(args, root)
    command = ["uv", "run", "python", "-m", "src.etl.run_pipeline"]
    if args.command == "run-etl":
        command.append("--full-refresh")
    log_path = root / f"{args.command}.log"
    before = resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss
    started = time.perf_counter()
    with log_path.open("w", encoding="utf-8") as log:
        result = subprocess.run(command, cwd=REPO, env=env, stdout=log, stderr=subprocess.STDOUT)
    duration = time.perf_counter() - started
    after = resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss
    metrics = {
        "command": args.command, "argv": command, "returncode": result.returncode,
        "wall_seconds": duration, "max_child_rss_kib": after,
        "rss_before_kib": before, "workers": args.workers,
        "driver_memory": args.driver_memory, "shuffle_partitions": args.shuffle_partitions,
        "write_partitions": args.write_partitions,
        "run_root": str(root), "log": str(log_path),
        "disk_bytes": {layer: _tree_bytes(root / "data" / layer) for layer in ("bronze", "silver", "gold")},
        "gold_rebuild_bytes": _tree_bytes(root / "gold-rebuild"),
    }
    _record(root, f"{args.command}-result", metrics)
    return metrics


def _tree_bytes(path: Path) -> int:
    if not path.exists():
        return 0
    return sum(entry.stat().st_size for entry in path.rglob("*") if entry.is_file())


def _find_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


async def _api_requests(base_url: str, contest_id: int, seconds: int,
                        concurrency: int) -> dict:
    import httpx
    urls = [
        ("REST summary", "GET", f"/api/contests/{contest_id}/summary", None),
        ("REST plays", "GET", f"/api/contests/{contest_id}/plays?limit=50", None),
        ("REST filtered", "GET", f"/api/plays?contest_id={contest_id}&event_type=GOAL&limit=50", None),
        ("REST analytics", "GET", f"/api/shooting-efficiency?contest_id={contest_id}", None),
        ("GraphQL contest", "POST", "/graphql",
         {"query": f"query {{ contest(id: {contest_id}) {{ contestId title }} }}"}),
        ("GraphQL plays", "POST", "/graphql",
         {"query": f"query {{ plays(contestId: {contest_id}, limit: 50) {{ playId }} }}"}),
    ]
    results: dict[str, list[float]] = {name: [] for name, *_ in urls}
    errors: dict[str, int] = {name: 0 for name, *_ in urls}
    response_bytes: dict[str, int] = {name: 0 for name, *_ in urls}
    deadline = time.monotonic() + seconds
    async with httpx.AsyncClient(base_url=base_url, timeout=120) as client:
        async def worker(index: int):
            turn = index
            while time.monotonic() < deadline:
                name, method, url, body = urls[turn % len(urls)]
                turn += concurrency
                start = time.perf_counter()
                try:
                    response = await client.request(method, url, json=body)
                    elapsed = time.perf_counter() - start
                    if response.status_code >= 400 or (
                        method == "POST" and response.json().get("errors")
                    ):
                        errors[name] += 1
                    else:
                        results[name].append(elapsed)
                        response_bytes[name] += len(response.content)
                except Exception:
                    errors[name] += 1
        await asyncio.gather(*(worker(index) for index in range(concurrency)))
    return {
        name: {"requests": len(samples), "errors": errors[name],
               "p50_ms": _percentile(samples, 0.50) * 1000,
               "p95_ms": _percentile(samples, 0.95) * 1000,
               "p99_ms": _percentile(samples, 0.99) * 1000,
               "response_bytes": response_bytes[name]}
        for name, samples in results.items()
    }


def _percentile(values: list[float], fraction: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, round(fraction * (len(ordered) - 1)))]


def _run_api(args) -> dict:
    import httpx
    root = _root(args.run_root)
    summary = json.loads((root / "generation_summary.json").read_text())
    contest_id = args.contest_id or FIRST_CONTEST_ID + summary["contests"] // 2
    port = _find_port()
    env = os.environ.copy()
    env["LAXPXP_DATA_DIR"] = str(root / "data")
    command = ["uv", "run", "uvicorn", "src.api.main:app", "--host", "127.0.0.1", "--port", str(port)]
    log_path = root / "run-api.log"
    with log_path.open("w", encoding="utf-8") as log:
        server = subprocess.Popen(command, cwd=REPO, env=env, stdout=log, stderr=subprocess.STDOUT)
        try:
            base_url = f"http://127.0.0.1:{port}"
            for _ in range(60):
                if server.poll() is not None:
                    raise RuntimeError(f"API stopped early; see {log_path}")
                try:
                    if httpx.get(base_url + "/api/health", timeout=1).status_code == 200:
                        break
                except httpx.HTTPError:
                    time.sleep(1)
            else:
                raise RuntimeError(f"API did not start; see {log_path}")
            results = asyncio.run(_api_requests(base_url, contest_id, args.seconds,
                                                 args.concurrency))
        finally:
            server.terminate()
            try:
                server.wait(timeout=10)
            except subprocess.TimeoutExpired:
                server.kill()
                server.wait()
    report = {"contest_id": contest_id, "seconds": args.seconds,
              "concurrency": args.concurrency, "results": results,
              "log": str(log_path)}
    _record(root, "run-api-result", report)
    return report


def _report(root: Path) -> Path:
    lines = ["# Load-test preparation and pilot results", ""]
    for path in sorted(root.glob("*-result.json")):
        data = json.loads(path.read_text())
        lines.extend([f"## {path.stem}", "", "~~~json", json.dumps(data, indent=2), "~~~", ""])
    output = root / "report.md"
    output.write_text("\n".join(lines), encoding="utf-8")
    return output


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("generate", "validate", "run-etl", "run-silver-gold", "run-api", "report"):
        cmd = commands.add_parser(name)
        cmd.add_argument("--run-root", required=True, help="isolated run directory")
        if name == "generate":
            cmd.add_argument("--target-plays", type=int, required=True)
            cmd.add_argument("--seed", type=int, default=1729)
        if name == "validate":
            cmd.add_argument("--sample", type=int)
        if name in {"run-etl", "run-silver-gold"}:
            cmd.add_argument("--workers", type=int, default=8)
            cmd.add_argument("--driver-memory", default="8g")
            cmd.add_argument("--shuffle-partitions", type=int, default=128)
            cmd.add_argument("--write-partitions", type=int, default=16)
        if name == "run-api":
            cmd.add_argument("--contest-id", type=int)
            cmd.add_argument("--seconds", type=int, default=30)
            cmd.add_argument("--concurrency", type=int, default=4)
    args = parser.parse_args()
    root = _root(args.run_root)
    if args.command == "generate":
        result = generate(root, args.target_plays, args.seed)
    elif args.command == "validate":
        result = validate(root, args.sample)
    elif args.command in {"run-etl", "run-silver-gold"}:
        result = _run_etl(args)
    elif args.command == "run-api":
        result = _run_api(args)
    else:
        result = {"report": str(_report(root))}
    print(json.dumps(result, indent=2))
    if result.get("returncode", 0):
        sys.exit(result["returncode"])


if __name__ == "__main__":
    main()
