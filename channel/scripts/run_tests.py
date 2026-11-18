#!/usr/bin/env python3
# Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
# SPDX-License-Identifier: Apache-2.0

"""按包边界运行全部测试，并保留每组的耗时与退出状态。"""

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
GROUPS = ("core", "store", "integration", "other")
TIMEOUTS = {"core": "5m", "store": "20m", "integration": "20m", "other": "10m"}


def list_groups():
    """每次从 Go 获取包清单，使新增包自动进入唯一分组。"""
    result = subprocess.run(
        ["go", "list", "-f", "{{.ImportPath}} {{.Module.Path}}", "./..."],
        cwd=ROOT, check=True, text=True, capture_output=True,
    )
    groups = {name: [] for name in GROUPS}
    for line in result.stdout.splitlines():
        package, module = line.split()
        relative = package.removeprefix(module).lstrip("/")
        if relative == "internal/runtime":
            group = "core"
        elif relative == "store" or relative.startswith(("store/", "internal/storage/", "internal/testutil/memorystore")):
            group = "store"
        elif relative == "integration" or relative.startswith("integration/"):
            group = "integration"
        else:
            group = "other"
        groups[group].append(package)
    return groups


def command(group, packages):
    args = ["go", "test", "-race", "-count=1", f"-timeout={TIMEOUTS[group]}", f"-coverprofile=coverage/{group}.out"]
    if group == "integration":
        args.append("-coverpkg=./internal/runtime")
    return args + packages


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("group", choices=("all",) + GROUPS, nargs="?", default="all")
    parser.add_argument("--list", action="store_true", help="输出动态分组 JSON，不执行测试")
    parser.add_argument("--dry-run", action="store_true", help="输出测试命令，不执行测试")
    options = parser.parse_args()
    groups = list_groups()
    (ROOT / "coverage").mkdir(exist_ok=True)
    if options.list:
        print(json.dumps(groups, ensure_ascii=False, indent=2))
        return 0
    selected = GROUPS if options.group == "all" else (options.group,)
    results = []
    for group in selected:
        packages = groups[group]
        args = command(group, packages)
        print(f"\n=== test-{group}: {len(packages)} packages, timeout {TIMEOUTS[group]} ===", flush=True)
        if options.dry_run:
            print(" ".join(args), flush=True)
            continue
        started = time.monotonic()
        # 空组不能调用无包参数的 go test，否则会意外执行根包。
        code = subprocess.run(args, cwd=ROOT, check=False).returncode if packages else 0
        elapsed = time.monotonic() - started
        results.append((group, code, elapsed))
        print(f"=== test-{group}: exit={code}, elapsed={elapsed:.2f}s ===", flush=True)
    if results:
        print("\n=== test summary ===", flush=True)
        for group, code, elapsed in results:
            print(f"{group}: exit={code}, elapsed={elapsed:.2f}s", flush=True)
    # 聚合执行保留全部失败信息；单组透传退出码，信号退出映射到 shell 约定。
    for _, code, _ in results:
        if code:
            return code if code > 0 else 128 - code
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except subprocess.CalledProcessError as error:
        if error.stderr:
            print(error.stderr, file=sys.stderr, end="")
        sys.exit(error.returncode)
