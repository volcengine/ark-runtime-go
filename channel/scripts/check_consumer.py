#!/usr/bin/env python3
# Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
# SPDX-License-Identifier: Apache-2.0

"""从临时 Go module proxy 验证发布候选包，不使用 go.work 或 replace。

默认要求根 SDK 已公开发布。--sdk-source 仅用于本地验证尚未发布的根 SDK。
"""
import argparse
import datetime
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile
import time
import zipfile

ROOT = Path(__file__).resolve().parents[1]
MODULE = "github.com/volcengine/ark-runtime-go/channel"
SDK = "github.com/volcengine/ark-runtime-go"


def write_module(proxy, path, version, source, sdk_version=None):
    target = proxy / path / "@v"
    target.mkdir(parents=True, exist_ok=True)
    module_file = (source / "go.mod").read_text()
    if sdk_version:
        module_file = re.sub(r"(?m)(\s*github\.com/volcengine/ark-runtime-go\s+)v\S+", lambda match: match[1] + sdk_version, module_file)
    (target / (version + ".mod")).write_text(module_file)
    (target / (version + ".info")).write_text(json.dumps({"Version": version, "Time": datetime.datetime.now(datetime.timezone.utc).isoformat()}))
    (target / "list").write_text(version + "\n")
    (target.parent / "@latest").write_text((target / (version + ".info")).read_text())
    files = subprocess.check_output(["git", "-C", str(source), "ls-files", "--cached", "--others", "--exclude-standard"], text=True).splitlines()
    with zipfile.ZipFile(target / (version + ".zip"), "w", zipfile.ZIP_DEFLATED) as archive:
        for relative in files:
            file = source / relative
            if not file.is_file() or file.is_symlink():
                continue
            parts = Path(relative).parts
            if any(part in (".git", ".superpowers", "coverage", "bin") for part in parts):
                continue
            if any((source.joinpath(*parts[:index]) / "go.mod").is_file() for index in range(1, len(parts))):
                continue
            name = path + "@" + version + "/" + relative
            if relative == "go.mod":
                archive.writestr(name, module_file)
            else:
                archive.write(file, name)
        license_name = path + "@" + version + "/LICENSE"
        if license_name not in archive.namelist():
            repository = Path(subprocess.check_output(["git", "-C", str(source), "rev-parse", "--show-toplevel"], text=True).strip())
            license_file = repository / "LICENSE"
            if license_file.is_file() and not license_file.is_symlink():
                archive.write(license_file, license_name)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--version", default=None)
    parser.add_argument("--sdk-source", type=Path)
    args = parser.parse_args()
    suffix = str(time.time_ns())
    version = args.version or "v0.0.0-consumer." + suffix
    if not re.fullmatch(r"v\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?", version):
        parser.error("invalid module version")
    with tempfile.TemporaryDirectory(prefix="channel-consumer-") as scratch:
        work = Path(scratch)
        proxy = work / "proxy"
        sdk_version = None
        if args.sdk_source:
            sdk_version = "v0.0.0-consumer." + suffix
            write_module(proxy, SDK, sdk_version, args.sdk_source.resolve())
        write_module(proxy, MODULE, version, ROOT, sdk_version)
        consumer = work / "consumer"
        consumer.mkdir()
        (consumer / "go.mod").write_text("module consumer.example/channel\n\ngo 1.25.0\n\nrequire " + MODULE + " " + version + "\n")
        (consumer / "main.go").write_text((ROOT.parent / "examples/channel_feishu/main.go").read_text())
        env = os.environ.copy()
        cache = Path(subprocess.check_output(["go", "env", "GOMODCACHE"], text=True).strip())
        env["GOWORK"] = "off"
        env["GOMODCACHE"] = str(work / "modcache")
        env["GOPROXY"] = proxy.as_uri() + "," + (cache / "cache/download").as_uri() + ",https://proxy.golang.org"
        local_modules = [MODULE] + ([SDK] if args.sdk_source else [])
        env["GONOSUMDB"] = ",".join(filter(None, [env.get("GONOSUMDB", "")] + local_modules))
        subprocess.run(["go", "mod", "tidy"], cwd=consumer, env=env, check=True)
        data = json.loads(subprocess.check_output(["go", "mod", "edit", "-json"], cwd=consumer, env=env, text=True))
        if data.get("Replace"):
            raise RuntimeError("consumer unexpectedly contains replace directives")
        subprocess.run(["go", "build", "./..."], cwd=consumer, env=env, check=True)
        env["GOBIN"] = str(work / "bin")
        subprocess.run(["go", "install", MODULE + "/cmd/channel-runner@" + version], cwd=work, env=env, check=True)
        print("External consumer and versioned CLI installation passed with GOWORK=off and no replacements.")
        if args.sdk_source:
            print("Root SDK validated from local release candidate; published dependency validation is still required before tagging.")


if __name__ == "__main__":
    main()
