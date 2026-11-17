#!/usr/bin/env python3
# Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
# SPDX-License-Identifier: Apache-2.0

"""确保 core 单测仅依赖领域与抽象 ports。"""
import json
import subprocess
import sys

packages = subprocess.check_output(['go', 'list', '-deps', '-test', './internal/runtime'], text=True).splitlines()
module = 'github.com/volcengine/ark-runtime-go/channel/'
blocked_prefixes = ('store', 'internal/storage', 'internal/testutil', 'internal/conformance', 'ma', 'provider', 'runner')
blocked = [p for p in packages if any(p == module + prefix or p.startswith(module + prefix + '/') for prefix in blocked_prefixes) or p.startswith(('modernc.org/', 'go.etcd.io/bbolt', 'github.com/larksuite/'))]

# testify/assert 自身间接依赖 httptest；只禁止 core 测试直接引入真实 HTTP harness。
metadata = json.loads(subprocess.check_output(['go', 'list', '-json', './internal/runtime'], text=True))
if 'net/http/httptest' in metadata.get('TestImports', []) + metadata.get('XTestImports', []):
    blocked.append('net/http/httptest (direct core test import)')
if blocked:
    print('core test dependency boundary violated:\n' + '\n'.join(blocked), file=sys.stderr)
    sys.exit(1)
print('core test dependency boundary passed')
