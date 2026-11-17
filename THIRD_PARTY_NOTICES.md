# Third-Party Notices

This repository contains code that is derived from or structurally adapted
from third-party open-source projects.

## Anthropic self-hosted worker SDK

Portions of the self-hosted worker lifecycle and local agent tool
implementations under
`arkruntime/selfhosted`, `arkruntime/lib/environments`, `arkruntime/toolset`,
`arkruntime/tools/agenttoolset`, and `mcp` are structurally adapted from
Anthropic's self-hosted worker SDK and client-side MCP helper implementation:

https://github.com/anthropics/anthropic-sdk-go

The upstream project is licensed under the MIT License. The MIT copyright and
permission notice is preserved below as required by that license.

```text
Copyright 2023 Anthropic, PBC.

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```

## Channel module dependencies

The independently versioned `channel/` module is licensed under Apache-2.0;
see [LICENSE](LICENSE). Its third-party components retain their respective
licenses and copyrights.

### Direct dependencies

The following external modules are direct dependencies in [channel/go.mod](channel/go.mod).
Versions and license references correspond to those module releases. Their
source code is downloaded by the Go toolchain and is not vendored in this
module.

| Module | Version | License (SPDX) | Upstream license |
| --- | --- | --- | --- |
| `github.com/go-sql-driver/mysql` | v1.8.1 | MPL-2.0 | [LICENSE](https://github.com/go-sql-driver/mysql/blob/v1.8.1/LICENSE) |
| `github.com/gorilla/websocket` | v1.5.0 | BSD-2-Clause | [LICENSE](https://github.com/gorilla/websocket/blob/v1.5.0/LICENSE) |
| `github.com/larksuite/oapi-sdk-go/v3` | v3.12.0 | MIT | [LICENSE](https://github.com/larksuite/oapi-sdk-go/blob/v3.12.0/LICENSE) |
| `go.etcd.io/bbolt` | v1.5.0 | MIT | [LICENSE](https://github.com/etcd-io/bbolt/blob/v1.5.0/LICENSE) |
| `golang.org/x/sys` | v0.45.0 | BSD-3-Clause | [LICENSE](https://github.com/golang/sys/blob/v0.45.0/LICENSE) |
| `modernc.org/sqlite` | v1.40.1 | BSD-3-Clause | [LICENSE](https://gitlab.com/cznic/sqlite/-/blob/v1.40.1/LICENSE) |

The `modernc.org/sqlite` module also includes SQLite code dedicated to the
public domain. Its [SQLITE-LICENSE](https://gitlab.com/cznic/sqlite/-/blob/v1.40.1/SQLITE-LICENSE)
describes that code separately from the Go implementation's BSD license.

Test code additionally uses `github.com/stretchr/testify` v1.12.1 under the
MIT License; see its [LICENSE](https://github.com/stretchr/testify/blob/v1.12.1/LICENSE).

### Transitive dependencies and redistribution

[channel/go.mod](channel/go.mod) and [channel/go.sum](channel/go.sum) record the dependency versions and
checksums. The table above is an index of direct dependencies, not a complete
license bundle for a compiled application.

When redistributing a binary or vendored sources, include the applicable
license texts, copyright notices and other required notices for the components
actually included, including transitive dependencies. For the MPL-2.0 MySQL
driver, the corresponding source is available at the
[v1.8.1 release](https://github.com/go-sql-driver/mysql/tree/v1.8.1);
redistributed modifications remain subject to that license's source
availability requirements.
