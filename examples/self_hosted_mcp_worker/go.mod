module github.com/volcengine/ark-runtime-go/examples/self_hosted_mcp_worker

go 1.23.0

require (
	github.com/modelcontextprotocol/go-sdk v1.3.1
	github.com/volcengine/ark-runtime-go v0.6.0
	github.com/volcengine/ark-runtime-go/mcp v0.6.0
)

require (
	github.com/go-faster/errors v0.7.1 // indirect
	github.com/go-faster/jx v1.2.0 // indirect
	github.com/google/jsonschema-go v0.4.2 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/segmentio/asm v1.2.1 // indirect
	github.com/segmentio/encoding v0.5.3 // indirect
	github.com/volcengine/volc-sdk-golang v1.0.23 // indirect
	github.com/volcengine/volcengine-go-sdk v1.2.15 // indirect
	github.com/yosida95/uritemplate/v3 v3.0.2 // indirect
	golang.org/x/oauth2 v0.30.0 // indirect
	golang.org/x/sys v0.35.0 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
)

replace github.com/volcengine/ark-runtime-go => ../..

replace github.com/volcengine/ark-runtime-go/mcp => ../../mcp
