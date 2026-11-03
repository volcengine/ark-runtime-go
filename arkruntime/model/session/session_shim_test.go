// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"encoding/json"
	"strings"
	"testing"
)

const nullArchivedAtSessionWire = `{
	"id":"sess-1",
	"type":"session",
	"status":"running",
	"environment_id":"env-1",
	"agent":{
		"id":"agent-1",
		"type":"agent",
		"archived_at":null,
		"tools":[{
			"type":"custom",
			"input_schema":{
				"type":"object",
				"properties":{
					"large":{"const":9007199254740993},
					"payload":{"default":{"type":"agent","archived_at":null}}
				}
			}
		}]
	},
	"created_at":"2026-10-23T10:00:00Z",
	"updated_at":"2026-10-23T10:00:00Z",
	"archived_at":null,
	"environment":{
		"id":"env-1",
		"type":"environment",
		"name":"environment-name",
		"config":{
			"type":"self_hosted",
			"networking":{"type":"unrestricted"},
			"default":{"type":"environment","archived_at":null}
		},
		"created_at":"2026-10-23T10:00:00Z",
		"updated_at":"2026-10-23T10:00:00Z",
		"scene":"default",
		"archived_at":null
	},
	"scene":"default",
	"stats":{"active_seconds":0,"duration_seconds":0}
}`

func TestSessionResponseNormalizesOnlyResourceArchivedAt(t *testing.T) {
	var response SessionResponse
	if err := json.Unmarshal([]byte(nullArchivedAtSessionWire), &response); err != nil {
		t.Fatal(err)
	}
	if response.ArchivedAt.Set {
		t.Fatal("top-level null archived_at was retained")
	}
	if response.Environment.Set {
		if _, ok := response.Environment.Value["archived_at"]; ok {
			t.Fatal("environment null archived_at was retained")
		}
		if config := string(response.Environment.Value["config"]); !strings.Contains(config, `"archived_at":null`) {
			t.Fatalf("nested environment config changed: %s", config)
		}
	}
	tools := string(response.Agent["tools"])
	if !strings.Contains(tools, `9007199254740993`) {
		t.Fatalf("large integer changed: %s", tools)
	}
	if !strings.Contains(tools, `"archived_at":null`) {
		t.Fatalf("nested user JSON changed: %s", tools)
	}
}

func TestListSessionsResponseWrapperAcceptsNullArchivedAt(t *testing.T) {
	wire := `{"data":[` + nullArchivedAtSessionWire + `,{
		"id":"sess-2",
		"type":"session",
		"status":"running",
		"environment_id":"env-2",
		"agent":{"id":"agent-2","type":"agent","tools":[]},
		"created_at":"2026-10-23T10:00:00Z",
		"updated_at":"2026-10-23T10:00:00Z",
		"scene":"default",
		"stats":{"active_seconds":0,"duration_seconds":0}
	}],"next_page":"next"}`
	var response ListSessionsResponseWrapper
	if err := json.Unmarshal([]byte(wire), &response); err != nil {
		t.Fatal(err)
	}
	if len(response.Data) != 2 || response.Data[0].ID != "sess-1" || response.Data[1].ID != "sess-2" {
		t.Fatalf("sessions = %+v", response.Data)
	}
	if next, ok := response.NextPage.Get(); !ok || next != "next" {
		t.Fatalf("next_page = %q, set=%v", next, ok)
	}
}
