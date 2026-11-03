package agent

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestMultiagentConfigRejectsNullSubagents(t *testing.T) {
	var config MultiagentConfig
	err := json.Unmarshal([]byte(`{"type":"multiagent_20261001","subagents":null}`), &config)
	if err == nil {
		t.Fatal("multiagent config accepted explicit null subagents")
	}
}

func TestUpdateMultiagentPatchPreservesSubagentsPresence(t *testing.T) {
	tests := []struct {
		name      string
		wire      string
		wantSet   bool
		wantNull  bool
		wantTop   bool
		wantType  MultiagentFeatureType
		wantValid bool
	}{
		{
			name:      "omitted",
			wire:      `{"version":1,"multiagent":{"type":"multiagent_20261001"}}`,
			wantSet:   false,
			wantValid: true,
		},
		{
			name:      "top-level null",
			wire:      `{"version":1,"multiagent":null}`,
			wantSet:   true,
			wantTop:   true,
			wantValid: true,
		},
		{
			name:      "explicit null",
			wire:      `{"version":1,"multiagent":{"type":"multiagent_20261001","subagents":null}}`,
			wantSet:   true,
			wantNull:  true,
			wantValid: true,
		},
		{
			name:      "object",
			wire:      `{"version":1,"multiagent":{"type":"multiagent_20261001","subagents":{"type":"enabled"}}}`,
			wantSet:   true,
			wantType:  MultiagentFeatureTypeEnabled,
			wantValid: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var request UpdateAgentRequest
			if err := json.Unmarshal([]byte(tt.wire), &request); err != nil {
				t.Fatal(err)
			}
			if err := request.Validate(); (err == nil) != tt.wantValid {
				t.Fatalf("Validate() error = %v, wantValid = %v", err, tt.wantValid)
			}
			if request.Multiagent.IsNull() != tt.wantTop {
				t.Fatalf("top-level null = %v, want %v", request.Multiagent.IsNull(), tt.wantTop)
			}
			if tt.wantTop {
				encoded, err := json.Marshal(&request)
				if err != nil {
					t.Fatal(err)
				}
				if string(encoded) != tt.wire {
					t.Fatalf("wire changed: want %s, got %s", tt.wire, encoded)
				}
				return
			}
			patch, ok := request.Multiagent.Get()
			if !ok {
				t.Fatal("multiagent patch was not decoded")
			}
			if patch.Subagents.Set != tt.wantSet || patch.Subagents.Null != tt.wantNull {
				t.Fatalf("subagents presence = set:%v null:%v, want set:%v null:%v",
					patch.Subagents.Set, patch.Subagents.Null, tt.wantSet, tt.wantNull)
			}
			if tt.wantSet && !tt.wantNull && patch.Subagents.Value.Type != tt.wantType {
				t.Fatalf("subagents type = %q, want %q", patch.Subagents.Value.Type, tt.wantType)
			}

			encoded, err := json.Marshal(&request)
			if err != nil {
				t.Fatal(err)
			}
			var want, got any
			if err := json.Unmarshal([]byte(tt.wire), &want); err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(encoded, &got); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(want, got) {
				t.Fatalf("wire changed: want %s, got %s", tt.wire, encoded)
			}
		})
	}
}

func TestUpdateMultiagentPatchCanSetTopLevelNull(t *testing.T) {
	request := UpdateAgentRequest{Version: 1}
	request.Multiagent.SetToNull()
	encoded, err := json.Marshal(&request)
	if err != nil {
		t.Fatal(err)
	}
	if string(encoded) != `{"version":1,"multiagent":null}` {
		t.Fatalf("wire = %s", encoded)
	}
	if err := request.Validate(); err != nil {
		t.Fatal(err)
	}
}
