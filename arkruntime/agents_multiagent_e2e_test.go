//go:build e2e

package arkruntime_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/volcengine/ark-runtime-go/arkruntime"
	"github.com/volcengine/ark-runtime-go/arkruntime/model/agent"
)

func TestMultiagentClientGateAgainstSTG(t *testing.T) {
	token := os.Getenv("ARK_STG_TOKEN")
	if token == "" {
		t.Fatal("ARK_STG_TOKEN is required for Managed Agents E2E")
	}
	baseURL := os.Getenv("ARK_E2E_BASE_URL")
	if baseURL == "" {
		t.Fatal("ARK_E2E_BASE_URL is required for Managed Agents E2E")
	}
	modelID := os.Getenv("ARK_E2E_MODEL_ID")
	if modelID == "" {
		modelID = "doubao-seed-1-6-250615"
	}

	ctx := context.Background()
	client := arkruntime.NewClientWithApiKey(token,
		arkruntime.WithBaseUrl(baseURL),
		arkruntime.WithRetryTimes(0),
	)
	created, err := client.CreateAgent(ctx, &agent.CreateAgentRequest{
		Name:  fmt.Sprintf("sdk-gate-go-%d", time.Now().UnixNano()),
		Model: agent.ModelConfig{ID: modelID},
		Multiagent: agent.NewOptMultiagentConfig(agent.MultiagentConfig{
			Type: agent.MultiagentTypeMultiagent20261001,
			Subagents: agent.NewOptMultiagentSubagentsConfig(agent.MultiagentSubagentsConfig{
				Type:             agent.MultiagentFeatureTypeEnabled,
				PredefinedAgents: agent.NewOptNilAgentRefArray([]agent.AgentRef{{Type: agent.AgentRefTypeSelf}}),
			}),
		}),
	})
	if err != nil {
		t.Fatalf("create dynamic agent: %v", err)
	}
	t.Cleanup(func() {
		if _, err := client.DeleteAgent(context.Background(), created.ID); err != nil {
			t.Logf("delete agent %s: %v", created.ID, err)
		}
	})

	if _, err := client.GetAgent(ctx, created.ID); err != nil {
		t.Fatalf("get dynamic agent: %v", err)
	}
	updated, err := client.UpdateAgent(ctx, created.ID, &agent.UpdateAgentRequest{
		Version: created.Version,
		Multiagent: agent.NewOptMultiagentPatch(agent.MultiagentPatch{
			Type: agent.MultiagentTypeMultiagent20261001,
			Subagents: agent.NewOptNilMultiagentSubagentsConfig(agent.MultiagentSubagentsConfig{
				Type:             agent.MultiagentFeatureTypeEnabled,
				PredefinedAgents: agent.NewOptNilAgentRefArray([]agent.AgentRef{}),
			}),
		}),
	})
	if err != nil {
		t.Fatalf("update dynamic agent: %v", err)
	}
	if updated.Version <= created.Version {
		t.Fatalf("update version = %d, want > %d", updated.Version, created.Version)
	}

	listed, err := client.ListAgents(ctx, &agent.AgentsListParams{
		Limit: agent.NewOptInt32(100),
	})
	if err != nil {
		t.Fatalf("list agents: %v", err)
	}
	found := false
	for _, item := range listed.Data {
		if item.ID == created.ID {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("created agent %s not present in list", created.ID)
	}

	legacyUA := arkruntime.WithCustomHeader("User-Agent", "ark-runtime-go/0.11.0")
	if _, err := client.GetAgent(ctx, created.ID, legacyUA); err == nil ||
		!strings.Contains(err.Error(), "MULTIAGENT_UNSUPPORTED_BY_CLIENT") {
		t.Fatalf("legacy get error = %v", err)
	}
	if _, err := client.UpdateAgent(ctx, created.ID, &agent.UpdateAgentRequest{
		Version:     updated.Version,
		Description: agent.NewOptString("must be rejected"),
	}, legacyUA); err == nil || !strings.Contains(err.Error(), "MULTIAGENT_UNSUPPORTED_BY_CLIENT") {
		t.Fatalf("legacy update error = %v", err)
	}

	legacyList, err := client.ListAgents(ctx, &agent.AgentsListParams{
		Limit: agent.NewOptInt32(100),
	}, legacyUA)
	if err != nil {
		t.Fatalf("legacy list: %v", err)
	}
	found = false
	for _, item := range legacyList.Data {
		if item.ID != created.ID {
			continue
		}
		found = true
		if item.Multiagent.IsSet() {
			t.Fatal("legacy list returned multiagent_20261001")
		}
	}
	if !found {
		t.Fatalf("created agent %s not present in legacy list", created.ID)
	}
}
