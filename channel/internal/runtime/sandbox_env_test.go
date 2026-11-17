// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIdentityPolicyValid(t *testing.T) {
	require.NoError(t, (IdentityPolicy{}).Valid())
	require.NoError(t, (IdentityPolicy{Group: IdentityBot, P2P: IdentityBot}).Valid())
	require.NoError(t, (IdentityPolicy{Group: IdentityBot}).Valid())
	require.NoError(t, (IdentityPolicy{P2P: IdentityUser}).Valid())
	require.Error(t, (IdentityPolicy{Group: IdentityUser}).Valid(), "group identity can never be user")
	require.Error(t, (IdentityPolicy{Group: "weird"}).Valid())
}

func TestSandboxEnvFor(t *testing.T) {
	binding := Binding{AppID: "cli_app", Identity: IdentityPolicy{Group: IdentityBot, P2P: IdentityBot}}

	group := SandboxEnvFor(binding, "group")
	if group[EnvLarkSuiteStrictMode] != "bot" || group[EnvLarkSuiteDefaultAs] != "bot" ||
		group[EnvLarkSuiteAppID] != "cli_app" || group[EnvLarkSuiteBrand] != "feishu" {
		t.Fatalf("unexpected group env: %#v", group)
	}
	topic := SandboxEnvFor(binding, "topic")
	if topic[EnvLarkSuiteStrictMode] != "bot" {
		t.Fatalf("topic must inherit bot: %#v", topic)
	}
	p2pNone := SandboxEnvFor(Binding{AppID: "x", Identity: IdentityPolicy{Group: IdentityBot}}, "p2p")
	if p2pNone != nil {
		t.Fatalf("p2p without p2p identity must inject nothing, got %#v", p2pNone)
	}
	empty := SandboxEnvFor(Binding{AppID: "x"}, "p2p")
	if empty != nil {
		t.Fatalf("no identity must inject nothing, got %#v", empty)
	}
}

type sandboxEnvironmentMA struct {
	MAClient
	env EnvironmentConfig
}

func (m sandboxEnvironmentMA) GetEnvironment(context.Context, string) (EnvironmentConfig, error) {
	return m.env, nil
}

func TestEnvironmentPlanReservedKeysWin(t *testing.T) {
	base := map[string]string{
		"CUSTOMER_TOKEN":       "customer",
		"HOOK_VALUE":           "base",
		EnvLarkSuiteAppID:      "customer-app",
		EnvLarkSuiteStrictMode: "user",
	}
	options, err := (RuntimeOptions{}).Normalize()
	require.NoError(t, err)
	runtime := &Runtime{options: options}
	j := &job{
		binding: Binding{AppID: "channel-app", EnvironmentID: "env", Identity: IdentityPolicy{Group: IdentityBot}},
		msg:     Message{ChatType: "group"},
	}
	plan, err := runtime.environmentPlan(context.Background(), Connection{
		MAClient: sandboxEnvironmentMA{env: EnvironmentConfig{Type: "cloud", Env: base}},
	}, j, SessionExtras{Env: map[string]string{"HOOK_VALUE": "hook"}})
	require.NoError(t, err)
	require.NotNil(t, plan.override)
	require.Equal(t, "cloud", plan.baseType)
	require.Equal(t, "cloud", plan.override.Type)
	require.Equal(t, map[string]string{
		"CUSTOMER_TOKEN":       "customer",
		"HOOK_VALUE":           "hook",
		EnvLarkSuiteAppID:      "channel-app",
		EnvLarkSuiteStrictMode: "bot",
		EnvLarkSuiteDefaultAs:  "bot",
		EnvLarkSuiteBrand:      "feishu",
	}, plan.override.Env)
	require.Equal(t, "customer-app", base[EnvLarkSuiteAppID], "base environment must remain unchanged")
	require.Equal(t, "user", base[EnvLarkSuiteStrictMode])
	require.Equal(t, "base", base["HOOK_VALUE"])
}

func TestValidateExtrasRejectsReservedEnv(t *testing.T) {
	require.Error(t, validateExtras(SessionExtras{Env: map[string]string{EnvLarkSuiteAppID: "x"}}))
	require.NoError(t, validateExtras(SessionExtras{Env: map[string]string{"CUSTOM_VAR": "x"}}))
	require.NoError(t, validateExtras(SessionExtras{VaultIDs: []string{"v1"}, Tags: map[string]string{"k": "v"}}))
}
