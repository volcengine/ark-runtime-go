// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

// End-to-end agent-loop example — Create Agent + Environment + Session,
// send a text prompt, stream events until the loop settles (status_idle /
// terminated / error), and print the assistant's response.
//
// This is the smallest full agent-loop demo — it exercises:
//
//	POST   /api/v3/agents          (CreateAgent)
//	POST   /api/v3/environments    (CreateEnvironment)
//	POST   /api/v3/sessions        (CreateSession)
//	POST   /api/v3/sessions/:id/events         (SendSessionEvents — user.message)
//	GET    /api/v3/sessions/:id/events (stream) (StreamSessionEvents — SSE)
//
//	export ARK_API_KEY=...
//	export ARK_MODEL_ID=doubao-seed-2-1-pro-260628
//	go run examples/sessions_loop/main.go
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/volcengine/ark-runtime-go/arkruntime"
	"github.com/volcengine/ark-runtime-go/arkruntime/model/agent"
	"github.com/volcengine/ark-runtime-go/arkruntime/model/environment"
	"github.com/volcengine/ark-runtime-go/arkruntime/model/session"
)

func main() {
	apiKey := os.Getenv("ARK_API_KEY")
	if apiKey == "" {
		log.Fatal("set ARK_API_KEY")
	}
	modelID := os.Getenv("ARK_MODEL_ID")
	if modelID == "" {
		modelID = "${YOUR_MODEL_ID}"
	}

	client := arkruntime.NewClientWithApiKey(apiKey)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	// 1. Agent — general-purpose with the toolset that ships with managed agents.
	ag, err := client.CreateAgent(ctx, &agent.CreateAgentRequest{
		Name:  fmt.Sprintf("example-loop-agent-%d", time.Now().UnixNano()),
		Model: agent.ModelConfig{ID: modelID},
		System: agent.NewOptString(
			"You are a helpful assistant. Answer the user's question briefly."),
		Tools: []agent.ToolItem{{Type: "agent_toolset_20260401"}},
	})
	if err != nil {
		log.Fatalf("create agent: %v", err)
	}
	defer func() { _, _ = client.DeleteAgent(context.Background(), ag.ID) }()
	fmt.Printf("agent:      id=%s\n", ag.ID)

	// 2. Environment — cloud + unrestricted networking.
	env, err := client.CreateEnvironment(ctx, &environment.CreateEnvironmentRequest{
		Name: fmt.Sprintf("example-loop-env-%d", time.Now().UnixNano()),
		Config: environment.NewOptEnvConfig(environment.EnvConfig{
			Type: environment.EnvConfigTypeCloud,
			Networking: environment.NewOptNetworkingConfig(environment.NetworkingConfig{
				Type: environment.NetworkingTypeUnrestricted,
			}),
		}),
	})
	if err != nil {
		log.Fatalf("create environment: %v", err)
	}
	defer func() { _, _ = client.DeleteEnvironment(context.Background(), env.ID) }()
	fmt.Printf("env:        id=%s\n", env.ID)

	// 3. Session — binds the agent to the environment.
	sess, err := client.CreateSession(ctx, &session.CreateSessionRequest{
		Agent:         session.NewStringAgentIdentifier(ag.ID),
		EnvironmentID: env.ID,
		Title:         session.NewOptString("ark-runtime-go example loop"),
	})
	if err != nil {
		log.Fatalf("create session: %v", err)
	}
	defer func() { _, _ = client.DeleteSession(context.Background(), sess.ID) }()
	fmt.Printf("session:    id=%s\n\n", sess.ID)

	// 4. Open the SSE stream first, then send the user message so we don't
	//    race and miss the earliest events.
	dec, err := client.StreamSessionEvents(ctx, sess.ID)
	if err != nil {
		log.Fatalf("open stream: %v", err)
	}
	defer dec.Close()

	// Fire the user.message asynchronously; the stream will surface both
	// the echoed user.message and the assistant's agent.message frames.
	go func() {
		// Small warmup so the SSE reader is fully attached before we push.
		time.Sleep(500 * time.Millisecond)
		if _, err := client.SendSessionEvents(ctx, sess.ID, &session.SendSessionEventsRequest{
			Events: []session.ManagedAgentsEventParams{{
				OneOf: session.NewManagedAgentsUserMessageEventParamsManagedAgentsEventParamsSum(
					session.ManagedAgentsUserMessageEventParams{
						Content: []session.ManagedAgentsMessageContentBlock{{
							OneOf: session.NewManagedAgentsTextBlockManagedAgentsMessageContentBlockSum(
								session.ManagedAgentsTextBlock{
									Text: "What's the tallest mountain? One sentence.",
								}),
						}},
					}),
			}},
		}); err != nil {
			log.Printf("send user.message: %v", err)
		}
	}()

	// 5. Drain the stream until the loop settles. session.status_idle is the
	//    normal terminal event; terminated/error are the failure modes. Every
	//    frame is delivered as a concrete typed struct — dispatch via a Go
	//    type-switch instead of hand-parsing JSON.
	var assistantOut strings.Builder
	done := false
	for !done && dec.Next() {
		frame := dec.Event()
		fmt.Printf("[EVT] %s\n", frame.Type)

		switch ev := frame.Data.(type) {
		case *session.ManagedAgentsAgentMessageEvent:
			for _, block := range ev.Content {
				if block.Text != "" {
					assistantOut.WriteString(block.Text)
				}
			}
		case *session.ManagedAgentsSessionStatusIdleEvent,
			*session.ManagedAgentsSessionStatusTerminatedEvent,
			*session.ManagedAgentsSessionErrorEvent:
			_ = ev // terminal — see printed [EVT] type above
			done = true
		}
	}

	if s := strings.TrimSpace(assistantOut.String()); s != "" {
		fmt.Printf("\nassistant → %s\n", s)
	} else {
		fmt.Println("\n(no assistant text captured — check the [EVT] trace above)")
	}
}
