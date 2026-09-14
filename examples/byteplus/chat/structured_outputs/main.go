// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

// Structured outputs example: drives chat.completions with a
// response_format=json_schema, then unmarshals the model's response
// straight into a typed Go struct.
//
// Run with:
//
//	cd examples && ARK_API_KEY=... go run ./byteplus/chat/structured_outputs
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/invopop/jsonschema" // requires go1.18+

	"github.com/volcengine/ark-runtime-go/arkruntime"
	"github.com/volcengine/ark-runtime-go/arkruntime/model/chat"
)

// HistoricalComputer is the typed schema we want the model to produce.
type HistoricalComputer struct {
	Origin       Origin   `json:"origin" jsonschema_description:"The origin of the computer"`
	Name         string   `json:"full_name" jsonschema_description:"The name of the device model"`
	Legacy       string   `json:"legacy" jsonschema:"enum=positive,enum=neutral,enum=negative" jsonschema_description:"Its influence on the field of computing"`
	NotableFacts []string `json:"notable_facts" jsonschema_description:"A few key facts about the computer"`
}

type Origin struct {
	YearBuilt    int64  `json:"year_of_construction" jsonschema_description:"The year it was made"`
	Organization string `json:"organization" jsonschema_description:"The organization that was in charge of its development"`
}

// generateSchema reflects T into a JSON Schema object.
func generateSchema[T any]() *jsonschema.Schema {
	reflector := jsonschema.Reflector{
		AllowAdditionalProperties: false,
		DoNotReference:            true,
	}
	var v T
	return reflector.Reflect(v)
}

var historicalComputerSchema = generateSchema[HistoricalComputer]()

func main() {
	client := arkruntime.NewByteplusClientWithApiKey(os.Getenv("ARK_API_KEY"))
	ctx := context.Background()

	question := "What computer ran the first neural network?"
	fmt.Printf("> %s\n", question)

	req := &chat.ChatCompletionRequest{
		Model: "seed-2-0-pro-260328",
		Messages: []chat.ChatCompletionRequestMessage{
			{
				OneOf: chat.NewChatCompletionRequestUserMessageChatCompletionRequestMessageSum(
					chat.ChatCompletionRequestUserMessage{
						Role:    chat.ChatCompletionRequestUserMessageRoleUser,
						Content: chat.NewOptNilChatCompletionMessageContent(chat.NewStringChatCompletionMessageContent(question)),
					},
				),
			},
		},
	}

	schemaJSON, err := json.Marshal(historicalComputerSchema)
	if err != nil {
		panic(err)
	}
	js := chat.ChatCompletionResponseFormatJsonSchema{
		Name:   "historical_computer",
		Schema: schemaJSON,
	}
	js.Description.SetTo("Information about a historical computer")
	js.Strict.SetTo(true)
	rf := chat.ChatCompletionResponseFormat{}
	rf.Type.SetTo(chat.ResponseFormatTypeJSONSchema)
	rf.JSONSchema.SetTo(js)
	req.ResponseFormat.SetTo(rf)

	resp, err := client.CreateChatCompletion(ctx, req)
	if err != nil {
		fmt.Printf("structured output chat error: %v\n", err)
		return
	}
	if len(resp.Choices) == 0 {
		fmt.Println("no choices in response")
		return
	}

	// The model responds with a JSON string; parse it into our typed struct.
	var computer HistoricalComputer
	if err := json.Unmarshal([]byte(resp.Choices[0].Message.Content), &computer); err != nil {
		panic(err)
	}

	fmt.Printf("Name: %v\n", computer.Name)
	fmt.Printf("Year: %v\n", computer.Origin.YearBuilt)
	fmt.Printf("Org: %v\n", computer.Origin.Organization)
	fmt.Printf("Legacy: %v\n", computer.Legacy)
	fmt.Printf("Facts:\n")
	for i, fact := range computer.NotableFacts {
		fmt.Printf("%d. %s\n", i+1, fact)
	}
}
