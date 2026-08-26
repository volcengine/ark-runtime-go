// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/volcengine/ark-runtime-go/arkruntime"
	"github.com/volcengine/ark-runtime-go/arkruntime/model/file"
)

func main() {
	target := flag.String("file", "", "path to file to upload")
	flag.Parse()
	if *target == "" {
		log.Fatal("usage: main -file <path>")
	}

	apiKey := os.Getenv("ARK_API_KEY")
	if apiKey == "" {
		log.Fatal("set ARK_API_KEY")
	}

	client := arkruntime.NewByteplusClientWithApiKey(apiKey)
	ctx := context.Background()

	f, err := os.Open(*target)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	uploaded, err := client.UploadFile(ctx, &file.FileCreateRequest{
		Purpose: file.PurposeUserData,
	}, f)
	if err != nil {
		log.Fatalf("upload: %v", err)
	}
	fmt.Printf("uploaded: id=%s status=%s\n", uploaded.ID, uploaded.Status)

	ready, err := client.WaitForFileProcessing(ctx, uploaded.ID, arkruntime.WaitForFileProcessingOptions{})
	if err != nil {
		log.Fatalf("wait: %v", err)
	}
	fmt.Printf("ready: status=%s\n", ready.Status)

	listed, err := client.ListFiles(ctx, &file.FilesListParams{})
	if err != nil {
		log.Fatalf("list: %v", err)
	}
	fmt.Printf("list: %d items, has_more=%v\n", len(listed.Data), listed.HasMore)

	deleted, err := client.DeleteFile(ctx, uploaded.ID)
	if err != nil {
		log.Fatalf("delete: %v", err)
	}
	fmt.Printf("deleted: id=%s deleted=%v\n", deleted.ID, deleted.Deleted)
}
