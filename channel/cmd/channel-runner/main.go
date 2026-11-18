// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

// Command channel-runner 提供离线配置验证、Bot 运行与只读状态查询。
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/volcengine/ark-runtime-go/channel/runner"
)

const usage = `usage: channel-runner <command> --config PATH [flags]

commands:
  validate                       offline configuration check
  install                        explicitly install new storage (never overwrite an existing local file)
  expand-user-auth               explicitly add User auth structure to existing encrypted storage (runner stopped)
  run                            start the runner
  status                         read-only task/mapping/delivery projection (runner stopped)
  doctor [--online]              check secrets, database and (optionally) MA/Feishu credentials
  reset --conversation ROUTE_KEY  next message of the conversation starts a new session (runner stopped)
  retry-delivery --id ID         requeue a failed delivery without re-running the agent (runner stopped)
  prune                          delete settled records older than runtime.retention (runner stopped)`

var commands = map[string]bool{"validate": true, "install": true, "expand-user-auth": true, "run": true, "status": true, "doctor": true, "reset": true, "retry-delivery": true, "prune": true}

func main() { os.Exit(execute(os.Args[1:], os.Stdout, os.Stderr)) }

func execute(args []string, stdout, stderr io.Writer) int {
	if len(args) == 1 && (args[0] == "help" || args[0] == "--help") {
		_, _ = fmt.Fprintln(stdout, usage)
		return 0
	}
	if len(args) == 0 || !commands[args[0]] {
		_, _ = fmt.Fprintln(stderr, usage)
		return 2
	}
	command := args[0]
	flags := flag.NewFlagSet(command, flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	path := flags.String("config", "", "runner JSON configuration")
	conversation := flags.String("conversation", "", "conversation (route) key for reset")
	id := flags.String("id", "", "delivery id for retry-delivery")
	online := flags.Bool("online", false, "doctor probes MA and Feishu credentials")
	if err := flags.Parse(args[1:]); err != nil {
		if err == flag.ErrHelp {
			_, _ = fmt.Fprintln(stdout, usage)
			return 0
		}
		_, _ = fmt.Fprintln(stderr, usage)
		return 2
	}
	if *path == "" || flags.NArg() != 0 || command == "reset" && *conversation == "" || command == "retry-delivery" && *id == "" {
		_, _ = fmt.Fprintln(stderr, usage)
		return 2
	}
	config, err := runner.Load(*path)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, err)
		return 1
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	switch command {
	case "validate":
		_, _ = fmt.Fprintln(stdout, "config valid (offline)")
		return 0
	case "install":
		if err := runner.Install(ctx, config); err != nil {
			_, _ = fmt.Fprintln(stderr, err)
			return 1
		}
		_, _ = fmt.Fprintln(stdout, "storage installed")
		return 0
	case "expand-user-auth":
		if err := runner.ExpandUserAuthStorage(ctx, config); err != nil {
			_, _ = fmt.Fprintln(stderr, err)
			return 1
		}
		_, _ = fmt.Fprintln(stdout, "user auth storage expanded")
		return 0
	case "status":
		status, err := runner.Status(ctx, config)
		if err != nil {
			_, _ = fmt.Fprintln(stderr, err)
			return 1
		}
		if err := json.NewEncoder(stdout).Encode(status); err != nil {
			_, _ = fmt.Fprintln(stderr, "status output failed")
			return 1
		}
		return 0
	case "doctor":
		checks := runner.Doctor(ctx, config, runner.DoctorOptions{Online: *online})
		code := 0
		for _, check := range checks {
			mark := "ok  "
			if !check.OK {
				mark, code = "FAIL", 1
			}
			_, _ = fmt.Fprintf(stdout, "%s %s: %s\n", mark, check.Name, check.Detail)
		}
		return code
	case "reset":
		reset, err := runner.ResetConversation(ctx, config, *conversation)
		return printResult(stdout, stderr, reset, err)
	case "retry-delivery":
		delivery, err := runner.RetryDelivery(ctx, config, *id)
		return printResult(stdout, stderr, delivery, err)
	case "prune":
		removed, err := runner.Prune(ctx, config)
		return printResult(stdout, stderr, removed, err)
	case "run":
		if err := runner.Run(ctx, config); err != nil {
			_, _ = fmt.Fprintln(stderr, err)
			return 1
		}
		return 0
	default:
		_, _ = fmt.Fprintln(stderr, usage)
		return 2
	}
}

func printResult(stdout, stderr io.Writer, value any, err error) int {
	if err != nil {
		_, _ = fmt.Fprintln(stderr, err)
		return 1
	}
	if err := json.NewEncoder(stdout).Encode(value); err != nil {
		_, _ = fmt.Fprintln(stderr, "output failed")
		return 1
	}
	return 0
}
