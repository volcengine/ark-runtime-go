// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core

import (
	"context"
	"errors"
)

// environmentSelfHosted 是平台自托管沙箱环境类型；与身份 env 注入互斥（无平台沙箱可挂载凭据）。
const environmentSelfHosted = "self_hosted"

// environmentPlan 是冻结到 SessionSnapshot 的环境决策：
//   - baseType 永远来自一次 base Environment 读取（FileMounts 需要它判断 self_hosted 跳过），
//     没有 EnvironmentID 或端口缺失时为空；
//   - override 非空表示以 environment_with_overrides 创建；nil 表示走 environment_id。
type environmentPlan struct {
	baseType string
	override *EnvironmentOverride
}

// environmentPlan 在创建 Session 前读取 base environment，合并身份沙箱 env，
// 返回冻结后的环境决策。
func (r *Runtime) environmentPlan(ctx context.Context, connection Connection, j *job, extras SessionExtras) (environmentPlan, error) {
	sandbox := SandboxEnvFor(j.binding, j.msg.ChatType)
	hookEnv := extras.Env
	// 附件也需要冻结环境类型，才能在关闭挂载时区分 cloud 说明与 self_hosted 下载链接。
	files := j.binding.FileMounts || len(j.msg.Attachments) > 0
	if len(sandbox) == 0 && len(hookEnv) == 0 && (!files || j.binding.EnvironmentID == "") {
		return environmentPlan{}, nil
	}
	if j.binding.EnvironmentID == "" {
		// 没有基础 Environment 却要注入身份 env：当前契约要求有 env 承载（self_hosted 检查也依赖它）。
		if len(sandbox) > 0 {
			return environmentPlan{}, errors.New("identity env requires an environment")
		}
		// 纯 Hook env 在未绑定 Environment 时无法发送 override；无 env 的挂载也无法判断类型。
		return environmentPlan{}, nil
	}
	reader, ok := connection.MAClient.(EnvironmentReader)
	if !ok {
		return environmentPlan{}, errors.New("ma client does not implement EnvironmentReader")
	}
	base, err := reader.GetEnvironment(ctx, j.binding.EnvironmentID)
	if err != nil {
		return environmentPlan{}, NewPreparationError(err, definitiveReject(err))
	}
	plan := environmentPlan{baseType: base.Type}
	if len(sandbox) == 0 && len(hookEnv) == 0 {
		// 只为挂载冻结类型；不改变创建请求（仍走 environment_id）。
		return plan, nil
	}
	if base.Type == environmentSelfHosted {
		// self_hosted 沙箱不受平台编排，无法保证身份 env 生效，明确失败而不是静默忽略。
		return environmentPlan{}, errors.New("identity env unsupported on self_hosted environment")
	}
	if base.Type != "cloud" {
		return environmentPlan{}, errors.New("unsupported environment config type")
	}
	merged := map[string]string{}
	for k, v := range base.Env {
		merged[k] = v
	}
	for k, v := range hookEnv {
		merged[k] = v
	}
	overriddenKeys := []string{}
	for k, v := range sandbox {
		if existing, exists := merged[k]; exists && existing != v {
			overriddenKeys = append(overriddenKeys, k)
		}
		merged[k] = v
	}
	if len(overriddenKeys) > 0 {
		r.report(ctx, Observation{Name: EventAuth, Status: "env_overridden", BindingID: j.BindingID, TurnID: j.ID})
	}
	plan.override = &EnvironmentOverride{Type: base.Type, Env: merged}
	return plan, nil
}
