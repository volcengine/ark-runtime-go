// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core

// TurnStage 指定执行池可领取的持久阶段；空值保持旧调用方领取全部阶段的行为。
type TurnStage string

const (
	// StageAny 兼容统一工作池。
	StageAny TurnStage = ""
	// StageCreate 推进准备阶段、Session 创建与创建结果对账。
	StageCreate TurnStage = "create"
	// StageSubmit 推进新轮次准备与输入提交。
	StageSubmit TurnStage = "submit"
	// StageObserve 只观察已受理的输入及其恢复。
	StageObserve TurnStage = "observe"
)

// Valid 校验执行池类型。
func (s TurnStage) Valid() bool {
	return s == StageAny || s == StageCreate || s == StageSubmit || s == StageObserve
}

// Accepts 判断执行池是否处理指定的持久阶段。
func (s TurnStage) Accepts(phase TurnPhase) bool {
	switch s {
	case StageAny:
		return true
	case StageCreate:
		return phase == PhaseNone || phase == PhaseCreate
	case StageSubmit:
		return phase == PhaseNone || phase == PhaseSend
	case StageObserve:
		return phase == PhaseObserve
	default:
		return false
	}
}
