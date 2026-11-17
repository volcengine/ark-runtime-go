// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

// Package embedded 提供单进程独占的 bbolt Store。
//
// 每个实体一个主 bucket（id → JSON），另有一组有序二级索引 bucket，与 SQL 实现的列和索引
// 一一对应。领取只遍历“就绪索引”（只含队首且按可领取时间排序），统计只遍历未结算索引，
// 清理按结算时间索引范围扫描，因此这些操作的开销与历史数据量无关。
package embedded
