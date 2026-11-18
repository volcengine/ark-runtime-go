// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package embedded

import core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"

func recallSourcePinned(tx storeTx, messageID string) (bool, error) {
	message, err := getJSON[core.InboundMessage](tx, bucketMessages, []byte(messageID))
	if err != nil || message == nil || message.Message.RecalledMessageID == "" {
		return false, err
	}
	original := tx.Bucket(bucketMsgByProviderID).Get(key([]byte(message.BindingID), []byte(message.Message.RecalledMessageID)))
	return original != nil, nil
}
