// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0
package validate

import "testing"

func TestObjectPropertyBounds(t *testing.T) {
	var v Object
	if v.Set() || v.ValidateProperties(100) != nil {
		t.Fatal("unset object should be unconstrained")
	}
	v.SetMinProperties(1)
	v.SetMaxProperties(16)
	if !v.Set() {
		t.Fatal("bounds not set")
	}
	for _, n := range []int{1, 16} {
		if err := v.ValidateProperties(n); err != nil {
			t.Fatal(err)
		}
	}
	for _, n := range []int{0, 17} {
		if v.ValidateProperties(n) == nil {
			t.Fatalf("accepted %d properties", n)
		}
	}
	v.SetMaxProperties(0)
	v.MinPropertiesSet = false
	if v.ValidateProperties(0) != nil || v.ValidateProperties(1) == nil {
		t.Fatal("zero maximum must be enforced")
	}
}
