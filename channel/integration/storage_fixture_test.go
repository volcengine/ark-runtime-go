// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core_test

import (
	"context"
	"database/sql"
	"os"

	"github.com/volcengine/ark-runtime-go/channel/internal/handles"
	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
	sqlstore "github.com/volcengine/ark-runtime-go/channel/store/sql"

	"github.com/volcengine/ark-runtime-go/channel/store/embedded"
)

func openEmbeddedFixture(path string, options embedded.Options) (testStore, error) {
	if _, err := os.Stat(path); os.IsNotExist(err) && !options.ReadOnly {
		if err = embedded.Install(path, options); err != nil {
			return nil, err
		}
	}
	return openEmbeddedStore(path, options)
}

type testStore interface {
	core.Store
	core.Exporter
}

func openEmbeddedStore(path string, options embedded.Options) (testStore, error) {
	handle, err := embedded.Open(path, options)
	if err != nil {
		return nil, err
	}
	return handles.StoreValue(handle).(testStore), nil
}

func openSQLStore(ctx context.Context, db *sql.DB, dialect sqlstore.Dialect, options sqlstore.Options) (core.Store, error) {
	handle, err := sqlstore.Open(ctx, db, dialect, options)
	if err != nil {
		return nil, err
	}
	return handles.StoreValue(handle), nil
}
