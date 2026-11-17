// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package sqlstore

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

type metadataErrorState struct {
	err     error
	writes  int
	queries []string
}
type metadataErrorConnector struct{ state *metadataErrorState }

func (c metadataErrorConnector) Connect(context.Context) (driver.Conn, error) {
	return metadataErrorConn(c), nil
}
func (metadataErrorConnector) Driver() driver.Driver { return metadataErrorDriver{} }

type metadataErrorDriver struct{}

func (metadataErrorDriver) Open(string) (driver.Conn, error) {
	return nil, errors.New("unexpected driver open")
}

type metadataErrorConn struct{ state *metadataErrorState }

func (metadataErrorConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("unexpected prepare")
}
func (metadataErrorConn) Close() error              { return nil }
func (metadataErrorConn) Begin() (driver.Tx, error) { return nil, errors.New("unexpected transaction") }
func (c metadataErrorConn) QueryContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	c.state.queries = append(c.state.queries, query)
	if c.state.err != nil {
		return nil, c.state.err
	}
	if strings.Contains(query, "FROM information_schema.tables") {
		return &metadataMissingRows{}, nil
	}
	return nil, errors.New("unexpected metadata query")
}

type metadataMissingRows struct{ read bool }

func (*metadataMissingRows) Columns() []string { return []string{"count"} }
func (*metadataMissingRows) Close() error      { return nil }
func (r *metadataMissingRows) Next(values []driver.Value) error {
	if r.read {
		return io.EOF
	}
	r.read = true
	values[0] = int64(0)
	return nil
}
func (c metadataErrorConn) ExecContext(context.Context, string, []driver.NamedValue) (driver.Result, error) {
	c.state.writes++
	return nil, errors.New("unexpected schema write")
}

func TestMetadataReadFailureIsNotMisclassifiedAsMissingSchema(t *testing.T) {
	for _, failure := range []error{errors.New("transport unavailable"), context.Canceled, &mysql.MySQLError{Number: 1142, Message: "permission denied"}} {
		t.Run(strings.ReplaceAll(failure.Error(), " ", "_"), func(t *testing.T) {
			state := &metadataErrorState{err: failure}
			db := sql.OpenDB(metadataErrorConnector{state: state})
			defer func() { require.NoError(t, db.Close()) }()
			_, err := open(t.Context(), db, MySQL, Options{})
			require.ErrorIs(t, err, failure)
			require.NotErrorIs(t, err, errSchema)
			require.Zero(t, state.writes, "an unavailable metadata read never authorizes ddl")
			require.NotEmpty(t, state.queries)
			for _, query := range state.queries {
				require.Contains(t, query, "FROM information_schema.tables")
			}
		})
	}
}
func TestMissingMetadataReportedByInformationSchemaIsSchemaError(t *testing.T) {
	state := &metadataErrorState{}
	db := sql.OpenDB(metadataErrorConnector{state: state})
	defer func() { require.NoError(t, db.Close()) }()
	_, err := open(t.Context(), db, MySQL, Options{})
	require.ErrorIs(t, err, errSchema)
	require.Zero(t, state.writes)
	require.NotEmpty(t, state.queries)
	for _, query := range state.queries {
		require.Contains(t, query, "FROM information_schema.tables")
	}
}
func TestMySQLIdentifierPrefixLimitMatchesGeneratedDDL(t *testing.T) {
	prefix := strings.Repeat("p", 31)
	for _, ddl := range ddl(MySQL, prefix) {
		fields := strings.Fields(ddl)
		for i, word := range fields {
			if word == "TABLE" {
				i += 4
				if len(fields) > i {
					require.LessOrEqual(t, len(fields[i]), 64, ddl)
				}
			}
			if word == "INDEX" && len(fields) > i+1 {
				require.LessOrEqual(t, len(fields[i+1]), 64, ddl)
			}
		}
	}
	state := &metadataErrorState{err: errors.New("database should not be queried")}
	db := sql.OpenDB(metadataErrorConnector{state: state})
	defer func() { require.NoError(t, db.Close()) }()
	_, err := open(t.Context(), db, MySQL, Options{TablePrefix: strings.Repeat("p", 32)})
	require.ErrorIs(t, err, core.ErrInvalid)
}
