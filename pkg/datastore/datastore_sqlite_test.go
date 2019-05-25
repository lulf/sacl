/*
 * Copyright 2019, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package datastore

import (
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestInitialize(t *testing.T) {
	os.Remove("init.db")
	ds, err := NewSqliteDatastore("init.db", 2)
	defer ds.Close()
	assert.Nil(t, err)
	assert.NotNil(t, ds)

	_, err = ds.handle.Exec("SELECT events FROM sqlite_master WHERE type='table'")
	assert.NotNil(t, err)

	err = ds.Initialize()
	assert.Nil(t, err)

	_, err = ds.handle.Exec("SELECT events FROM sqlite_master WHERE type='table'")
	assert.NotNil(t, err)
}

func TestInsertNewEntry(t *testing.T) {
	os.Remove("insert.db")
	ds, err := NewSqliteDatastore("insert.db", 2)
	defer ds.Close()
	assert.Nil(t, err)
	assert.NotNil(t, ds)

	err = ds.InsertNewEntry(1, 2, "dev1", "payload1")
	assert.NotNil(t, err)

	err = ds.Initialize()
	assert.Nil(t, err)
	err = ds.InsertNewEntry(1, 1, "dev1", "payload1")
	assert.Nil(t, err)

	count, err := countEntries(t, ds)
	assert.Nil(t, err)
	assert.Equal(t, 1, count)

	err = ds.InsertNewEntry(2, 2, "dev2", "payload2")
	assert.Nil(t, err)
	count, err = countEntries(t, ds)
	assert.Nil(t, err)
	assert.Equal(t, 2, count)

	err = ds.InsertNewEntry(3, 3, "dev2", "payload3")
	assert.Nil(t, err)
	count, err = countEntries(t, ds)
	assert.Nil(t, err)
	assert.Equal(t, 3, count)

	err = ds.InsertNewEntry(4, 4, "dev2", "payload4")
	assert.Nil(t, err)
	count, err = countEntries(t, ds)
	assert.Nil(t, err)
	assert.Equal(t, 3, count)
}

func countEntries(t *testing.T, ds *SqlDatastore) (int, error) {
	var count int
	row := ds.handle.QueryRow("SELECT COUNT(id) FROM events")
	assert.NotNil(t, row)
	err := row.Scan(&count)
	return count, err
}
