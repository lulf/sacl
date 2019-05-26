/*
 * Copyright 2019, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package datastore

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"testing"
)

func tempDbFile(t *testing.T, prefix string) string {
	f, err := ioutil.TempFile("", fmt.Sprintf("%s.*.db", prefix))
	assert.Nil(t, err)
	assert.NotNil(t, f)
	return f.Name()
}

func TestInitialize(t *testing.T) {
	f := tempDbFile(t, "init")
	ds, err := NewSqliteDatastore(f, 2)
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

func TestInsertEvent(t *testing.T) {
	f := tempDbFile(t, "insert")
	ds, err := NewSqliteDatastore(f, 2)
	defer ds.Close()
	assert.Nil(t, err)
	assert.NotNil(t, ds)

	err = ds.InsertEvent(NewEvent(1, 1, 2, "dev1", "payload1"))
	assert.NotNil(t, err)

	err = ds.Initialize()
	assert.Nil(t, err)
	err = ds.InsertEvent(NewEvent(1, 1, 1, "dev1", "payload1"))
	assert.Nil(t, err)

	count, err := countEntries(t, ds)
	assert.Nil(t, err)
	assert.Equal(t, 1, count)

	err = ds.InsertEvent(NewEvent(2, 2, 2, "dev2", "payload2"))
	assert.Nil(t, err)
	count, err = countEntries(t, ds)
	assert.Nil(t, err)
	assert.Equal(t, 2, count)

	err = ds.InsertEvent(NewEvent(3, 3, 3, "dev2", "payload3"))
	assert.Nil(t, err)
	count, err = countEntries(t, ds)
	assert.Nil(t, err)
	assert.Equal(t, 3, count)

	err = ds.InsertEvent(NewEvent(4, 4, 4, "dev2", "payload4"))
	assert.Nil(t, err)
	count, err = countEntries(t, ds)
	assert.Nil(t, err)
	assert.Equal(t, 3, count)
}

func TestListEvents(t *testing.T) {
	f := tempDbFile(t, "listevents")
	ds, err := NewSqliteDatastore(f, 6)
	defer ds.Close()
	assert.NotNil(t, ds)

	ds.Initialize()
	ds.InsertEvent(NewEvent(1, 1, 1, "dev1", "payload1"))
	ds.InsertEvent(NewEvent(2, 2, 2, "dev1", "payload2"))
	ds.InsertEvent(NewEvent(3, 3, 3, "dev1", "payload3"))
	ds.InsertEvent(NewEvent(4, 4, 4, "dev1", "payload4"))

	lst, err := ds.ListEvents(-1, 0)
	assert.Nil(t, err)
	assert.Equal(t, 4, len(lst))

	lst, err = ds.ListEvents(-1, 2)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(lst))

	lst, err = ds.ListEvents(1, 2)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(lst))
}

func TestNumEvents(t *testing.T) {
	f := tempDbFile(t, "numevents")
	ds, err := NewSqliteDatastore(f, 6)
	defer ds.Close()
	assert.NotNil(t, ds)

	ds.Initialize()
	count, err := ds.NumEvents()
	assert.Nil(t, err)
	assert.Equal(t, 0, count)
	ds.InsertEvent(NewEvent(1, 1, 1, "dev1", "payload1"))
	count, err = ds.NumEvents()
	assert.Equal(t, 1, count)
	ds.InsertEvent(NewEvent(2, 2, 2, "dev1", "payload2"))
	ds.InsertEvent(NewEvent(3, 3, 3, "dev1", "payload3"))
	count, err = ds.NumEvents()
	assert.Equal(t, 3, count)
	ds.InsertEvent(NewEvent(4, 4, 4, "dev1", "payload4"))
	count, err = ds.NumEvents()
	assert.Equal(t, 4, count)
}

func countEntries(t *testing.T, ds *SqlDatastore) (int, error) {
	var count int
	row := ds.handle.QueryRow("SELECT COUNT(id) FROM events")
	assert.NotNil(t, row)
	err := row.Scan(&count)
	return count, err
}
