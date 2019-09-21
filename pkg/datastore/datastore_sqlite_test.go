/*
 * Copyright 2019, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package datastore

import (
	"fmt"
	"github.com/lulf/sacl/pkg/api"
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

func TestCreateTopic(t *testing.T) {
	f := tempDbFile(t, "init")
	ds, err := NewSqliteDatastore(f, 0, 0)
	defer ds.Close()
	assert.Nil(t, err)
	assert.NotNil(t, ds)

	_, err = ds.handle.Exec("SELECT mytopic FROM sqlite_master WHERE type='table'")
	assert.NotNil(t, err)

	_, err = ds.CreateTopic("mytopic")
	assert.Nil(t, err)

	_, err = ds.handle.Exec("SELECT mytopic FROM sqlite_master WHERE type='table'")
	assert.NotNil(t, err)
}

func TestInsertMessage(t *testing.T) {
	f := tempDbFile(t, "insert")
	ds, err := NewSqliteDatastore(f, 3, 0)
	defer ds.Close()
	assert.Nil(t, err)
	assert.NotNil(t, ds)

	_, err = ds.CreateTopic("mytopic")
	assert.Nil(t, err)

	err = ds.InsertMessage("mytopic", api.NewMessage(1, []byte("payload1")))
	assert.Nil(t, err)

	count, err := countEntries(t, ds)
	assert.Nil(t, err)
	assert.Equal(t, 1, count)

	err = ds.InsertMessage("mytopic", api.NewMessage(2, []byte("payload2")))
	assert.Nil(t, err)
	count, err = countEntries(t, ds)
	assert.Nil(t, err)
	assert.Equal(t, 2, count)

	err = ds.InsertMessage("mytopic", api.NewMessage(3, []byte("payload3")))
	assert.Nil(t, err)
	count, err = countEntries(t, ds)
	assert.Nil(t, err)
	assert.Equal(t, 3, count)

	err = ds.InsertMessage("mytopic", api.NewMessage(4, []byte("payload4")))
	assert.Nil(t, err)
	count, err = countEntries(t, ds)
	assert.Nil(t, err)
	assert.Equal(t, 4, count)
}

func TestListMessages(t *testing.T) {
	f := tempDbFile(t, "listevents")
	ds, err := NewSqliteDatastore(f, 6, 0)
	defer ds.Close()
	assert.NotNil(t, ds)

	ds.CreateTopic("mytopic")
	ds.InsertMessage("mytopic", api.NewMessage(1, []byte("payload1")))
	ds.InsertMessage("mytopic", api.NewMessage(2, []byte("payload2")))
	ds.InsertMessage("mytopic", api.NewMessage(3, []byte("payload3")))
	ds.InsertMessage("mytopic", api.NewMessage(4, []byte("payload4")))

	lst, err := ds.ListMessages("mytopic", -1, 0)
	assert.Nil(t, err)
	assert.Equal(t, 4, len(lst))

	lst, err = ds.ListMessages("mytopic", -1, 2)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(lst))

	lst, err = ds.ListMessages("mytopic", 1, 2)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(lst))
}

func TestNumMessages(t *testing.T) {
	f := tempDbFile(t, "numevents")
	ds, err := NewSqliteDatastore(f, 6, 0)
	defer ds.Close()
	assert.NotNil(t, ds)

	ds.CreateTopic("mytopic")
	count, err := ds.NumMessages("mytopic")
	assert.Nil(t, err)
	assert.Equal(t, 0, int(count))
	ds.InsertMessage("mytopic", api.NewMessage(1, []byte("payload1")))
	count, err = ds.NumMessages("mytopic")
	assert.Equal(t, 1, int(count))
	ds.InsertMessage("mytopic", api.NewMessage(2, []byte("payload2")))
	ds.InsertMessage("mytopic", api.NewMessage(3, []byte("payload3")))
	count, err = ds.NumMessages("mytopic")
	assert.Equal(t, 3, int(count))
	ds.InsertMessage("mytopic", api.NewMessage(4, []byte("payload4")))
	count, err = ds.NumMessages("mytopic")
	assert.Equal(t, 4, int(count))
	count, err = ds.NumMessages("unknown")
	assert.Equal(t, 0, int(count))
}

func countEntries(t *testing.T, ds *SqlDatastore) (int, error) {
	var count int
	row := ds.handle.QueryRow("SELECT COUNT(id) FROM mytopic")
	assert.NotNil(t, row)
	err := row.Scan(&count)
	return count, err
}
