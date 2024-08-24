package fsm

import (
	"bytes"
	"testing"

	"github.com/google/uuid"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

type snapshotSink struct {
	*bytes.Buffer
}

func (s *snapshotSink) ID() string {
	return uuid.NewString()
}

func (s *snapshotSink) Cancel() error {
	return nil
}

func (s *snapshotSink) Close() error {
	return nil
}

func TestHash(t *testing.T) {
	store1 := NewHashStore()

	result := store1.Apply(stringToLog(`{"key1":"value1", "key2":"value2"}`))
	require.Nil(t, result)

	value, exists := store1.Get("key1")
	require.True(t, exists)
	require.Equal(t, "value1", value)

	value, exists = store1.Get("key2")
	require.True(t, exists)
	require.Equal(t, "value2", value)

	result = store1.Apply(stringToLog(`{"key1":"newValue1", "key3":"value3"}`))
	require.Nil(t, result)

	value, exists = store1.Get("key1")
	require.True(t, exists)
	require.Equal(t, "newValue1", value)

	value, exists = store1.Get("key2")
	require.True(t, exists)
	require.Equal(t, "value2", value)

	value, exists = store1.Get("key3")
	require.True(t, exists)
	require.Equal(t, "value3", value)

	fsmSnapshot, err := store1.Snapshot()
	require.NoError(t, err)

	snapshotBuffer := &snapshotSink{
		bytes.NewBuffer([]byte{}),
	}

	require.NoError(t, fsmSnapshot.Persist(snapshotBuffer))

	store2 := NewHashStore()
	require.NoError(t, store2.Restore(snapshotBuffer))

	require.EqualValues(t, store1.store, store2.store)

}

func stringToLog(data string) *raft.Log {
	return &raft.Log{Data: []byte(data)}
}
