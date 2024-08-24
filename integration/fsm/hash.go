package fsm

import (
	"encoding/json"
	"io"
	"sync"

	"github.com/hashicorp/raft"
)

func NewHashStore() *HashStore {
	return &HashStore{
		lock:  new(sync.RWMutex),
		store: map[string]string{},
	}
}

type HashStore struct {
	lock  *sync.RWMutex
	store map[string]string
}

func (h *HashStore) Apply(entry *raft.Log) interface{} {
	h.lock.Lock()
	defer h.lock.Unlock()

	newData := map[string]string{}
	if err := json.Unmarshal(entry.Data, &newData); err != nil {
		return err
	}

	for key, value := range newData {
		h.store[key] = value
	}

	return nil
}

func (h *HashStore) Snapshot() (raft.FSMSnapshot, error) {
	return raft.FSMSnapshot(h), nil
}

func (h *HashStore) Restore(snapshot io.ReadCloser) error {
	h.lock.Lock()
	defer h.lock.Unlock()
	defer snapshot.Close()

	err := json.NewDecoder(snapshot).Decode(&h.store)
	if err != nil {
		return err
	}

	return nil
}

func (h *HashStore) Persist(sink raft.SnapshotSink) error {
	h.lock.Lock()
	defer h.lock.Unlock()

	err := json.NewEncoder(sink).Encode(h.store)
	if err != nil {
		return err
	}

	return nil
}

func (h *HashStore) Release() {
}

func (h *HashStore) Get(key string) (string, bool) {
	h.lock.RLock()
	defer h.lock.RUnlock()

	value, ok := h.store[key]
	return value, ok
}

func (h *HashStore) Lengh() int {
	h.lock.RLock()
	defer h.lock.RUnlock()

	return len(h.store)
}
