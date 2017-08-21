package raft

import (
	"fmt"
)

type DummyStore struct {
	data []byte
}

func (s *DummyStore) Commit(data []byte) error {
	fmt.Println("[dummy] commit: ", string(data))
	s.data = append(s.data, data...)
	return nil
}

func (s *DummyStore) LoadSnapshot(data []byte) error {
	s.data = data
	return nil
}

func (s *DummyStore) GetSnapshot() ([]byte, error) {
	return s.data, nil
}
