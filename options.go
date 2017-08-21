package raft

import (
	"github.com/coreos/etcd/raft"
)

type RaftOption func(r *RaftNode) error

func WithTransport(t func(r *RaftNode) (Transport, error)) RaftOption {
	return func(r *RaftNode) (err error) {
		r.transport, err = t(r)
		return err
	}
}

func WithID(id int) RaftOption {
	return func(r *RaftNode) error {
		r.id = id
		return nil
	}
}

func WithPeers(peers ...string) RaftOption {
	return func(r *RaftNode) error {
		for i, p := range peers {
			r.peers = append(r.peers, raft.Peer{
				ID:      uint64(i + 1),
				Context: []byte(p),
			})
		}
		return nil
	}
}

func WithStorage(m Storage) RaftOption {
	return func(r *RaftNode) error {
		r.storage = m
		return nil
	}
}

func WithStore(store Store) RaftOption {
	return func(r *RaftNode) error {
		r.store = store
		return nil
	}
}
