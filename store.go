package raft

import (
	"os"

	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/coreos/etcd/snap"
	"github.com/coreos/etcd/wal"
	"github.com/coreos/etcd/wal/walpb"
	"github.com/pkg/errors"
)

type Storage interface {
	raft.Storage
	IsEmpty() bool
	Save(rd raft.Ready) error
}

type MemStorage struct {
	raft.MemoryStorage
	wal         *wal.WAL
	snapshotter *snap.Snapshotter
}

func NewMemStorage(walDir, snapDir string) (*MemStorage, error) {
	m := MemStorage{
		MemoryStorage: *raft.NewMemoryStorage(),
	}

	// create snapshotter
	if err := os.MkdirAll(snapDir, 0750); err != nil {
		return nil, errors.Wrapf(err, "cannot create snapshot dir %s", snapDir)
	}
	m.snapshotter = snap.New(snapDir)

	// create wal
	snapshot, err := m.snapshotter.Load()
	if err != nil && err != snap.ErrNoSnapshot {
		return nil, errors.Wrap(err, "failed to load snapshot")
	}

	w, err := openWAL(walDir, snapshot)
	if err != nil {
		return nil, errors.Wrap(err, "failed to open wal")
	}
	m.wal = w
	_, st, ents, err := w.ReadAll()
	if err != nil {
		return nil, errors.Wrap(err, "failed to read wal")
	}
	if snapshot != nil {
		if err := m.ApplySnapshot(*snapshot); err != nil {
			return nil, errors.Wrap(err, "failed to apply snapshot")
		}
	}
	if err := m.SetHardState(st); err != nil {
		return nil, errors.Wrap(err, "failed to apply state")
	}
	if err := m.Append(ents); err != nil {
		return nil, errors.Wrap(err, "failed to append entries")
	}

	return &m, nil
}

func (s *MemStorage) Save(rd raft.Ready) error {
	if err := s.wal.Save(rd.HardState, rd.Entries); err != nil {
		return err
	}

	if err := s.Append(rd.Entries); err != nil {
		return err
	}

	s.needSnapshot()

	// Ignore empty snapshot
	if raft.IsEmptySnap(rd.Snapshot) {
		return nil
	}

	if err := s.ApplySnapshot(rd.Snapshot); err != nil {
		return err
	}

	if err := s.snapshotter.SaveSnap(rd.Snapshot); err != nil {
		return err
	}

	walSnap := walpb.Snapshot{
		Index: rd.Snapshot.Metadata.Index,
		Term:  rd.Snapshot.Metadata.Term,
	}

	if err := s.wal.SaveSnapshot(walSnap); err != nil {
		return err
	}

	return s.wal.ReleaseLockTo(rd.Snapshot.Metadata.Index)
}

func (s *MemStorage) IsEmpty() bool {
	lastIndex, _ := s.MemoryStorage.LastIndex()
	return lastIndex == 0
}

func (s *MemStorage) needSnapshot() bool {
	lastIndex, _ := s.MemoryStorage.LastIndex()
	snap, _ := s.Snapshot()
	return lastIndex-snap.Metadata.Index > 1000
}

func openWAL(dir string, snapshot *raftpb.Snapshot) (*wal.WAL, error) {
	if !wal.Exist(dir) {
		if err := os.MkdirAll(dir, 0750); err != nil {
			return nil, errors.Wrapf(err, "failed to create wal dir")
		}

		w, err := wal.Create(dir, nil)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to create wal")
		}
		if err := w.Close(); err != nil {
			return nil, errors.Wrap(err, "failed to close wal")
		}
	}

	walsnap := walpb.Snapshot{}
	if snapshot != nil {
		walsnap.Index, walsnap.Term = snapshot.Metadata.Index, snapshot.Metadata.Term
	}
	w, err := wal.Open(dir, walsnap)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to load wal")
	}

	return w, nil
}
