package raft

import (
	"sync"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
)

type RaftNode struct {
	id   int
	stop chan struct{}

	transport Transport

	storage Storage

	node raft.Node

	peers  []raft.Peer
	leader int

	proposeLock sync.Mutex

	store Store
}

func NewRaftNode(opts ...RaftOption) (*RaftNode, error) {
	r := RaftNode{}

	for _, opt := range opts {
		if err := opt(&r); err != nil {
			return nil, errors.Wrap(err, "failed to apply option")
		}
	}

	return &r, nil
}

func (r *RaftNode) Stop() error {
	select {
	case r.stop <- struct{}{}:
		return nil
	default:
		return errors.New("raft node is not started")
	}
}

func (r *RaftNode) Propose(data []byte) error {
	r.proposeLock.Lock()
	defer r.proposeLock.Unlock()
	ctx, _ := context.WithTimeout(context.Background(), 3*time.Second)
	return errors.Wrap(r.node.Propose(ctx, data), "propose failed")
}

func (r *RaftNode) ProposeConfChange(cc raftpb.ConfChange) error {
	r.proposeLock.Lock()
	defer r.proposeLock.Unlock()
	ctx, _ := context.WithTimeout(context.Background(), 3*time.Second)
	return errors.Wrap(r.node.ProposeConfChange(ctx, cc), "propose conf change failed")
}

func (r *RaftNode) Start() error {
	cfg := raft.Config{
		ID:              uint64(r.id),
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         r.storage,
		MaxSizePerMsg:   1024 * 1024,
		MaxInflightMsgs: 256,
	}

	if r.storage.IsEmpty() {
		r.node = raft.StartNode(&cfg, r.peers)
	} else {
		r.node = raft.RestartNode(&cfg)
	}
	r.stop = make(chan struct{})

	if err := r.transport.Start(); err != nil {
		return errors.Wrap(err, "failed to start transport")
	}

	defer func() {
		close(r.stop)
		r.stop = nil
		if err := r.transport.Stop(); err != nil {
			logrus.WithError(err).Errorln("Failed to stop transport")
		}
		r.node.Stop()
	}()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			r.node.Tick()
		case rd := <-r.node.Ready():
			// #1 Write HardState, Entries, and Snapshot to persistent storage if they are not empty
			if err := r.storage.Save(rd); err != nil {
				return err
			}

			// #2 Send all Messages to the nodes named in the To field
			r.transport.Send(rd.Messages)

			// #3 Apply Snapshot (if any) and CommittedEntries to the state machine
			for _, entry := range rd.CommittedEntries {
				switch entry.Type {
				case raftpb.EntryNormal:
					if len(entry.Data) == 0 {
						break
					}
					if err := r.store.Commit(entry.Data); err != nil {
						return err
					}
				case raftpb.EntryConfChange:
					var cc raftpb.ConfChange
					if err := cc.Unmarshal(entry.Data); err != nil {
						return err
					}
					r.node.ApplyConfChange(cc)
					if err := r.transport.ChangeConf(cc); err != nil {
						return err
					}
				}
			}

			// #4 Call Node.Advance() to signal readiness for the next batch of updates
			r.node.Advance()

			r.leader = int(r.node.Status().Lead)
		case err := <-r.transport.ErrorC():
			return errors.Wrap(err, "transport error")
		case <-r.stop:
			return nil
		}
	}
}

func (r *RaftNode) Process(ctx context.Context, m raftpb.Message) error {
	return r.node.Step(ctx, m)
}

func (r *RaftNode) IsIDRemoved(id uint64) bool {
	return false
}

func (r *RaftNode) ReportUnreachable(id uint64) {
	logrus.WithField("id", id).Errorln("Unreachable")
}

func (r *RaftNode) ReportSnapshot(id uint64, status raft.SnapshotStatus) {
	logrus.WithField("id", id).Errorln("Snapshot")
}

func init() {
	raft.SetLogger(logrus.StandardLogger().WithField("source", "raft"))
}
