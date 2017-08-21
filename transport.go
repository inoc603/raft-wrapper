package raft

import (
	"context"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/coreos/etcd/etcdserver/stats"
	"github.com/coreos/etcd/pkg/types"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/coreos/etcd/rafthttp"
	"github.com/pkg/errors"
)

type Transport interface {
	Send(messages []raftpb.Message)
	Start() error
	Stop() error
	ChangeConf(cc raftpb.ConfChange) error
	ErrorC() <-chan error
}

type HTTPTransport struct {
	rafthttp.Transport
	server *http.Server
	raft   *RaftNode
	errorC chan error
}

func NewHTTPTransport(r *RaftNode) (Transport, error) {
	t := HTTPTransport{
		Transport: rafthttp.Transport{
			ID:          types.ID(r.id),
			ClusterID:   0x1000,
			Raft:        r,
			ServerStats: stats.NewServerStats("", ""),
			LeaderStats: stats.NewLeaderStats(strconv.Itoa(r.id)),
			ErrorC:      make(chan error),
		},
		raft:   r,
		errorC: make(chan error, 1),
	}
	u, err := url.Parse(string(r.peers[r.id-1].Context))
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse url")
	}

	t.server = &http.Server{
		Handler: t.Transport.Handler(),
		Addr:    u.Host,
	}

	return &t, nil
}

func (t *HTTPTransport) ErrorC() <-chan error {
	return t.errorC
}

func (t *HTTPTransport) Start() error {
	if err := t.Transport.Start(); err != nil {
		return errors.Wrap(err, "failed to start raft Transport")
	}
	for _, peer := range t.raft.peers {
		if peer.ID != uint64(t.raft.id) {
			t.AddPeer(
				types.ID(peer.ID),
				[]string{string(peer.Context)},
			)
		}
	}

	go func() {
		err := t.server.ListenAndServe()
		select {
		case t.errorC <- err:
		default:
		}
	}()

	go func() {
		err := <-t.Transport.ErrorC
		select {
		case t.errorC <- err:
		default:
		}
	}()

	return nil
}

func (t *HTTPTransport) Stop() error {
	t.Transport.Stop()
	ctx, _ := context.WithTimeout(context.Background(), 3*time.Second)
	return errors.Wrap(t.server.Shutdown(ctx), "failed to shutdown http server")
}

func (t *HTTPTransport) ChangeConf(cc raftpb.ConfChange) error {
	switch cc.Type {
	case raftpb.ConfChangeAddNode:
		if len(cc.Context) > 0 {
			t.AddPeer(types.ID(cc.NodeID), []string{string(cc.Context)})
		}
	case raftpb.ConfChangeRemoveNode:
		if cc.NodeID == uint64(t.raft.id) {
			return errors.New("Removed from cluster")
		}
		t.RemovePeer(types.ID(cc.NodeID))
	}
	return nil
}
