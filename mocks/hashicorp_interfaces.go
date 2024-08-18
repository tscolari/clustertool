package mocks

import raft "github.com/hashicorp/raft"

type applyFuture interface {
	raft.ApplyFuture
}

type indexFuture interface {
	raft.IndexFuture
}

type configurationFuture interface {
	raft.ConfigurationFuture
}
