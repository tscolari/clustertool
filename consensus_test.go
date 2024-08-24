package clustertool_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/hashicorp/raft"
	mock "github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	. "github.com/tscolari/clustertool"
	"github.com/tscolari/clustertool/mocks"
)

func TestConsensus_Name(t *testing.T) {
	consensus, _, _ := testConsensus(t, "name-1")
	require.Equal(t, "name-1", consensus.Name())
}

func TestConsensus_Address(t *testing.T) {
	consensus, _, _ := testConsensus(t, "name-1")
	require.Equal(t, "127.0.0.1:7001", consensus.Address())
}

func TestConsensus_Apply(t *testing.T) {
	consensus, raftMock, _ := testConsensus(t, "name-1")

	cmd := []byte("hello")
	timeout := time.Second

	future := mocks.NewApplyFuture(t)

	raftMock.
		On("Apply", cmd, timeout).
		Twice().
		Return(future)

	future.On("Error").Once().Return(nil)

	require.NoError(t, consensus.Apply(cmd, timeout))

	t.Run("when the future returns an error", func(t *testing.T) {
		future.On("Error").Once().Return(errors.New("oh no"))

		err := consensus.Apply(cmd, timeout)
		require.Error(t, err)
		require.ErrorContains(t, err, "oh no")
	})
}

func TestConsensus_IsLeader(t *testing.T) {
	consensus, _, notifyCh := testConsensus(t, "name-1")

	notifyCh <- false
	require.Eventually(t, func() bool {
		return !consensus.IsLeader()
	}, 150*time.Millisecond, 100*time.Microsecond)

	notifyCh <- true
	require.Eventually(t, func() bool {
		return consensus.IsLeader()
	}, 150*time.Millisecond, 100*time.Microsecond)
}

func TestConsensus_Leader(t *testing.T) {
	consensus, raftMock, _ := testConsensus(t, "name-1")

	raftMock.
		On("LeaderWithID").
		Once().
		Return(raft.ServerAddress("0.0.0.0:1"), raft.ServerID("cool-node"))

	address, id := consensus.Leader()
	require.Equal(t, "0.0.0.0:1", address)
	require.Equal(t, "cool-node", id)
}

func TestConsensus_AddNode(t *testing.T) {
	consensus, raftMock, _ := testConsensus(t, "name-1")

	future := mocks.NewIndexFuture(t)

	raftMock.
		On("AddVoter", raft.ServerID("cool-node"), raft.ServerAddress("0.0.0.0:1"), uint64(0), time.Duration(0)).
		Twice().
		Return(future)

	future.On("Error").Once().Return(nil)
	require.NoError(t, consensus.AddNode("cool-node", "0.0.0.0:1"))

	t.Run("when future returns an error", func(t *testing.T) {
		future.On("Error").Once().Return(errors.New("oh no"))
		err := consensus.AddNode("cool-node", "0.0.0.0:1")
		require.Error(t, err)
		require.ErrorContains(t, err, "oh no")
	})
}

func TestConsensus_DemoteNode(t *testing.T) {
	consensus, raftMock, _ := testConsensus(t, "name-1")

	future := mocks.NewIndexFuture(t)

	raftMock.
		On("AddNonvoter", raft.ServerID("cool-node"), raft.ServerAddress("0.0.0.0:1"), uint64(0), time.Duration(0)).
		Twice().
		Return(future)

	future.On("Error").Once().Return(nil)
	require.NoError(t, consensus.DemoteNode("cool-node", "0.0.0.0:1"))

	t.Run("when future returns an error", func(t *testing.T) {
		future.On("Error").Once().Return(errors.New("oh no"))
		err := consensus.DemoteNode("cool-node", "0.0.0.0:1")
		require.Error(t, err)
		require.ErrorContains(t, err, "oh no")
	})
}

func TestConsensus_RemoveNode(t *testing.T) {
	consensus, raftMock, _ := testConsensus(t, "name-1")

	future := mocks.NewIndexFuture(t)

	raftMock.
		On("RemoveServer", raft.ServerID("cool-node"), uint64(0), time.Duration(0)).
		Twice().
		Return(future)

	future.On("Error").Once().Return(nil)
	require.NoError(t, consensus.RemoveNode("cool-node"))

	t.Run("when future returns an error", func(t *testing.T) {
		future.On("Error").Once().Return(errors.New("oh no"))
		err := consensus.RemoveNode("cool-node")
		require.Error(t, err)
		require.ErrorContains(t, err, "oh no")
	})
}

func TestConsensus_Nodes(t *testing.T) {
	consensus, raftMock, _ := testConsensus(t, "name-1")

	future := mocks.NewConfigurationFuture(t)

	raftMock.
		On("GetConfiguration").
		Twice().
		Return(future)

	future.On("Error").Once().Return(nil)

	returnedServers := []raft.Server{
		{ID: raft.ServerID("1")}, {ID: raft.ServerID("2")},
	}

	future.
		On("Configuration").
		Once().
		Return(raft.Configuration{
			Servers: returnedServers,
		})

	nodes, err := consensus.Nodes()
	require.NoError(t, err)
	require.Len(t, nodes, 2)
	require.ElementsMatch(t, returnedServers, nodes)

	t.Run("when future returns an error", func(t *testing.T) {
		future.On("Error").Once().Return(errors.New("oh no"))
		_, err := consensus.Nodes()
		require.Error(t, err)
		require.ErrorContains(t, err, "oh no")
	})
}

func TestConsensus_Stop(t *testing.T) {
	consensus, raftMock, notifyCh := testConsensus(t, "name-1")
	notifyCh <- false

	require.Eventually(t, func() bool {
		return !consensus.IsLeader()
	}, 100*time.Millisecond, 10*time.Microsecond)

	shutdownFuture := mocks.NewIndexFuture(t)
	raftMock.
		On("Shutdown").
		Once().
		Return(shutdownFuture)

	shutdownFuture.On("Error").Once().Return(nil)

	select {
	case <-consensus.Done():
		require.Fail(t, "node should not be done yet")
	case <-time.After(time.Millisecond):
	}

	require.NoError(t, consensus.Stop())

	select {
	case <-consensus.Done():
	case <-time.After(time.Millisecond):
		require.Fail(t, "node should have been done already")
	}

	t.Run("when stopped is called multiple times at once", func(t *testing.T) {
		consensus, raftMock, notifyCh := testConsensus(t, "name-1")
		notifyCh <- false

		wg := new(sync.WaitGroup)
		wg.Add(1)

		shutdownFuture := mocks.NewIndexFuture(t)
		raftMock.
			On("Shutdown").
			Run(func(args mock.Arguments) {
				// Ensure this call blocks so we can test parallel calls to Stop.
				wg.Wait()
			}).
			Once().
			Return(shutdownFuture)

		shutdownFuture.On("Error").Once().Return(nil)

		call1Ch := make(chan error)
		go func() {
			call1Ch <- consensus.Stop()
		}()

		select {
		case <-call1Ch:
			require.Fail(t, "first call to Stop should not have returned yet")
		case <-time.After(time.Millisecond):
		}

		call2Ch := make(chan error)
		go func() {
			call2Ch <- consensus.Stop()
		}()

		select {
		case <-call2Ch:
			require.Fail(t, "second call to Stop should not have returned yet")
		case <-time.After(time.Millisecond):
		}

		wg.Done()

		select {
		case <-call1Ch:
		case <-time.After(time.Millisecond):
			require.Fail(t, "first call to Stop should have returned")
		}

		select {
		case <-call2Ch:
		case <-time.After(time.Millisecond):
			require.Fail(t, "second call to Stop should have returned")
		}
	})

	t.Run("when raft's Shutdown fail", func(t *testing.T) {
		consensus, raftMock, notifyCh := testConsensus(t, "name-1")
		notifyCh <- false

		shutdownFuture := mocks.NewIndexFuture(t)
		raftMock.
			On("Shutdown").
			Once().
			Return(shutdownFuture)

		shutdownFuture.On("Error").Once().Return(errors.New("oh no"))

		err := consensus.Stop()
		require.Error(t, err)
		require.ErrorContains(t, err, "oh no")

		t.Run("repeated calls to Stop will give the same error", func(t *testing.T) {
			err := consensus.Stop()
			require.Error(t, err)
			require.ErrorContains(t, err, "oh no")

			err = consensus.Stop()
			require.Error(t, err)
			require.ErrorContains(t, err, "oh no")
		})
	})

	t.Run("when the node is the leader", func(t *testing.T) {
		consensus, raftMock, notifyCh := testConsensus(t, "name-1")
		// Ensure it's the leader.
		notifyCh <- true

		require.Eventually(t, func() bool {
			return consensus.IsLeader()
		}, 100*time.Millisecond, 10*time.Microsecond)

		leadershipFuture := mocks.NewIndexFuture(t)
		raftMock.
			On("LeadershipTransfer").
			Once().
			Return(leadershipFuture)

		leadershipFuture.On("Error").Once().Return(nil)

		shutdownFuture := mocks.NewIndexFuture(t)
		raftMock.
			On("Shutdown").
			Once().
			Return(shutdownFuture)

		shutdownFuture.On("Error").Once().Return(nil)
		require.NoError(t, consensus.Stop())
	})
}

func testConsensus(t *testing.T, name string) (Consensus, *mocks.HashicorpRaft, chan<- bool) {
	fsm := mocks.NewFSM(t)
	config := DefaultConsensusConfig()
	config.Addr = "127.0.0.1:7001"
	config.DataDir = path.Join(os.TempDir(), uuid.NewString())
	config.Raft.Logger = nil
	config.Raft.LogOutput = io.Discard

	consensus, err := NewConsensus(
		context.Background(),
		slog.Default(),
		name,
		fsm,
		config,
	)

	require.NoError(t, err)

	raft := mocks.NewHashicorpRaft(t)
	TestAttachMockToConsensus(t, consensus, raft)

	return consensus, raft, config.Raft.NotifyCh
}
