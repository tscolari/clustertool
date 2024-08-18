package clustertool_test

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	time "time"

	raft "github.com/hashicorp/raft"
	serf "github.com/hashicorp/serf/serf"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	. "github.com/tscolari/clustertool"
	"github.com/tscolari/clustertool/mocks"
)

func TestNode_Initialization(t *testing.T) {
	consensus, discovery, fsm := nodeTestMocks(t, "test-node")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	logger := slog.Default()

	node, err := NewNode(ctx, logger, discovery, consensus, fsm)
	require.NoError(t, err)

	t.Cleanup(func() {
		node.Stop()
	})

	t.Run("when names don't match", func(t *testing.T) {
		discovery := mocks.NewDiscovery(t)
		discovery.On("Name").Return("name-1")
		consensus := mocks.NewConsensus(t)
		consensus.On("Name").Return("name-2")

		logger := slog.Default()

		_, err := NewNode(ctx, logger, discovery, consensus, fsm)
		require.Error(t, err)
	})
}

func TestNode_Apply(t *testing.T) {
	consensus, discovery, fsm := nodeTestMocks(t, "test-node")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	logger := slog.Default()

	consensus.On("IsLeader").Once().Return(true)

	node, err := NewNode(ctx, logger, discovery, consensus, fsm)
	require.NoError(t, err)
	t.Cleanup(func() {
		node.Stop()
	})

	msg := []byte("hello world")
	timeout := 10 * time.Second

	consensus.
		On("Apply", msg, timeout).
		Once().
		Return(nil)

	require.NoError(t, node.Apply(msg, timeout))

	t.Run("when consensus fails to apply", func(t *testing.T) {
		consensus.
			On("Apply", msg, timeout).
			Once().
			Return(errors.New("failed"))

		consensus.On("IsLeader").Once().Return(true)

		require.Error(t, node.Apply(msg, timeout))
	})

	t.Run("when not the leader", func(t *testing.T) {
		msg := []byte("good bye")
		timeout := 10 * time.Second

		defer consensus.AssertNotCalled(t, "Apply", msg, timeout)
		consensus.On("IsLeader").Once().Return(false)

		require.Error(t, node.Apply(msg, timeout))
	})
}

func TestNode_IsLeader(t *testing.T) {
	consensus, discovery, fsm := nodeTestMocks(t, "test-node")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	logger := slog.Default()

	node, err := NewNode(ctx, logger, discovery, consensus, fsm)
	require.NoError(t, err)
	t.Cleanup(func() {
		node.Stop()
	})

	consensus.
		On("IsLeader").
		Once().
		Return(true)

	require.True(t, node.IsLeader())
}

func TestNode_ConnectToNode(t *testing.T) {
	consensus, discovery, fsm := nodeTestMocks(t, "test-node")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	logger := slog.Default()

	node, err := NewNode(ctx, logger, discovery, consensus, fsm)
	require.NoError(t, err)
	t.Cleanup(func() {
		node.Stop()
	})

	nodes := []string{"127.0.0.1:100", "127.0.01:200"}

	discovery.
		On("JoinNodes", nodes[0], nodes[1]).
		Once().
		Return(nil)

	require.NoError(t, node.ConnectToNode(nodes...))

	t.Run("when discovery fails to join nodes", func(t *testing.T) {
		discovery.
			On("JoinNodes", nodes[0], nodes[1]).
			Once().
			Return(errors.New("failed to join nodes"))

		require.Error(t, node.ConnectToNode(nodes...))
	})
}

func TestNode_Cluster(t *testing.T) {
	consensus, discovery, fsm := nodeTestMocks(t, "test-node")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	logger := slog.Default()

	node, err := NewNode(ctx, logger, discovery, consensus, fsm)
	require.NoError(t, err)
	t.Cleanup(func() {
		node.Stop()
	})

	members := []serf.Member{
		{
			Name:   "node-1",
			Tags:   map[string]string{"tag": "hello"},
			Status: serf.StatusAlive,
		},
		{
			Name:   "node-2",
			Tags:   map[string]string{"tag": "hello"},
			Status: serf.StatusFailed,
		},
	}

	discovery.
		On("ConnectedNodes").
		Once().
		Return(members)

	nodes, err := node.Cluster()
	require.NoError(t, err)

	require.Len(t, nodes, 2)
	require.ElementsMatch(t, nodes, []*NodeInfo{
		{
			Name:   nodes[0].Name,
			Status: nodes[0].Status,
			Tags:   nodes[0].Tags,
		},
		{
			Name:   nodes[1].Name,
			Status: nodes[1].Status,
			Tags:   nodes[1].Tags,
		},
	})
}

func TestNode_Tag(t *testing.T) {
	consensus, discovery, fsm := nodeTestMocks(t, "test-node")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	logger := slog.Default()

	discovery.
		On("Tags").
		Times(3).
		Return(map[string]string{"tag1": "value1", "tag2": "value2"})

	node, err := NewNode(ctx, logger, discovery, consensus, fsm)
	require.NoError(t, err)
	t.Cleanup(func() {
		node.Stop()
	})

	actual, ok := node.Tag("tag2")
	require.True(t, ok)
	require.Equal(t, "value2", actual)

	actual, ok = node.Tag("tag1")
	require.True(t, ok)
	require.Equal(t, "value1", actual)

	actual, ok = node.Tag("tag3")
	require.False(t, ok)
	require.Empty(t, actual)
}

func TestNode_Done(t *testing.T) {
	consensus, discovery, fsm := nodeTestMocks(t, "test-node")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	logger := slog.Default()

	node, err := NewNode(ctx, logger, discovery, consensus, fsm)
	require.NoError(t, err)

	select {
	case <-node.Done():
		require.Fail(t, "Done was closed before the node was stopped")
	case <-time.After(200 * time.Millisecond):
	}

	cancel()

	select {
	case <-time.After(200 * time.Millisecond):
		require.Fail(t, "the context was cancelled but the done channel is still open")
	case <-node.Done():
	}

}

func TestNode_Stop(t *testing.T) {
	consensus, discovery, fsm := nodeTestMocks(t, "test-node")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	logger := slog.Default()

	node, err := NewNode(ctx, logger, discovery, consensus, fsm)
	require.NoError(t, err)
	node.Stop()
}

func TestNode_Reconciliation(t *testing.T) {
	consensus, discovery, fsm := nodeTestMocks(t, "test-node-1")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	logger := slog.Default()

	ReconciliationInterval = 20 * time.Millisecond
	t.Cleanup(func() {
		ReconciliationInterval = 30 * time.Second
	})

	consensus.On("IsLeader").Return(true)

	consensusNodes := []raft.Server{
		{ID: "node-1"}, {ID: "node-2"}, {ID: "node-3"},
	}

	consensus.On("Nodes").Return(consensusNodes, nil)

	discoveryNodes := []serf.Member{
		{Name: "node-1", Status: serf.StatusAlive, Tags: map[string]string{"raft_addr": "0.0.0.0:1"}},
		{Name: "node-2", Status: serf.StatusFailed, Tags: map[string]string{"raft_addr": "0.0.0.0:2"}},
		{Name: "node-4", Status: serf.StatusAlive, Tags: map[string]string{"raft_addr": "0.0.0.0:4"}},
		{Name: "node-5", Status: serf.StatusLeaving, Tags: map[string]string{"raft_addr": "0.0.0.0:4"}},
	}

	discovery.On("ConnectedNodes").Return(discoveryNodes)

	consensus.On("AddNode", "node-1", "0.0.0.0:1").Return(nil)
	consensus.On("DemoteNode", "node-2", "0.0.0.0:2").Return(nil)
	consensus.On("RemoveNode", "node-3").Return(nil)
	consensus.On("AddNode", "node-4", "0.0.0.0:4").Return(nil)
	consensus.On("RemoveNode", "node-5").Return(nil)

	node, err := NewNode(ctx, logger, discovery, consensus, fsm)
	require.NoError(t, err)

	time.Sleep(40 * time.Millisecond)

	node.Stop()

	t.Run("when not a leader", func(t *testing.T) {
		consensus, discovery, fsm := nodeTestMocks(t, "test-node-2")
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		logger := slog.Default()

		consensus.On("IsLeader").Return(false)

		defer consensus.AssertNotCalled(t, "Nodes")
		defer discovery.AssertNotCalled(t, "ConnectedNodes")
		defer consensus.AssertNotCalled(t, "AddNode", mock.Anything, mock.Anything)
		defer consensus.AssertNotCalled(t, "DemoteNote", mock.Anything, mock.Anything)
		defer consensus.AssertNotCalled(t, "RemoveNode", mock.Anything)

		node, err := NewNode(ctx, logger, discovery, consensus, fsm)
		require.NoError(t, err)

		time.Sleep(40 * time.Millisecond)
		node.Stop()
	})
}

func TestNode_MemberEvents(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	logger := slog.Default()

	testCases := map[serf.EventType]struct {
		event       serf.Event
		expectation func(*testing.T, *mocks.Consensus, *mocks.Discovery)
	}{
		serf.EventMemberJoin: {
			event: serf.MemberEvent{
				Type: serf.EventMemberJoin,
				Members: []serf.Member{
					{Name: "node-1", Tags: map[string]string{"raft_addr": "0.0.0.0:1"}, Status: serf.StatusAlive},
					{Name: "node-2", Tags: map[string]string{"raft_addr": "0.0.0.0:2"}, Status: serf.StatusAlive},
				},
			},
			expectation: func(t *testing.T, c *mocks.Consensus, d *mocks.Discovery) {
				c.On("AddNode", "node-1", "0.0.0.0:1").Once().Return(nil)
				c.On("AddNode", "node-2", "0.0.0.0:2").Once().Return(nil)
			},
		},

		serf.EventMemberReap: {
			event: serf.MemberEvent{
				Type: serf.EventMemberReap,
				Members: []serf.Member{
					{Name: "node-1", Tags: map[string]string{"raft_addr": "0.0.0.0:1"}, Status: serf.StatusAlive},
					{Name: "node-2", Tags: map[string]string{"raft_addr": "0.0.0.0:2"}, Status: serf.StatusAlive},
				},
			},
			expectation: func(t *testing.T, c *mocks.Consensus, d *mocks.Discovery) {
				c.On("AddNode", "node-1", "0.0.0.0:1").Once().Return(nil)
				c.On("AddNode", "node-2", "0.0.0.0:2").Once().Return(nil)
			},
		},

		serf.EventMemberUpdate: {
			event: serf.MemberEvent{
				Type: serf.EventMemberUpdate,
				Members: []serf.Member{
					{Name: "node-1", Tags: map[string]string{"raft_addr": "0.0.0.0:1"}, Status: serf.StatusAlive},
					{Name: "node-2", Tags: map[string]string{"raft_addr": "0.0.0.0:2"}, Status: serf.StatusAlive},
				},
			},
			expectation: func(t *testing.T, c *mocks.Consensus, d *mocks.Discovery) {
				c.On("AddNode", "node-1", "0.0.0.0:1").Once().Return(nil)
				c.On("AddNode", "node-2", "0.0.0.0:2").Once().Return(nil)
			},
		},

		serf.EventMemberFailed: {
			event: serf.MemberEvent{
				Type: serf.EventMemberFailed,
				Members: []serf.Member{
					{Name: "node-1", Tags: map[string]string{"raft_addr": "0.0.0.0:1"}, Status: serf.StatusFailed},
					{Name: "node-2", Tags: map[string]string{"raft_addr": "0.0.0.0:2"}, Status: serf.StatusLeaving},
				},
			},
			expectation: func(t *testing.T, c *mocks.Consensus, d *mocks.Discovery) {
				c.On("DemoteNode", "node-1", "0.0.0.0:1").Once().Return(nil)
				c.On("DemoteNode", "node-2", "0.0.0.0:2").Once().Return(nil)
			},
		},

		serf.EventMemberLeave: {
			event: serf.MemberEvent{
				Type: serf.EventMemberLeave,
				Members: []serf.Member{
					{Name: "node-1", Tags: map[string]string{"raft_addr": "0.0.0.0:1"}, Status: serf.StatusLeft},
					{Name: "node-2", Tags: map[string]string{"raft_addr": "0.0.0.0:2"}, Status: serf.StatusLeft},
				},
			},
			expectation: func(t *testing.T, c *mocks.Consensus, d *mocks.Discovery) {
				c.On("RemoveNode", "node-1").Once().Return(nil)
				c.On("RemoveNode", "node-2").Once().Return(nil)
			},
		},
	}

	for eventType, tc := range testCases {
		t.Run(eventType.String(), func(t *testing.T) {
			consensus, discovery, fsm := nodeTestMocks(t, "test-node-1", eventType)

			tc.expectation(t, consensus, discovery)

			discovery.
				On("SubscribeToEvent", eventType, mock.Anything).
				Run(func(args mock.Arguments) {
					memberFunction, ok := args[1].(func(serf.Event))
					require.True(t, ok)
					memberFunction(tc.event)
				}).
				Once()

			consensus.On("IsLeader").Return(true)

			node, err := NewNode(ctx, logger, discovery, consensus, fsm)
			require.NoError(t, err)
			t.Cleanup(func() { node.Stop() })
		})
	}

}

func nodeTestMocks(t *testing.T, name string, skipSubscriptions ...serf.EventType) (*mocks.Consensus, *mocks.Discovery, *mocks.FSM) {
	consensus := mocks.NewConsensus(t)
	consensus.On("Name").Return(name)
	// This is called during test tear down.
	consensus.On("Stop").Return(nil)
	// This is called during initialization.
	consensus.On("Address").Return("9.9.9.9:9")

	discovery := mocks.NewDiscovery(t)
	discovery.On("Name").Return(name)
	// This is called during test tear down.
	discovery.On("Stop").Return(nil)
	// This is called during initialization.
	discovery.On("Tags").Once().Return(map[string]string{"tag": "value"})
	discovery.On("SetTags", map[string]string{"tag": "value", "raft_addr": "9.9.9.9:9"}).Once().Return(nil)

	// subscribe to all member events is part of the initialization.
	allSubscriptions := []serf.EventType{
		serf.EventMemberJoin,
		serf.EventMemberUpdate,
		serf.EventMemberReap,
		serf.EventMemberLeave,
		serf.EventMemberFailed,
	}

	for _, eventType := range allSubscriptions {
		skip := false
		for _, skipEventType := range skipSubscriptions {
			if skipEventType == eventType {
				skip = true
				break
			}
		}

		if skip {
			continue
		}

		discovery.On("SubscribeToEvent", eventType, mock.Anything)
	}

	fsm := mocks.NewFSM(t)

	return consensus, discovery, fsm
}
