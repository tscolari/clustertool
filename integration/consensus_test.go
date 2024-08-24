package integration_test

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
	"github.com/tscolari/clustertool"
)

func TestConsensus(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	t.Cleanup(cancel)

	logger := slog.Default()

	n1Config := testConsensusConfig(t, 9001, true)
	n2Config := testConsensusConfig(t, 9002, false)

	fsm1 := &raft.MockFSM{}
	node1, err := clustertool.NewConsensus(ctx, logger, "1", fsm1, n1Config)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, node1.Stop()) })

	fsm2 := &raft.MockFSM{}
	node2, err := clustertool.NewConsensus(ctx, logger, "2", fsm2, n2Config)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, node2.Stop()) })

	require.Eventually(t, func() bool {
		return node1.IsLeader()
	}, 3*time.Second, 100*time.Millisecond)

	require.NoError(t, node1.AddNode("2", n2Config.Addr))

	require.False(t, node2.IsLeader())

	fsm3 := &raft.MockFSM{}
	n3Config := testConsensusConfig(t, 9003, false)
	node3, err := clustertool.NewConsensus(ctx, logger, "3", fsm3, n3Config)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, node3.Stop()) })

	require.NoError(t, node1.AddNode("3", n3Config.Addr))

	t.Run("leader is shutdown", func(t *testing.T) {
		require.NoError(t, node1.Stop())

		require.Eventually(t, func() bool {
			return node2.IsLeader() || node3.IsLeader()
		}, 3*time.Second, 100*time.Millisecond)
	})
}

func testConsensusConfig(t *testing.T, port int, bootstrap bool) clustertool.ConsensusConfig {
	c := clustertool.DefaultConsensusConfig()
	c.Addr = fmt.Sprint("127.0.0.1:", port)
	c.Bootstrap = bootstrap
	dir, err := os.MkdirTemp("", "raft_test")
	require.NoError(t, err)
	c.DataDir = dir

	return c
}
