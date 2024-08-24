package integration_test

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
	"github.com/tscolari/clustertool"
	"github.com/tscolari/clustertool/integration/fsm"
)

var nodeTestDataDirBase string

func init() {
	clustertool.ReconciliationInterval = 500 * time.Millisecond
	var err error
	nodeTestDataDirBase, err = os.MkdirTemp("", "clustertool")
	if err != nil {
		panic(err)
	}
}

func TestNode(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fsm1 := fsm.NewHashStore()
	node1, cancel1 := testNode(t, ctx, "node_1", 9000, true, fsm1)
	t.Cleanup(cancel1)

	t.Log("waiting for node1 to be leader")
	require.Eventually(t, node1.IsLeader, 3*time.Second, 100*time.Millisecond)

	require.NoError(t,
		node1.Apply(hashToBytes(t, map[string]string{"key1": "value1"}), time.Second),
	)

	fsm2 := fsm.NewHashStore()
	node2, cancel2 := testNode(t, ctx, "node_2", 10000, false, fsm2)
	t.Cleanup(cancel2)

	require.NoError(t, node1.ConnectToNode("127.0.0.1:10001"))

	t.Log("waiting for node2 to sync FSM")
	require.Eventually(t, func() bool {
		value, ok := fsm2.Get("key1")
		return ok && value == "value1"
	}, 2*time.Second, 100*time.Millisecond)

	fsm3 := fsm.NewHashStore()
	node3, cancel3 := testNode(t, ctx, "node_3", 11000, false, fsm3)
	t.Cleanup(cancel3)

	require.NoError(t, node3.ConnectToNode("127.0.0.1:10001"))

	t.Log("waiting for node3 to sync FSM")
	require.Eventually(t, func() bool {
		value, ok := fsm3.Get("key1")
		return ok && value == "value1"
	}, 2*time.Second, 100*time.Millisecond)

	t.Log("executing Apply on leader node (node1)")
	require.NoError(t,
		node1.Apply(hashToBytes(t, map[string]string{"key1": "newValue1", "key2": "value2"}), time.Second),
	)

	t.Log("waiting for apply to be reach consensus")
	require.Eventually(t, func() bool {
		value1, ok1 := fsm2.Get("key1")
		value2, ok2 := fsm3.Get("key2")
		return ok1 && ok2 && value1 == "newValue1" && value2 == "value2"
	}, 2*time.Second, 100*time.Millisecond)

	t.Log("stopping node 1 (leader)")
	cancel1()

	t.Log("waiting for new leader")
	require.Eventually(t, func() bool {
		return node2.IsLeader() || node3.IsLeader()
	}, 5*time.Second, 100*time.Millisecond)

	t.Logf("new leader is %q", node2.Leader())

	apply := node2.Apply
	if leaderName := node2.Leader(); leaderName == "node3" {
		apply = node3.Apply
	}

	t.Logf("sending apply to new leader (%s)", node2.Leader())
	require.NoError(t,
		apply(hashToBytes(t, map[string]string{"key3": "value3"}), time.Second),
	)

	require.Eventually(t, func() bool {
		value1, ok1 := fsm2.Get("key3")
		value2, ok2 := fsm3.Get("key3")
		return ok1 && ok2 && value1 == "value3" && value2 == "value3"
	}, 2*time.Second, 100*time.Millisecond)

	t.Logf("starting node1 again")
	fsm11 := fsm.NewHashStore()
	node11, cancel11 := testNode(t, ctx, "node_1", 9000, false, fsm11)
	t.Cleanup(cancel11)

	require.NoError(t, node11.ConnectToNode("127.0.0.1:11001"))

	t.Logf("waiting for node 1 to sync")
	require.Eventually(t, func() bool {
		value, ok := fsm11.Get("key3")
		return ok && value == "value3"
	}, 4*time.Second, 100*time.Millisecond)
}

func testNode(t *testing.T, ctx context.Context, name string, basePort int, bootstrap bool, fsm raft.FSM) (clustertool.Node, func()) {
	logger := slog.Default()

	serfConfig := testSerfConfig(basePort + 1)
	discovery, err := clustertool.NewDiscovery(ctx, logger, name, serfConfig)
	require.NoError(t, err)

	consensusConfig := testConsensusConfig(t, basePort+2, bootstrap)
	consensusConfig.DataDir = filepath.Join(nodeTestDataDirBase, name)
	consensus, err := clustertool.NewConsensus(ctx, logger, name, fsm, consensusConfig)
	require.NoError(t, err)

	node, err := clustertool.NewNode(ctx, logger, discovery, consensus)
	require.NoError(t, err)

	return node, func() {
		require.NoError(t, node.Stop())
	}
}

func hashToBytes(t *testing.T, values map[string]string) []byte {
	v, err := json.Marshal(values)
	require.NoError(t, err)

	return v
}
