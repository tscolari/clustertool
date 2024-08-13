package clustertool_test

import (
	"context"
	"fmt"
	"io"
	"log"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hashicorp/serf/serf"
	"github.com/stretchr/testify/require"
	. "github.com/tscolari/clustertool"
)

func TestDiscovery(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger := slog.Default()

	ctx1, cancel1 := context.WithCancel(ctx)
	defer cancel1()
	node1Config := testSerfConfig(8001)
	node1, err := NewDiscovery(ctx1, logger.With("node", "1"), "1", node1Config)
	require.NoError(t, err)

	ctx2, cancel2 := context.WithCancel(ctx)
	defer cancel2()
	node2Config := testSerfConfig(8002)
	node2, err := NewDiscovery(ctx2, logger.With("node", "2"), "2", node2Config)
	require.NoError(t, err)

	require.NoError(t, node2.JoinNodes("127.0.0.1:8001"))

	require.Eventually(t, func() bool {
		return len(node1.ConnectedNodes()) == 2
	}, time.Second, time.Millisecond)

	require.Eventually(t, func() bool {
		return len(node2.ConnectedNodes()) == 2
	}, time.Second, time.Millisecond)

	ctx3, cancel3 := context.WithCancel(ctx)
	defer cancel3()
	node3Config := testSerfConfig(8003)
	node3, err := NewDiscovery(ctx3, logger.With("node", "3"), "3", node3Config)
	require.NoError(t, err)

	require.NoError(t, node3.JoinNodes("127.0.0.1:8002"))

	require.Eventually(t, func() bool {
		return len(node3.ConnectedNodes()) == 3
	}, time.Second, time.Millisecond)

	require.Eventually(t, func() bool {
		return len(node2.ConnectedNodes()) == 3
	}, time.Second, time.Millisecond)

	require.Eventually(t, func() bool {
		return len(node1.ConnectedNodes()) == 3
	}, time.Second, time.Millisecond)

	t.Run("event subscription and submission", func(t *testing.T) {
		eventReceivedCount := atomic.Int32{}

		eventReceivedFn := func(e serf.Event) {
			query, ok := e.(*serf.Query)
			require.True(t, ok)
			require.Equal(t, "test-event", query.Name)
			require.Equal(t, "payload", string(query.Payload))
			counter := eventReceivedCount.Add(1)
			require.NoError(t, query.Respond([]byte(fmt.Sprint(counter))))

		}

		node1.SubscribeToEvent(serf.EventQuery, eventReceivedFn)
		node2.SubscribeToEvent(serf.EventQuery, eventReceivedFn)
		resp, err := node3.SendEvent("test-event", []byte("payload"), &serf.QueryParam{
			Timeout: 10 * time.Second,
		})
		require.NoError(t, err)

		collectedResponses := make([]string, 0, 2)
		func() {
			for {
				select {
				case respPayload, ok := <-resp.ResponseCh():
					if !ok {
						return
					}
					collectedResponses = append(collectedResponses, string(respPayload.Payload))
				case <-time.After(time.Until(resp.Deadline())):
					require.Fail(t, "response channel timed out")
					return
				}
			}
		}()

		require.ElementsMatch(t, []string{"1", "2"}, collectedResponses)
		require.EqualValues(t, 2, eventReceivedCount.Load())
	})

	cancel2()

	require.Eventually(t, func() bool {
		return len(node3.ConnectedNodes()) == 2
	}, time.Second, time.Millisecond)

	require.Eventually(t, func() bool {
		return len(node1.ConnectedNodes()) == 2
	}, time.Second, time.Millisecond)

	t.Cleanup(func() {
		require.NoError(t, node1.Stop())
		require.NoError(t, node2.Stop())
		require.NoError(t, node3.Stop())
	})
}

func TestDiscovery_EventSubscription(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger := slog.Default()

	ctx1, cancel1 := context.WithCancel(ctx)
	defer cancel1()
	node1Config := testSerfConfig(8001)
	node1, err := NewDiscovery(ctx1, logger.With("node", "1"), "1", node1Config)
	require.NoError(t, err)

	ctx2, cancel2 := context.WithCancel(ctx)
	defer cancel2()
	node2Config := testSerfConfig(8002)
	node2, err := NewDiscovery(ctx2, logger.With("node", "2"), "2", node2Config)
	require.NoError(t, err)

	memberJoinedEvent := atomic.Bool{}
	node1.SubscribeToEvent(serf.EventMemberJoin, func(e serf.Event) {
		memberJoinedEvent.Swap(true)
	})

	require.NoError(t, node2.JoinNodes("127.0.0.1:8001"))

	require.Eventually(t, func() bool {
		return memberJoinedEvent.Load()
	}, time.Second, 100*time.Millisecond)

	memberLeftEvent := atomic.Bool{}
	node2.SubscribeToEvent(serf.EventMemberLeave, func(e serf.Event) {
		memberLeftEvent.Swap(true)
	})

	cancel1()

	require.Eventually(t, func() bool {
		return memberLeftEvent.Load()
	}, time.Second, 100*time.Millisecond)

	t.Cleanup(func() {
		require.NoError(t, node1.Stop())
		require.NoError(t, node2.Stop())
	})
}

func TestDiscovery_NodeDoesntExist(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger := slog.Default()

	ctx1 := context.WithoutCancel(ctx)
	node1Config := testSerfConfig(8001)
	node1, err := NewDiscovery(ctx1, logger.With("node", "1"), "1", node1Config)
	require.NoError(t, err)

	require.Error(t, node1.JoinNodes("127.0.0.1:10009", "127.0.0.1:10010"))

	t.Cleanup(func() {
		require.NoError(t, node1.Stop())
	})
}

func testSerfConfig(port int) *serf.Config {
	config := serf.DefaultConfig()
	config.Logger = log.New(io.Discard, "", 1)
	config.MemberlistConfig.BindAddr = "127.0.0.1"
	config.MemberlistConfig.BindPort = port
	config.MemberlistConfig.AdvertisePort = port
	config.MemberlistConfig.Logger = log.New(io.Discard, "", 1)
	return config
}
