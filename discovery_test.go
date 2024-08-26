package clustertool_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hashicorp/serf/serf"
	mock "github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	. "github.com/tscolari/clustertool"
	"github.com/tscolari/clustertool/mocks"
)

func TestDiscovery_Name(t *testing.T) {
	discovery, _, _ := testDiscovery(t, "node-1")
	require.Equal(t, "node-1", discovery.Name())
}

func TestDiscovery_Address(t *testing.T) {
	discovery, _, _ := testDiscovery(t, "node-1")
	// serf's default configuration bind address
	require.Equal(t, "0.0.0.0:7946", discovery.Address())
}

func TestDiscovery_ConnectedNodes(t *testing.T) {
	discovery, serfMock, _ := testDiscovery(t, "node-1")

	members := []serf.Member{
		{Name: "0", Status: serf.StatusAlive},
		{Name: "1", Status: serf.StatusLeft},
		{Name: "2", Status: serf.StatusNone},
		{Name: "3", Status: serf.StatusFailed},
		{Name: "4", Status: serf.StatusLeaving},
		{Name: "5", Status: serf.StatusAlive},
	}

	serfMock.
		On("Members").
		Once().
		Return(members)

	expectedMembers := []DiscoveryMember{
		{Name: "0", Status: serf.StatusAlive},
		{Name: "3", Status: serf.StatusFailed},
		{Name: "4", Status: serf.StatusLeaving},
		{Name: "5", Status: serf.StatusAlive},
	}

	require.Equal(t, expectedMembers, discovery.ConnectedNodes())
}

func TestDiscovery_Tags(t *testing.T) {
	discovery, _, _ := testDiscovery(t, "node-1")

	tags := discovery.Tags()
	require.Equal(t, map[string]string{
		"tag1": "value1", "tag2": "value2",
	}, tags)
}

func TestDiscovery_SubscribeToEvent(t *testing.T) {
	discovery, _, eventsCh := testDiscovery(t, "node-1")

	action1Exec := atomic.Bool{}
	action1 := func(_ serf.Event) { action1Exec.Store(true) }
	action2Exec := atomic.Bool{}
	action2 := func(_ serf.Event) { action2Exec.Store(true) }
	action3Exec := atomic.Bool{}
	action3 := func(_ serf.Event) { action3Exec.Store(true) }

	discovery.SubscribeToEvent(serf.EventQuery, action1)
	discovery.SubscribeToEvent(serf.EventUser, action2)
	discovery.SubscribeToEvent(serf.EventQuery, action3)

	require.False(t, action1Exec.Load())
	require.False(t, action2Exec.Load())
	require.False(t, action3Exec.Load())

	eventsCh <- &serf.Query{}

	require.Eventually(t, func() bool {
		return action1Exec.Load() && action3Exec.Load()
	}, 50*time.Millisecond, 10*time.Microsecond)

	require.False(t, action2Exec.Load())

	eventsCh <- &serf.UserEvent{}

	require.Eventually(t, func() bool {
		return action2Exec.Load()
	}, 10*time.Millisecond, 10*time.Microsecond)
}

func TestDiscovery_SendEvent(t *testing.T) {
	discovery, serfMock, _ := testDiscovery(t, "node-1")

	queryName := "my-query"
	queryPayload := []byte("payload")
	queryParam := &QueryParam{RequestAck: true}

	queryResponse := &serf.QueryResponse{}

	serfMock.
		On("Query", queryName, queryPayload, testQueryParamToSerf(queryParam)).
		Once().
		Return(queryResponse, nil)

	resp, err := discovery.SendEvent(queryName, queryPayload, queryParam)
	require.NoError(t, err)
	require.Equal(t, queryResponse, resp)

	t.Run("when serf returns an error", func(t *testing.T) {
		serfMock.
			On("Query", queryName, queryPayload, testQueryParamToSerf(queryParam)).
			Once().
			Return(nil, errors.New("failed"))

		resp, err := discovery.SendEvent(queryName, queryPayload, queryParam)
		require.Error(t, err)
		require.ErrorContains(t, err, "failed")
		require.Nil(t, resp)
	})
}

func TestDiscovery_SetTags(t *testing.T) {
	discovery, serfMock, _ := testDiscovery(t, "node-1")

	newTags := map[string]string{"tag1": "override"}

	serfMock.
		On("SetTags", newTags).
		Once().
		Return(nil)

	err := discovery.SetTags(newTags)
	require.NoError(t, err)

	require.Equal(t, newTags, discovery.Tags())

	t.Run("when serf fails to set tags", func(t *testing.T) {
		newerTags := map[string]string{"tag1": "override again", "tag2": "value"}

		serfMock.
			On("SetTags", newerTags).
			Once().
			Return(errors.New("failed badly"))

		err := discovery.SetTags(newerTags)
		require.Error(t, err)
		require.ErrorContains(t, err, "failed badly")

		require.NotEqual(t, newerTags, discovery.Tags())
		require.Equal(t, newTags, discovery.Tags())
	})
}

func TestDiscovery_JoinNodes(t *testing.T) {
	discovery, serfMock, _ := testDiscovery(t, "node-1")

	serfMock.
		On("Join", []string{"0.0.0.1:10", "0.0.0.2:10"}, true).
		Once().
		Return(2, nil)

	err := discovery.JoinNodes("0.0.0.1:10", "0.0.0.2:10")
	require.NoError(t, err)

	t.Run("when serf returns an error", func(t *testing.T) {
		serfMock.
			On("Join", []string{"0.0.0.3:10", "0.0.0.4:10"}, true).
			Once().
			Return(0, errors.New("failed badly"))

		err := discovery.JoinNodes("0.0.0.3:10", "0.0.0.4:10")
		require.Error(t, err)
		require.ErrorContains(t, err, "failed badly")
	})
}

func TestDiscovery_Stop(t *testing.T) {
	discovery, serfMock, _ := testDiscovery(t, "node-1")

	serfMock.
		On("Leave").
		Once().
		Return(nil)

	serfMock.
		On("Shutdown").
		Once().
		Return(nil)

	require.NoError(t, discovery.Stop())
	select {
	case <-discovery.Done():
	case <-time.After(time.Millisecond):
		require.Fail(t, "Done() should return immediately")
	}

	t.Run("it should wait if another stop is in progress", func(t *testing.T) {
		discovery, serfMock, _ := testDiscovery(t, "node-1")

		wg := new(sync.WaitGroup)
		wg.Add(1)

		// Leave will block, so that the first stop stays in progress
		serfMock.
			On("Leave").
			Run(func(args mock.Arguments) {
				wg.Wait()
			}).
			Once().
			Return(nil)

		serfMock.
			On("Shutdown").
			Once().
			Return(nil)

		firstStopReturned := make(chan bool)
		go func() {
			require.NoError(t, discovery.Stop())
			firstStopReturned <- true
		}()

		select {
		case <-firstStopReturned:
			require.Fail(t, "first stop should not have returned")
		case <-time.After(10 * time.Millisecond):
		}

		secondStopReturned := make(chan bool)
		go func() {
			require.NoError(t, discovery.Stop())
			secondStopReturned <- true
		}()

		select {
		case <-secondStopReturned:
			require.Fail(t, "second stop should not have returned")
		case <-time.After(10 * time.Millisecond):
		}

		// Done() should also be blocking
		select {
		case <-discovery.Done():
			require.Fail(t, "Done channel should not have closed")
		case <-time.After(10 * time.Millisecond):
		}

		wg.Done()

		require.True(t, <-firstStopReturned)
		require.True(t, <-secondStopReturned)

		select {
		case <-discovery.Done():
		case <-time.After(10 * time.Millisecond):
			require.Fail(t, "Done channel should have closed but timed out")
		}
	})

	t.Run("when serf's Leave fails", func(t *testing.T) {
		discovery, serfMock, _ := testDiscovery(t, "node-1")

		serfMock.
			On("Leave").
			Once().
			Return(errors.New("failed"))

		serfMock.
			On("Shutdown").
			Once().
			Return(nil)

		require.NoError(t, discovery.Stop())
		select {
		case <-discovery.Done():
		case <-time.After(time.Millisecond):
			require.Fail(t, "Done() should return immediately")
		}
	})

	t.Run("when serf's shutdown fails", func(t *testing.T) {
		discovery, serfMock, _ := testDiscovery(t, "node-1")

		serfMock.
			On("Leave").
			Once().
			Return(nil)

		serfMock.
			On("Shutdown").
			Once().
			Return(errors.New("boom"))

		err := discovery.Stop()
		require.Error(t, err)
		require.ErrorContains(t, err, "boom")

		// It should still be considered as Done.
		select {
		case <-discovery.Done():
		case <-time.After(time.Millisecond):
			require.Fail(t, "Done() should return immediately")
		}

		t.Run("calling Stop again will produce the same error", func(t *testing.T) {
			err := discovery.Stop()
			require.Error(t, err)
			require.ErrorContains(t, err, "boom")

			err = discovery.Stop()
			require.Error(t, err)
			require.ErrorContains(t, err, "boom")
		})
	})
}

func TestDiscovery_KeyManager(t *testing.T) {
	discovery, serfMock, _ := testDiscovery(t, "node-1")

	keyManager := new(serf.KeyManager)

	serfMock.
		On("KeyManager").
		Once().
		Return(keyManager)

	require.Equal(t, keyManager, discovery.KeyManager())
}

func testDiscovery(t *testing.T, nodeName string) (Discovery, *mocks.HashicorpSerf, chan<- serf.Event) {
	serfConfig := serf.DefaultConfig()
	serfConfig.Tags = map[string]string{
		"tag1": "value1",
		"tag2": "value2",
	}
	serfConfig.Logger = nil
	serfConfig.LogOutput = io.Discard
	serfConfig.MemberlistConfig.Logger = nil
	serfConfig.MemberlistConfig.LogOutput = io.Discard

	discovery, err := NewDiscovery(
		context.Background(),
		slog.Default(),
		nodeName,
		serfConfig,
	)

	require.NoError(t, err)
	serfMock := mocks.NewHashicorpSerf(t)
	eventsCh := make(chan serf.Event, 10)
	TestAttachMockToDiscovery(t, discovery, serfMock, eventsCh)

	return discovery, serfMock, eventsCh
}
