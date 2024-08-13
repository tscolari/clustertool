package clustertool_test

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/hashicorp/serf/serf"
	mock "github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	. "github.com/tscolari/clustertool"
)

func TestApp_Apply(t *testing.T) {
	t.Run("when node is leader", func(t *testing.T) {
		node := newTestNode(t)

		node.
			On("IsLeader").Return(true)

		cmd := []byte("hello world")
		node.
			On("Apply", cmd, 30*time.Second).
			Once().
			Return(nil)

		app := newTestApp(t, node)
		require.NoError(t, app.Apply(cmd))

		t.Run("when node fails to Apply", func(t *testing.T) {
			node.
				On("Apply", cmd, 30*time.Second).
				Once().
				Return(errors.New("failed"))

			require.Error(t, app.Apply(cmd))
		})
	})

	t.Run("when node is not leader", func(t *testing.T) {
		node := newTestNode(t)
		node.On("IsLeader").Return(false)

		app := newTestApp(t, node)

		leaderName := "leader-node"
		node.On("Leader").Return(leaderName)

		cmd := []byte("hello world")

		eventResponse := newTestEventResponse(t, leaderName, "{}")
		queryParams := serf.QueryParam{
			FilterNodes: []string{leaderName},
			RequestAck:  true,
			Timeout:     DefaultAppOptions().ApplyTimeout,
		}

		node.On("SendEvent", "apply_redirect", cmd, &queryParams).
			Once().
			Return(eventResponse, nil)

		require.NoError(t, app.Apply(cmd))

		t.Run("when SendEvent fails", func(t *testing.T) {
			node.On("SendEvent", "apply_redirect", cmd, &queryParams).
				Once().
				Return(nil, errors.New("failed to send query"))

			err := app.Apply(cmd)
			require.Error(t, err)
			require.ErrorContains(t, err, "failed to send query")
		})

		t.Run("when the reply is not from the leader", func(t *testing.T) {
			eventResponse := newTestEventResponse(t, "not-the-leader", "{}")
			node.On("SendEvent", "apply_redirect", cmd, &queryParams).
				Once().
				Return(eventResponse, nil)

			err := app.Apply(cmd)
			require.Error(t, err)
			require.ErrorContains(t, err, "redirect responded by non-leader")
		})

		t.Run("when the reply contains an error", func(t *testing.T) {
			eventResponse := newTestEventResponse(t, leaderName, `{"Error": "unexpected command"}`)
			node.On("SendEvent", "apply_redirect", cmd, &queryParams).
				Once().
				Return(eventResponse, nil)

			err := app.Apply(cmd)
			require.Error(t, err)
			require.ErrorContains(t, err, "unexpected command")
		})

		t.Run("with DontRedirectApplies enabled", func(t *testing.T) {
			node := newTestNode(t)
			node.On("IsLeader").Return(false)
			defer node.AssertNotCalled(t, "Apply", mock.Anything, mock.Anything)

			app := newTestApp(t, node, AppOptions{DontRedirectApplies: true})
			err := app.Apply([]byte("hello"))
			require.Error(t, err)
			require.ErrorContains(t, err, "only the leader can process applies")
		})
	})
}

func TestApp_SubscribeToQuery(t *testing.T) {
	node := newTestNode(t)
	app := newTestApp(t, node)

	queryName := "my-query"
	someQuery := &serf.Query{Name: uuid.NewString()}
	actionCalled := false
	action := func(query *serf.Query) error {
		actionCalled = true
		require.Equal(t, someQuery, query)
		return nil
	}

	node.On("SubscribeToEvent", queryName, mock.Anything).
		Run(func(args mock.Arguments) {
			actionWrapper, ok := args[1].(func(*serf.Query))
			require.True(t, ok)
			actionWrapper(someQuery)
		})

	require.False(t, actionCalled)
	app.SubscribeToQuery(queryName, action)
	require.True(t, actionCalled)
}

func TestApp_SendQuery(t *testing.T) {
	node := newTestNode(t)
	app := newTestApp(t, node)

	queryName := "my-query"
	payload := []byte("hello")
	params := &serf.QueryParam{FilterNodes: []string{uuid.NewString()}}

	eventResponse := NewMockEventResponse(t)

	node.
		On("SendEvent", queryName, payload, params).
		Once().
		Return(eventResponse, nil)

	response, err := app.SendQuery(queryName, payload, params)
	require.NoError(t, err)
	require.Equal(t, eventResponse, response)
}

func TestApp_Node(t *testing.T) {
	node := newTestNode(t)
	app := newTestApp(t, node)
	require.Equal(t, node, app.Node())
}

func newTestNode(t *testing.T) *MockNode {
	node := NewMockNode(t)
	node.On("SubscribeToEvent", "apply_redirect", mock.Anything).Once()
	node.On("Name").Return("node-1")
	return node
}

func newTestApp(t *testing.T, node Node, options ...AppOptions) App {
	appOptions := DefaultAppOptions()
	if len(options) > 0 {
		appOptions = options[0]
	}

	logger := slog.Default()
	app, err := NewApp(context.Background(), logger, node, appOptions)
	require.NoError(t, err)

	return app
}

func newTestEventResponse(t *testing.T, from string, payload string) *MockEventResponse {
	eventResponse := NewMockEventResponse(t)
	eventResponse.On("Close").Once()
	responseCh := make(chan serf.NodeResponse, 10)

	chanReader := func() <-chan serf.NodeResponse {
		return responseCh
	}

	responseCh <- serf.NodeResponse{
		From:    from,
		Payload: []byte(payload),
	}

	eventResponse.On("ResponseCh").Once().Return(chanReader())

	return eventResponse
}
