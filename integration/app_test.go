package integration_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tscolari/clustertool"
	"github.com/tscolari/clustertool/integration/fsm"
)

func TestApp(t *testing.T) {
	app1FSM := fsm.NewHashStore()
	app1, err := clustertool.Factory{}.
		WithFSM(app1FSM).
		WithName("app-1").
		WithSerfConfig(testSerfConfig(8001)).
		WithConsensusConfig(testConsensusConfig(t, 8002, true)).
		Build()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, app1.Stop()) })

	require.Eventually(t, app1.Node().IsLeader, 5*time.Second, 100*time.Millisecond)

	app2FSM := fsm.NewHashStore()
	app2, err := clustertool.Factory{}.
		WithFSM(app2FSM).
		WithName("app-2").
		WithSerfConfig(testSerfConfig(9001)).
		WithConsensusConfig(testConsensusConfig(t, 9002, false)).
		Build()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, app2.Stop()) })

	app3FSM := fsm.NewHashStore()
	app3, err := clustertool.Factory{}.
		WithFSM(app3FSM).
		WithName("app-3").
		WithSerfConfig(testSerfConfig(10001)).
		WithConsensusConfig(testConsensusConfig(t, 10002, false)).
		Build()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, app3.Stop()) })

	t.Run("Joining other apps", func(t *testing.T) {
		require.NoError(t, app2.ConnectToNode("127.0.0.1:8001"))
		require.NoError(t, app3.ConnectToNode("127.0.0.1:9001"))
	})

	t.Run("Writting to the Leader", func(t *testing.T) {
		require.NoError(t, app1.Apply([]byte(`{"hello": "world"}`)))

		v, _ := app1FSM.Get("hello")
		require.Equal(t, "world", v)

		require.Eventually(t, func() bool {
			v2, ok2 := app2FSM.Get("hello")
			v3, ok3 := app2FSM.Get("hello")
			return ok2 && ok3 && v2 == v3 && v2 == "world"
		}, time.Second, 50*time.Millisecond)
	})

	t.Run("Writting to a non-Leader", func(t *testing.T) {
		require.NoError(t, app2.Apply([]byte(`{"hello": "universe"}`)))

		v, _ := app1FSM.Get("hello")
		require.Equal(t, "universe", v)

		require.Eventually(t, func() bool {
			v2, ok2 := app2FSM.Get("hello")
			v3, ok3 := app2FSM.Get("hello")
			return ok2 && ok3 && v2 == v3 && v2 == "universe"
		}, time.Second, 50*time.Millisecond)
	})

	t.Run("Subscribing to queries", func(t *testing.T) {
		err := app1.SubscribeToQuery("test-query", func(q clustertool.Query) error {
			q.Respond([]byte("hello-from-1"))
			return nil
		})
		require.NoError(t, err)

		err = app3.SubscribeToQuery("test-query", func(q clustertool.Query) error {
			q.Respond([]byte("hello-from-3"))
			return nil
		})
		require.NoError(t, err)

		t.Run("broadcast query", func(t *testing.T) {
			resp, err := app2.SendQuery("test-query", []byte("hello"), &clustertool.QueryParam{})
			require.NoError(t, err)

			responses := make([][]byte, 0, 2)
			for i := 0; i < 2; i++ {
				select {
				case response := <-resp.ResponseCh():
					responses = append(responses, response.Payload)
				case <-time.After(500 * time.Millisecond):
					require.Fail(t, "timed out waiting for response")
				}
			}

			require.ElementsMatch(t, [][]byte{
				[]byte("hello-from-1"),
				[]byte("hello-from-3"),
			}, responses)
		})

		t.Run("target app", func(t *testing.T) {
			resp, err := app2.SendQuery("test-query", []byte("good-bye"), &clustertool.QueryParam{
				FilterNodes: []string{"app-3"},
			})
			require.NoError(t, err)

			responses := make([][]byte, 0, 2)
			for i := 0; i < 2; i++ {
				select {
				case response := <-resp.ResponseCh():
					responses = append(responses, response.Payload)
				case <-time.After(500 * time.Millisecond):
				}
			}

			require.Len(t, responses, 1)
			require.Equal(t, []byte("hello-from-3"), responses[0])
		})
	})
}
