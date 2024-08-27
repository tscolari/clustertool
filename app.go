package clustertool

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/hashicorp/serf/serf"
)

type App interface {
	Apply(cmd []byte) error

	SubscribeToQuery(name string, action func(Query) error) error
	SendQuery(name string, payload []byte, params *QueryParam) (EventResponse, error)

	ConnectToNode(addresses ...string) error

	Node() Node

	stoppable
}

type EventResponse interface {
	Close()
	ResponseCh() <-chan serf.NodeResponse
}

const applyRedirectQueryName = "apply_redirect"

func DefaultAppOptions() AppOptions {
	return AppOptions{
		ApplyTimeout:        30 * time.Second,
		DontRedirectApplies: false,
	}
}

type AppOptions struct {
	ApplyTimeout        time.Duration
	DontRedirectApplies bool
}

// app is an implementation of App.
// It wraps the Node hiding the complexity, and expose simpler functions
// for the interacting code.
// It can also redirect Applies to the leader (when received in a non-leader).
type app struct {
	node    Node
	options AppOptions
	logger  *slog.Logger
}

// NewApp returns an instance of app wrapping the given Node.
func NewApp(ctx context.Context, logger *slog.Logger, node Node, options AppOptions) (*app, error) {
	c := &app{
		node:    node,
		logger:  logger.With("component", "app", "node_name", node.Name()),
		options: options,
	}

	if err := c.init(); err != nil {
		return nil, fmt.Errorf("failed to initialize app: %w", err)
	}

	return c, nil
}

func (c *app) init() error {
	c.node.SubscribeToEvent(applyRedirectQueryName, func(query Query) {
		if err := c.applyFromEvent(query); err != nil {
			c.logger.Error("failed to process redirected apply", "error", err, "source_node", query.SourceNode())
			return
		}
	})

	return nil
}

func (c *app) Apply(cmd []byte) error {
	if c.node.IsLeader() {
		return c.node.Apply(cmd, c.options.ApplyTimeout)
	}

	if c.options.DontRedirectApplies {
		return errors.New("only the leader can process applies when redirect is disabled")
	}

	leader := c.node.Leader()

	params := QueryParam{
		FilterNodes: []string{leader},
		RequestAck:  true,
		Timeout:     c.options.ApplyTimeout,
	}

	resp, err := c.node.SendEvent(applyRedirectQueryName, cmd, &params)
	if err != nil {
		return fmt.Errorf("failed to redirect Apply command to Leader: %w", err)
	}

	defer resp.Close()

	var nodeResp serf.NodeResponse
	select {
	case nodeResp = <-resp.ResponseCh():
	case <-time.After(c.options.ApplyTimeout):
		return errors.New("timed out waiting for apply redirect")
	}

	if nodeResp.From != leader {
		return fmt.Errorf("apply redirect responded by non-leader (%s)", nodeResp.From)
	}

	var redirecrResponse *redirectResponse
	if err := json.Unmarshal(nodeResp.Payload, &redirecrResponse); err != nil {
		return fmt.Errorf("invalid apply redirect response format: %w", err)
	}

	if redirecrResponse.Error != nil {
		return errors.New(*redirecrResponse.Error)
	}

	return nil
}

// applyFromEvent is called when the leader receives a redirected apply from a non-leader node.
func (c *app) applyFromEvent(event Query) error {
	if !c.node.IsLeader() {
		return errors.New("can't apply because I'm not the leader")
	}

	err := c.node.Apply(event.Payload(), time.Until(event.Deadline()))
	response := newRedirectResponse(err)

	responsePayload, err := json.Marshal(&response)
	if err != nil {
		return fmt.Errorf("failed to marshal applyFromEvent response: %w", err)
	}

	if err := event.Respond(responsePayload); err != nil {
		return fmt.Errorf("failed to respond to applyFromEvent: %w", err)
	}

	return nil
}

// SubscribeToQuery will execute the given action every time a query with the given name
// is received by the app's node.
func (c *app) SubscribeToQuery(name string, action func(Query) error) error {
	if name == applyRedirectQueryName {
		return fmt.Errorf("%q is a reserved query", applyRedirectQueryName)
	}

	c.node.SubscribeToEvent(name, func(query Query) {
		if err := action(query); err != nil {
			c.logger.Error("subscribed action failed to process query", "error", err, "source_node", query.SourceNode())
		}
	})

	return nil
}

// SendQuery will use the Node's API to send a query with the given name and payload
// using the given params.
func (c *app) SendQuery(name string, payload []byte, params *QueryParam) (EventResponse, error) {
	if name == applyRedirectQueryName {
		return nil, fmt.Errorf("%q is a reserved query", applyRedirectQueryName)
	}

	return c.node.SendEvent(name, payload, params)
}

func (c *app) ConnectToNode(addresses ...string) error {
	return c.node.ConnectToNode(addresses...)
}

// Node returns the wrapped Node implementation to expose more
// low level commands.
func (c *app) Node() Node {
	return c.node
}

func (c *app) Stop() error {
	return c.node.Stop()
}

func (c *app) Done() <-chan struct{} {
	return c.node.Done()
}

func newRedirectResponse(err error) redirectResponse {
	if err != nil {
		errMessage := err.Error()
		return redirectResponse{&errMessage}
	}

	return redirectResponse{}
}

// redirectResponse is a simple struct returned by the apply redirect query.
// It's used to let the calling node to know if the apply was successful or not.
type redirectResponse struct {
	Error *string
}

var _ App = &app{}
