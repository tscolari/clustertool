package clustertool

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/hashicorp/raft"
	"github.com/hashicorp/serf/serf"
)

const (
	raftAddrTag                   = "raft_addr"
	defaultReconciliationInterval = 30 * time.Second
)

var (
	ReconciliationInterval = defaultReconciliationInterval
)

type NodeStatus = serf.MemberStatus

type stoppable interface {
	Stop() error
	Done() <-chan struct{}
}

type addressable interface {
	Name() string
	Address() string
}

type Discovery interface {
	ConnectedNodes() []serf.Member
	JoinNodes(nodes ...string) error
	KeyManager() serf.KeyManager

	SetTags(map[string]string) error
	Tags() map[string]string

	SubscribeToEvent(et serf.EventType, action func(serf.Event))
	SendEvent(name string, payload []byte, params *serf.QueryParam) (*serf.QueryResponse, error)

	addressable
	stoppable
}

type Consensus interface {
	Apply(cmd []byte, timeout time.Duration) error
	IsLeader() bool
	Leader() (address string, id string)

	AddNode(id string, address string) error
	DemoteNode(id string, address string) error
	RemoveNode(id string) error
	Nodes() (servers []raft.Server, err error)

	addressable
	stoppable
}

// FSM defines the interface that the internal finite state machine must have.
// It will be called to apply changes in the state, as well as snapshotting and restoring it.
type FSM interface {
	raft.FSM
}

// NodeInfo is a basic node information.
type NodeInfo struct {
	Name   string
	Tags   map[string]string
	Status NodeStatus
}

// Node contains the behaviour that a Node must present.
// It is a simplified wrapper around the Discovery and Consensus interfaces.
type Node interface {
	// Name returns the unique name of the given node.
	Name() string

	// IsLeader tells if the node is currently the leader of the cluster.
	IsLeader() bool

	// Leader returns the unique Name/ID of the leader.
	Leader() string

	// Apply will perform a change in the underlying FSM.
	// Only leaders can perform applies, if called on a non-leader it will fail.
	Apply([]byte, time.Duration) error

	// SubscribeToEvent will call the given action function every time an
	// internal query event matching the queryName is received.
	SubscribeToEvent(queryName string, action func(*serf.Query))

	// SendEvent sends a query event to other nodes.
	// params can be used to control the spread of the event.
	SendEvent(name string, payload []byte, params *serf.QueryParam) (EventResponse, error)

	// ConnectToNode is a helper to inter-connect nodes.
	// To join a cluster, it only needs to know about a single node.
	ConnectToNode(...string) error

	// Cluster returns all known-active nodes of the cluster.
	Cluster() ([]*NodeInfo, error)

	// Tag is a way to access the tags that this node is annotated with.
	Tag(string) (string, bool)

	stoppable
}

type node struct {
	name      string
	consensus Consensus
	discovery Discovery

	logger          *slog.Logger
	internalCtx     context.Context
	stopInternalCtx func()
	doneChan        chan struct{}
}

func NewNode(
	ctx context.Context,
	logger *slog.Logger,
	discovery Discovery,
	consensus Consensus,
	fsm FSM,
) (*node, error) {

	if discovery.Name() != consensus.Name() {
		return nil, errors.New("discovery and consensus must have the same name")
	}

	logger = logger.With("node_name", discovery.Name())

	// internalCtx is used by all the internal components to signal
	// the termination of execution.
	internalCtx, stopInternalCtx := context.WithCancel(ctx)

	n := &node{
		name:      discovery.Name(),
		discovery: discovery,
		consensus: consensus,

		internalCtx:     internalCtx,
		stopInternalCtx: stopInternalCtx,
		logger:          logger.With("component", "node"),
		doneChan:        make(chan struct{}),
	}

	if err := n.init(ReconciliationInterval); err != nil {
		stopInternalCtx()
		return n, fmt.Errorf("failed to initialize node: %w", err)
	}

	return n, nil
}

func (n *node) Name() string {
	return n.discovery.Name()
}

func (n *node) IsLeader() bool {
	return n.consensus.IsLeader()
}

func (n *node) Leader() string {
	_, leaderName := n.consensus.Leader()
	return leaderName
}

func (n *node) Apply(cmd []byte, timeout time.Duration) error {
	if !n.IsLeader() {
		return errors.New("can't Apply if not the leader")
	}

	return n.consensus.Apply(cmd, timeout)
}

func (n *node) SendEvent(name string, payload []byte, params *serf.QueryParam) (EventResponse, error) {
	return n.discovery.SendEvent(name, payload, params)
}

func (n *node) ConnectToNode(nodes ...string) error {
	if err := n.discovery.JoinNodes(nodes...); err != nil {
		n.logger.Error("failed to connect to nodes", "error", err)
		return fmt.Errorf("discovery failed to connect to nodes: %w", err)
	}
	return nil
}

func (n *node) Cluster() ([]*NodeInfo, error) {
	members := n.discovery.ConnectedNodes()

	quickAccess := map[string]*NodeInfo{}

	nodeInfos := make([]*NodeInfo, 0, len(members))
	for _, member := range members {
		nodeInfo := &NodeInfo{
			Name:   member.Name,
			Status: member.Status,
			Tags:   member.Tags,
		}

		quickAccess[nodeInfo.Name] = nodeInfo
		nodeInfos = append(nodeInfos, nodeInfo)
	}

	return nodeInfos, nil
}

func (n *node) Tag(key string) (string, bool) {
	value, ok := n.discovery.Tags()[key]
	return value, ok
}

func (n *node) SubscribeToEvent(queryName string, action func(*serf.Query)) {
	n.discovery.SubscribeToEvent(serf.EventQuery, func(e serf.Event) {
		query, ok := e.(*serf.Query)
		if !ok {
			n.logger.Error("could not perform subscribed action on query: event is not a query")
			return
		}

		if queryName == query.Name {
			action(query)
		}
	})
}

// Stop will block until all internal components are closed.
func (n *node) Stop() error {
	n.stopInternalCtx()

	var errs error

	if err := n.discovery.Stop(); err != nil {
		n.logger.Error("failed to stop the discovery interface: %w", err)
		errs = fmt.Errorf("%w: %w", errs, err)
	}

	if err := n.consensus.Stop(); err != nil {
		n.logger.Error("failed to stop the consensus interface: %w", err)
		errs = fmt.Errorf("%w: %w", errs, err)
	}

	return errs
}

func (n *node) Done() <-chan struct{} {
	return n.doneChan
}

func (n *node) init(reconciliationInterval time.Duration) error {
	tags := n.discovery.Tags()
	tags[raftAddrTag] = n.consensus.Address()
	if err := n.discovery.SetTags(tags); err != nil {
		return fmt.Errorf("failed to set %s tag to discovery node", raftAddrTag)
	}

	n.discovery.SubscribeToEvent(serf.EventMemberJoin, n.memberWelcomeEvent)
	n.discovery.SubscribeToEvent(serf.EventMemberUpdate, n.memberWelcomeEvent)
	n.discovery.SubscribeToEvent(serf.EventMemberReap, n.memberWelcomeEvent)
	n.discovery.SubscribeToEvent(serf.EventMemberLeave, n.memberGoneEvent)
	n.discovery.SubscribeToEvent(serf.EventMemberFailed, n.memberGoneEvent)

	go n.watchInternalCtx()
	go n.reconcileMembers(reconciliationInterval)

	return nil
}

func (n *node) watchInternalCtx() {
	<-n.internalCtx.Done()
	_ = n.discovery.Stop()
	_ = n.consensus.Stop()

	close(n.doneChan)
}

func (n *node) reconcileMembers(reconciliationInterval time.Duration) {
	for {
		select {
		case <-n.internalCtx.Done():
			return
		case <-time.After(reconciliationInterval):
			if !n.IsLeader() {
				continue
			}

			n.logger.Info("running reconciliation")

			consensusNodes, err := n.consensus.Nodes()
			if err != nil {
				n.logger.Error("failed to fetch consensus nodes for reconciliation", "error", err)
			}

			nodesMap := make(map[string]struct{}, len(consensusNodes))
			for _, node := range consensusNodes {
				nodesMap[string(node.ID)] = struct{}{}
			}

			members := n.discovery.ConnectedNodes()
			for _, member := range members {
				delete(nodesMap, member.Name)

				if member.Name == n.name {
					continue
				}

				switch member.Status {
				case serf.StatusAlive:
					if err := n.addMember(member); err != nil {
						n.logger.Error("failed to try adding member during reconciliation", "error", err)
					}
				case serf.StatusFailed:
					if err := n.demoteMember(member); err != nil {
						n.logger.Error("failed to try demoting member during reconciliation", "error", err)
					}

				default:
					if err := n.removeMember(member); err != nil {
						n.logger.Error("failed to try removing member during reconciliation", "error", err)
					}
				}
			}

			for id := range nodesMap {
				if err := n.removeMember(serf.Member{Name: id}); err != nil {
					n.logger.Error("failed to remove stale consensus node during reconciliation", "error", err)
				}
			}
		}
	}
}

func (n *node) memberWelcomeEvent(e serf.Event) {
	if !n.IsLeader() {
		n.logger.Debug("not leader, ignoring new member event")
		return
	}

	switch e.EventType() {
	default:
		n.logger.Error("invalid event type sent as member welcome event", "event_type", e.EventType().String())

	case serf.EventMemberUpdate, serf.EventMemberReap, serf.EventMemberJoin:

		memberEvent, ok := e.(serf.MemberEvent)
		if !ok {
			n.logger.Error("received invalid member event message for event type", "event_type", e.EventType().String())
			return
		}

		for _, member := range memberEvent.Members {
			if err := n.addMember(member); err != nil {
				n.logger.Error("could not add member", "member_name", member.Name)
			}
		}
	}
}

func (n *node) memberGoneEvent(e serf.Event) {
	if !n.IsLeader() {
		n.logger.Debug("not leader, ignoring member gone event")
		return
	}

	switch e.EventType() {
	default:
		n.logger.Error("invalid event type sent as member gone event", "event_type", e.EventType().String())

	// On EventMemberLeave we remove them straight away from the consensus
	// as the expectation is they won't come back.
	case serf.EventMemberLeave:
		memberEvent, ok := e.(serf.MemberEvent)
		if !ok {
			n.logger.Error("received invalid member event message for event type", "event_type", e.EventType().String())
			return
		}

		for _, member := range memberEvent.Members {
			if err := n.removeMember(member); err != nil {
				n.logger.Error("could not remove member", "member_name", member.Name)
			}
		}

	// On EventMemberFailed we demote the node to non-voter, as it might or not recover.
	case serf.EventMemberFailed:
		memberEvent, ok := e.(serf.MemberEvent)
		if !ok {
			n.logger.Error("received invalid member event message for event type", "event_type", e.EventType().String())
			return
		}

		for _, member := range memberEvent.Members {
			if err := n.demoteMember(member); err != nil {
				n.logger.Error("could not demote member", "member_name", member.Name)
			}
		}
	}
}
func (n *node) addMember(member serf.Member) error {
	raftAddr, ok := member.Tags[raftAddrTag]
	if !ok {
		return errors.New("no raft_addr in member's tags")
	}

	if err := n.consensus.AddNode(member.Name, raftAddr); err != nil {
		return fmt.Errorf("failed to add node to consensus: %w", err)
	}

	return nil
}

func (n *node) demoteMember(member serf.Member) error {
	raftAddr, ok := member.Tags[raftAddrTag]
	if !ok {
		return errors.New("no raft_addr in member's tags")
	}

	if err := n.consensus.DemoteNode(member.Name, raftAddr); err != nil {
		return err
	}

	return nil
}

func (n *node) removeMember(member serf.Member) error {
	if err := n.consensus.RemoveNode(member.Name); err != nil {
		return fmt.Errorf("failed to remove member: %w", err)
	}

	return nil
}

var _ Node = &node{}
