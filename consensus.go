package clustertool

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"github.com/stretchr/testify/require"
)

type ConsensusConfig struct {
	Addr string
	Raft *raft.Config

	LogCacheSize      int
	SnapshotCacheSize int
	DataDir           string

	NodeOperationsTimeout time.Duration

	Bootstrap bool
}

func (c ConsensusConfig) Validate() error {
	if c.Addr == "" {
		return errors.New("field Addr can't be empty")
	}

	if c.Raft == nil {
		return errors.New("field Raft can't be nil")
	}

	if c.DataDir == "" {
		return errors.New("field DataDir can't be empty")
	}

	return nil
}

func DefaultConsensusConfig() ConsensusConfig {
	raftConfig := raft.DefaultConfig()
	raftConfig.LogLevel = "WARN"
	return ConsensusConfig{
		Addr:              "127.0.0.1:9001",
		LogCacheSize:      1024,
		SnapshotCacheSize: 32,
		DataDir:           os.TempDir(),
		Bootstrap:         false,
		Raft:              raftConfig,
	}
}

type HashicorpRaft interface {
	Apply(cmd []byte, timeout time.Duration) raft.ApplyFuture
	LeaderWithID() (raft.ServerAddress, raft.ServerID)
	AddVoter(id raft.ServerID, addr raft.ServerAddress, prevIndex uint64, timeout time.Duration) raft.IndexFuture
	AddNonvoter(id raft.ServerID, addr raft.ServerAddress, prevIndex uint64, timeout time.Duration) raft.IndexFuture
	RemoveServer(id raft.ServerID, prevIndex uint64, timeout time.Duration) raft.IndexFuture
	GetConfiguration() raft.ConfigurationFuture
	LeadershipTransfer() raft.Future
	LeaderCh() <-chan bool
	Shutdown() raft.Future
}

// FSM defines the interface that the internal finite state machine must have.
// It will be called to apply changes in the state, as well as snapshotting and restoring it.
type FSM interface {
	raft.FSM
}

func NewConsensus(
	ctx context.Context,
	logger *slog.Logger,
	name string,
	fsm raft.FSM,
	config ConsensusConfig,
) (*consensus, error) {

	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("consensus configuration is invalid: %v", err)
	}

	logger = logger.With("node_name", name)
	c := consensus{
		config:   config,
		doneChan: make(chan struct{}),
	}

	c.internalCtx, c.stopInternalCtx = context.WithCancel(ctx)
	c.name = name
	c.address = config.Addr

	if err := c.init(logger, name, fsm); err != nil {
		return nil, fmt.Errorf("failed to initialize consensus: %w", err)
	}

	return &c, nil
}

type consensus struct {
	config ConsensusConfig
	logger *slog.Logger

	name    string
	address string

	fsm       raft.FSM
	raft      HashicorpRaft
	transport raft.Transport

	store       raft.StableStore
	logStore    raft.LogStore
	snapStore   raft.SnapshotStore
	closeStores func() error

	internalCtx     context.Context
	stopInternalCtx func()

	notifyChannel chan bool
	isLeader      atomic.Bool
	shutdown      atomic.Bool
	doneChan      chan struct{}
	doneErr       atomic.Pointer[error]
}

// Name returns the name of this node.
func (c *consensus) Name() string {
	return c.name
}

// Address returns the address of this node.
func (c *consensus) Address() string {
	return c.address
}

// Apply can only be performed by the leader.
// It will forward the given cmd to the wrapped FSM implementation.
func (c *consensus) Apply(cmd []byte, timeout time.Duration) error {
	if err := c.raft.Apply(cmd, timeout).Error(); err != nil {
		return fmt.Errorf("failed to apply command: %w", err)
	}

	return nil
}

// IsLeader will return true if this node is the leader of the cluster.
func (c *consensus) IsLeader() bool {
	return c.isLeader.Load()
}

// Leader returns the address and the name of the leader node.
func (c *consensus) Leader() (string, string) {
	address, id := c.raft.LeaderWithID()
	return string(address), string(id)
}

// AddNode can only be called by the leader.
// It will add a new node to the cluster.
func (c *consensus) AddNode(name, addr string) error {
	c.logger.Info("adding member", "member_name", name, "member_addr", addr)
	future := c.raft.AddVoter(raft.ServerID(name), raft.ServerAddress(addr), 0, c.config.NodeOperationsTimeout)
	if err := future.Error(); err != nil {
		return fmt.Errorf("failed to add voter: %w", err)
	}

	return nil
}

// DemoteNode can only be called by the leader.
// It will add or update a node to the cluster, but as a non-voter.
func (c *consensus) DemoteNode(name, addr string) error {
	c.logger.Info("demoting member", "member_name", name, "member_addr", addr)
	future := c.raft.AddNonvoter(raft.ServerID(name), raft.ServerAddress(addr), 0, c.config.NodeOperationsTimeout)
	if err := future.Error(); err != nil {
		return fmt.Errorf("failed to add non-voter: %w", err)
	}

	return nil
}

// RemoveNode can only be called by the leader.
// It will remove a node from the cluster.
func (c *consensus) RemoveNode(name string) error {
	c.logger.Info("removing member", "member_name", name)
	future := c.raft.RemoveServer(raft.ServerID(name), 0, c.config.NodeOperationsTimeout)
	if err := future.Error(); err != nil {
		return fmt.Errorf("failed to add voter: %w", err)
	}

	return nil
}

// Nodes will return all known nodes of the cluster.
func (c *consensus) Nodes() ([]raft.Server, error) {
	future := c.raft.GetConfiguration()
	if err := future.Error(); err != nil {
		return nil, fmt.Errorf("failed to get raft configuration: %w", err)
	}

	return future.Configuration().Servers, nil
}

// Stop will shutdown the node.
// It will try to transfer leadership first, if it's the leader.
// Stop will block until all closing operations are done.
func (c *consensus) Stop() error {
	if old := c.shutdown.Swap(true); old {
		<-c.doneChan

		c.logger.Info("already stopped")
		if errPtr := c.doneErr.Load(); errPtr != nil {
			return *errPtr
		}
		return nil
	}

	c.logger.Info("consensus shutting down")

	defer close(c.doneChan)

	if c.IsLeader() {
		c.logger.Info("transferring leadership")
		if err := c.raft.LeadershipTransfer().Error(); err != nil {
			c.logger.Error("failed to transfer leadership", "error", err)
		}
	}

	defer func() {
		if err := c.closeStores(); err != nil {
			c.logger.Error("failed to close store", "error", err)
		}
	}()

	c.stopInternalCtx()

	if err := c.raft.Shutdown().Error(); err != nil {
		c.doneErr.Store(&err)
		return fmt.Errorf("failed to shutdown raft: %w", err)
	}

	return nil
}

// Done returns a channel that blocks until the cluster is stopped.
func (c *consensus) Done() <-chan struct{} {
	return c.doneChan
}

func (c *consensus) init(logger *slog.Logger, name string, fsm raft.FSM) error {
	c.logger = logger.With("component", "consensus")
	c.fsm = fsm
	c.notifyChannel = make(chan bool, 1)

	c.logger.Info("initializing node")

	c.config.Raft.LocalID = raft.ServerID(name)

	if err := c.setupTransport(); err != nil {
		return fmt.Errorf("failed to setup transport: %w", err)
	}

	if err := c.setupStores(); err != nil {
		return fmt.Errorf("failed to setup internal storage: %w", err)
	}

	if err := c.bootstrap(); err != nil {
		return fmt.Errorf("failed to bootstrap node: %w", err)
	}

	if err := c.setupRaft(logger); err != nil {
		return fmt.Errorf("failed to setup raft: %w", err)
	}

	go c.watchForLeadership()
	go c.watchInternalCtx()

	return nil
}

func (c *consensus) setupTransport() error {
	transport, err := raft.NewTCPTransport(
		c.config.Addr,
		nil,
		5,
		10*time.Second,
		nil,
	)
	if err != nil {
		return fmt.Errorf("could not create TCP Transport: %w", err)
	}

	c.transport = transport
	c.logger.Info("binding to address", "address", c.transport.LocalAddr())

	return nil
}

func (c *consensus) setupStores() error {
	c.logger.Info("setting up store")

	path := filepath.Join(c.config.DataDir, "/raft")

	if err := os.MkdirAll(path, 0755); err != nil {
		return fmt.Errorf("failed to create data dir: %w", err)
	}

	c.logger.Debug("setting up bolddb storage")
	store, err := raftboltdb.NewBoltStore(filepath.Join(path, "raft.db"))
	if err != nil {
		return fmt.Errorf("failed to create disk storage: %w", err)
	}
	c.store = store
	c.closeStores = store.Close

	cacheStore, err := raft.NewLogCache(c.config.LogCacheSize, store)
	if err != nil {
		return fmt.Errorf("failed to create log cache store: %w", err)
	}
	c.logStore = cacheStore

	c.snapStore, err = raft.NewFileSnapshotStore(path, c.config.SnapshotCacheSize, nil)
	if err != nil {
		return fmt.Errorf("failed to create snapshot store: %w", err)
	}

	return nil
}

func (c *consensus) bootstrap() error {
	existingState, err := raft.HasExistingState(c.logStore, c.store, c.snapStore)
	if err != nil {
		return fmt.Errorf("failed to check for existing state: %w", err)
	}

	if existingState || !c.config.Bootstrap {
		return nil
	}

	c.logger.Info("bootstrapping")

	configuration := raft.Configuration{
		Servers: []raft.Server{
			{
				ID:      c.config.Raft.LocalID,
				Address: c.transport.LocalAddr(),
			},
		},
	}

	if err := raft.BootstrapCluster(c.config.Raft, c.logStore, c.store, c.snapStore, c.transport, configuration); err != nil {
		return fmt.Errorf("bootstrap cluster failed: %w", err)
	}

	return nil
}

func (c *consensus) setupRaft(logger *slog.Logger) error {
	c.logger.Debug("setting up raft")

	raftLogger := slog.NewLogLogger(
		logger.With("component", "consensus/raft").Handler(),
		slog.LevelWarn,
	)

	c.config.Raft.Logger = hclog.FromStandardLogger(raftLogger, &hclog.LoggerOptions{
		Level: hclog.Warn,
	})
	c.config.Raft.NotifyCh = c.notifyChannel

	raft, err := raft.NewRaft(c.config.Raft, c.fsm, c.logStore, c.store, c.snapStore, c.transport)
	if err != nil {
		return fmt.Errorf("failed to create raft: %w", err)
	}
	c.raft = raft
	return nil
}

func (c *consensus) watchForLeadership() {
	for {
		select {
		case <-c.internalCtx.Done():
			return
		case isLeader := <-c.notifyChannel:
			if isLeader {
				c.logger.Info("is now leader")
			} else if c.isLeader.Load() {
				c.logger.Info("is no longer leader")
			}
			c.isLeader.Store(isLeader)
		}
	}
}

func (c *consensus) watchInternalCtx() {
	<-c.internalCtx.Done()
	c.logger.Info("internal context closed")
	_ = c.Stop()
}

var _ Consensus = &consensus{}

func TestAttachMockToConsensus(t *testing.T, c *consensus, raft HashicorpRaft) {
	require.NoError(t, c.raft.Shutdown().Error())
	c.raft = raft
}
