package clustertool

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/hashicorp/serf/serf"
	"github.com/stretchr/testify/require"
)

const (
	serfEventBuffer = 1024
)

type DiscoveryMemberStatus = serf.MemberStatus

// HashicorpSerf is an interface that exposes the methods used by discovery
// from serf.Serf.
type HashicorpSerf interface {
	KeyManager() *serf.KeyManager
	Members() []serf.Member
	Query(name string, payload []byte, params *serf.QueryParam) (*serf.QueryResponse, error)
	SetTags(tags map[string]string) error
	Leave() error
	Shutdown() error
	Join(addresses []string, ignoreOld bool) (int, error)
}

type DiscoveryMember struct {
	Name   string
	Addr   net.IP
	Port   uint16
	Tags   map[string]string
	Status DiscoveryMemberStatus
}

func discoveryMemberFromSerf(m serf.Member) DiscoveryMember {
	return DiscoveryMember{
		Name:   m.Name,
		Addr:   m.Addr,
		Port:   m.Port,
		Tags:   m.Tags,
		Status: m.Status,
	}
}

func NewDiscovery(ctx context.Context, logger *slog.Logger, name string, config *serf.Config) (*discovery, error) {
	d := discovery{
		name:          name,
		address:       fmt.Sprint(config.MemberlistConfig.BindAddr, ":", config.MemberlistConfig.BindPort),
		logger:        logger.With("compoenent", "discovery", "node_name", name),
		events:        make(chan serf.Event, serfEventBuffer),
		lock:          new(sync.RWMutex),
		subscriptions: map[serf.EventType][]func(serf.Event){},
		tags:          config.Tags,
		doneChan:      make(chan struct{}),
	}

	config.NodeName = d.name
	config.MemberlistConfig.Name = d.name
	config.EventCh = d.events

	config.Init()
	serf, err := serf.Create(config)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize Serf: %w", err)
	}

	d.serf = serf
	go d.serfEventHandler(ctx)

	return &d, nil
}

type discovery struct {
	lock *sync.RWMutex

	name    string
	address string
	logger  *slog.Logger
	serf    HashicorpSerf
	tags    map[string]string

	events chan serf.Event

	subscriptions map[serf.EventType][]func(serf.Event)
	doneChan      chan struct{}
	done          atomic.Bool
	doneErr       atomic.Pointer[error]
}

func (d *discovery) Name() string {
	return d.name
}

func (d *discovery) Address() string {
	return d.address
}

func (d *discovery) KeyManager() *serf.KeyManager {
	return d.serf.KeyManager()
}

func (d *discovery) ConnectedNodes() []DiscoveryMember {
	allMembers := d.serf.Members()
	members := make([]DiscoveryMember, 0, len(allMembers))
	for _, member := range allMembers {
		// Consider nodes that are still considered part of the cluster, even though they are about to go or failing.
		if member.Status == serf.StatusAlive || member.Status == serf.StatusFailed || member.Status == serf.StatusLeaving {
			members = append(members, discoveryMemberFromSerf(member))
		}
	}

	return members
}

func (d *discovery) Tags() map[string]string {
	d.lock.RLock()
	defer d.lock.RUnlock()
	return d.tags
}

func (d *discovery) SubscribeToEvent(et serf.EventType, action func(serf.Event)) {
	d.lock.Lock()
	defer d.lock.Unlock()

	actions, ok := d.subscriptions[et]
	if !ok {
		d.subscriptions[et] = []func(serf.Event){action}
		return
	}

	actions = append(actions, action)
	d.subscriptions[et] = actions
}

func (d *discovery) SendEvent(name string, payload []byte, params *QueryParam) (*serf.QueryResponse, error) {
	qp := serf.QueryParam(*params)
	result, err := d.serf.Query(name, payload, &qp)
	if err != nil {
		d.logger.Error("failed to send event", "error", err)
	}

	return result, err
}

func (d *discovery) SetTags(tags map[string]string) error {
	d.lock.Lock()
	defer d.lock.Unlock()

	err := d.serf.SetTags(tags)
	if err != nil {
		d.logger.Error("failed to set tags", "error", err)
		return err
	}

	d.tags = tags

	return nil
}

func (d *discovery) JoinNodes(addresses ...string) error {
	joinedCount, err := d.serf.Join(addresses, true)
	if err != nil {
		d.logger.Error("failed to join nodes", "error", err)
		return err
	}

	d.logger.Info("joined nodes", "nodes_count", joinedCount)
	return nil
}

func (d *discovery) Stop() error {
	if old := d.done.Swap(true); old {
		<-d.doneChan

		d.logger.Info("already stopped")
		if errPtr := d.doneErr.Load(); errPtr != nil {
			return *errPtr
		}

		return nil
	}

	d.logger.Info("discovery shutting down")
	defer close(d.doneChan)

	if err := d.serf.Leave(); err != nil {
		d.logger.Error("failed to leave cluster gracefully", "error", err)
	}

	if err := d.serf.Shutdown(); err != nil {
		d.logger.Error("failed to shutdown Serf", "error", err)
		d.doneErr.Store(&err)
		return fmt.Errorf("failed to stop Serf: %w", err)
	}

	return nil
}

func (d *discovery) Done() <-chan struct{} {
	return d.doneChan
}

func (d *discovery) serfEventHandler(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			_ = d.Stop()
			return

		case e := <-d.events:
			switch e.EventType() {
			case serf.EventMemberJoin:
				d.memberJoined(e.(serf.MemberEvent))

			case serf.EventMemberLeave, serf.EventMemberFailed:
				d.memberLeft(e.(serf.MemberEvent))
			}

			func() {
				d.lock.RLock()
				defer d.lock.RUnlock()

				actions := d.subscriptions[e.EventType()]
				for _, action := range actions {
					action(e)
				}
			}()
		}
	}
}

func (d *discovery) memberJoined(event serf.MemberEvent) {
	for _, m := range event.Members {
		d.logger.Info("adding member", "member_name", m.Name, "member_addr", m.Addr, "member_port", m.Port)

	}
}

func (d *discovery) memberLeft(event serf.MemberEvent) {
	for _, m := range event.Members {
		d.logger.Info("removing member", "member_name", m.Name, "member_addr", m.Addr, "member_port", m.Port)

	}
}

func TestAttachMockToDiscovery(t *testing.T, d *discovery, serf HashicorpSerf, eventsCh chan serf.Event) {
	require.NoError(t, d.serf.Shutdown())
	d.serf = serf
	d.events = eventsCh
}

var _ Discovery = &discovery{}
