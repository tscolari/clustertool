package clustertool

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/hashicorp/serf/serf"
)

const (
	serfEventBuffer = 1024
)

func NewDiscovery(ctx context.Context, logger *slog.Logger, name string, config *serf.Config) (*discovery, error) {
	d := discovery{
		name:          name,
		address:       fmt.Sprint(config.MemberlistConfig.BindAddr, ":", config.MemberlistConfig.BindPort),
		logger:        logger.With("compoenent", "discovery"),
		events:        make(chan serf.Event, serfEventBuffer),
		lock:          new(sync.RWMutex),
		subscriptions: map[serf.EventType][]func(serf.Event){},
		tags:          config.Tags,
		doneChan:      make(chan struct{}),
	}

	serf, err := d.init(ctx, config, d.events)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize Serf: %w", err)
	}

	d.serf = serf

	return &d, nil
}

type discovery struct {
	lock *sync.RWMutex

	name    string
	address string
	logger  *slog.Logger
	serf    *serf.Serf
	tags    map[string]string

	events chan serf.Event

	subscriptions map[serf.EventType][]func(serf.Event)
	doneChan      chan struct{}
	done          atomic.Bool
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

func (d *discovery) ConnectedNodes() []serf.Member {
	allMembers := d.serf.Members()
	members := make([]serf.Member, 0, len(allMembers))
	for _, member := range allMembers {
		// Consider nodes that are still considered part of the cluster, even though they are about to go or failing.
		if member.Status == serf.StatusAlive || member.Status == serf.StatusFailed || member.Status == serf.StatusLeaving {
			members = append(members, member)
		}
	}

	return members
}

func (d *discovery) Tags() map[string]string {
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

func (d *discovery) SendEvent(name string, payload []byte, params *serf.QueryParam) (*serf.QueryResponse, error) {
	result, err := d.serf.Query(name, payload, params)
	if err != nil {
		d.logger.Error("failed to send event", "error", err)
	}

	return result, err
}

func (d *discovery) SetTags(tags map[string]string) error {
	err := d.serf.SetTags(tags)
	if err != nil {
		d.logger.Error("failed to set tags", "error", err)
	}

	return err
}

func (d *discovery) Stop() error {
	if d.done.Load() {
		<-d.doneChan
		return nil
	}

	d.done.Store(true)
	defer close(d.doneChan)

	if err := d.serf.Leave(); err != nil {
		d.logger.Error("failed to leave cluster gracefully", "error", err)
	}

	if err := d.serf.Shutdown(); err != nil {
		d.logger.Error("failed to shutdown Serf", "error", err)
		return fmt.Errorf("failed to stop Serf: %w", err)
	}

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

func (d *discovery) Done() <-chan struct{} {
	return d.doneChan
}

func (d *discovery) init(ctx context.Context, config *serf.Config, ch chan serf.Event) (*serf.Serf, error) {
	config.Init()

	config.NodeName = d.name
	config.MemberlistConfig.Name = d.name
	config.EventCh = ch

	go d.serfEventHandler(ctx)

	serf, err := serf.Create(config)
	return serf, err
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

			d.lock.RLock()
			actions := d.subscriptions[e.EventType()]
			d.lock.RUnlock()

			for _, action := range actions {
				action(e)
			}
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

var _ Discovery = &discovery{}
