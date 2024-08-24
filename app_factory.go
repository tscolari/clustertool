package clustertool

import (
	"context"
	"errors"
	"log/slog"

	"github.com/hashicorp/serf/serf"
)

type Factory struct {
	ctx            context.Context
	name           string
	logger         *slog.Logger
	loggerOverride bool

	serfConfig      *serf.Config
	consensusConfig *ConsensusConfig
	fsm             FSM
	appOptions      *AppOptions

	consensus Consensus
	discovery Discovery
}

func (f Factory) WithContext(ctx context.Context) Factory {
	newF := f
	newF.ctx = ctx
	return newF
}

func (f Factory) WithName(name string) Factory {
	newF := f
	newF.name = name
	return newF
}

func (f Factory) WithLogger(logger *slog.Logger, overrideInternals bool) Factory {
	newF := f
	newF.logger = logger
	newF.loggerOverride = overrideInternals
	return newF
}

func (f Factory) WithSerfConfig(config *serf.Config) Factory {
	newF := f
	newF.serfConfig = config
	return newF
}

func (f Factory) WithFSM(fsm FSM) Factory {
	newF := f
	newF.fsm = fsm
	return newF
}

func (f Factory) WithConsensusConfig(config ConsensusConfig) Factory {
	newF := f
	newF.consensusConfig = &config
	return newF
}

func (f Factory) WithAppOptions(options AppOptions) Factory {
	newF := f
	newF.appOptions = &options
	return newF
}

func (f Factory) WithDiscovery(d Discovery) Factory {
	newF := f
	newF.discovery = d
	return newF
}

func (f Factory) WithConsensus(c Consensus) Factory {
	newF := f
	newF.consensus = c
	return newF
}

func (f Factory) Build() (App, error) {
	if f.name == "" {
		return nil, errors.New("name must be defined")
	}

	if f.fsm == nil {
		return nil, errors.New("fsm must be defined")
	}

	logger := slog.Default()
	if f.logger != nil {
		logger = f.logger
	}

	ctx := context.Background()
	if f.ctx != nil {
		ctx = f.ctx
	}

	consensus := f.consensus
	if consensus == nil {
		var consensusConfig = DefaultConsensusConfig()
		if f.consensusConfig != nil {
			consensusConfig = *f.consensusConfig
		}

		var err error
		consensus, err = NewConsensus(ctx, logger, f.name, f.fsm, consensusConfig)
		if err != nil {
			return nil, err
		}
	}

	discovery := f.discovery
	if discovery == nil {
		serfConfig := serf.DefaultConfig()
		if f.serfConfig != nil {
			serfConfig = f.serfConfig
		}

		var err error
		discovery, err = NewDiscovery(ctx, logger, f.name, serfConfig)
		if err != nil {
			return nil, err
		}
	}

	node, err := NewNode(ctx, logger, discovery, consensus)
	if err != nil {
		return nil, err
	}

	appOptions := DefaultAppOptions()
	if f.appOptions != nil {
		appOptions = *f.appOptions
	}

	app, err := NewApp(ctx, logger, node, appOptions)

	return app, err
}
