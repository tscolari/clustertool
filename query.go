package clustertool

import (
	"time"

	"github.com/hashicorp/serf/serf"
)

type Query interface {
	Name() string
	Payload() []byte
	SourceNode() string
	Deadline() time.Time
	Respond(buf []byte) error
}

type query struct {
	*serf.Query
}

func (q *query) Name() string {
	return q.Query.Name
}

func (q *query) Payload() []byte {
	return q.Query.Payload
}

type QueryParam serf.QueryParam
