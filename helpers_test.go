package clustertool_test

import (
	"github.com/hashicorp/serf/serf"
	"github.com/tscolari/clustertool"
)

func testQueryParamToSerf(p *clustertool.QueryParam) *serf.QueryParam {
	qp := serf.QueryParam(*p)
	return &qp
}
