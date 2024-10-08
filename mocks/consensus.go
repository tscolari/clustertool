// Code generated by mockery v2.43.2. DO NOT EDIT.

package mocks

import (
	raft "github.com/hashicorp/raft"
	mock "github.com/stretchr/testify/mock"

	time "time"
)

// Consensus is an autogenerated mock type for the Consensus type
type Consensus struct {
	mock.Mock
}

// AddNode provides a mock function with given fields: id, address
func (_m *Consensus) AddNode(id string, address string) error {
	ret := _m.Called(id, address)

	if len(ret) == 0 {
		panic("no return value specified for AddNode")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(string, string) error); ok {
		r0 = rf(id, address)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// Address provides a mock function with given fields:
func (_m *Consensus) Address() string {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for Address")
	}

	var r0 string
	if rf, ok := ret.Get(0).(func() string); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(string)
	}

	return r0
}

// Apply provides a mock function with given fields: cmd, timeout
func (_m *Consensus) Apply(cmd []byte, timeout time.Duration) error {
	ret := _m.Called(cmd, timeout)

	if len(ret) == 0 {
		panic("no return value specified for Apply")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func([]byte, time.Duration) error); ok {
		r0 = rf(cmd, timeout)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// DemoteNode provides a mock function with given fields: id, address
func (_m *Consensus) DemoteNode(id string, address string) error {
	ret := _m.Called(id, address)

	if len(ret) == 0 {
		panic("no return value specified for DemoteNode")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(string, string) error); ok {
		r0 = rf(id, address)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// Done provides a mock function with given fields:
func (_m *Consensus) Done() <-chan struct{} {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for Done")
	}

	var r0 <-chan struct{}
	if rf, ok := ret.Get(0).(func() <-chan struct{}); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(<-chan struct{})
		}
	}

	return r0
}

// IsLeader provides a mock function with given fields:
func (_m *Consensus) IsLeader() bool {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for IsLeader")
	}

	var r0 bool
	if rf, ok := ret.Get(0).(func() bool); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(bool)
	}

	return r0
}

// Leader provides a mock function with given fields:
func (_m *Consensus) Leader() (string, string) {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for Leader")
	}

	var r0 string
	var r1 string
	if rf, ok := ret.Get(0).(func() (string, string)); ok {
		return rf()
	}
	if rf, ok := ret.Get(0).(func() string); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(string)
	}

	if rf, ok := ret.Get(1).(func() string); ok {
		r1 = rf()
	} else {
		r1 = ret.Get(1).(string)
	}

	return r0, r1
}

// Name provides a mock function with given fields:
func (_m *Consensus) Name() string {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for Name")
	}

	var r0 string
	if rf, ok := ret.Get(0).(func() string); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(string)
	}

	return r0
}

// Nodes provides a mock function with given fields:
func (_m *Consensus) Nodes() ([]raft.Server, error) {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for Nodes")
	}

	var r0 []raft.Server
	var r1 error
	if rf, ok := ret.Get(0).(func() ([]raft.Server, error)); ok {
		return rf()
	}
	if rf, ok := ret.Get(0).(func() []raft.Server); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]raft.Server)
		}
	}

	if rf, ok := ret.Get(1).(func() error); ok {
		r1 = rf()
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// RemoveNode provides a mock function with given fields: id
func (_m *Consensus) RemoveNode(id string) error {
	ret := _m.Called(id)

	if len(ret) == 0 {
		panic("no return value specified for RemoveNode")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(string) error); ok {
		r0 = rf(id)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// Stop provides a mock function with given fields:
func (_m *Consensus) Stop() error {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for Stop")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func() error); ok {
		r0 = rf()
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// NewConsensus creates a new instance of Consensus. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewConsensus(t interface {
	mock.TestingT
	Cleanup(func())
}) *Consensus {
	mock := &Consensus{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
