// Code generated by mockery v2.35.3. DO NOT EDIT.

package clustertool

import mock "github.com/stretchr/testify/mock"

// Mockaddressable is an autogenerated mock type for the addressable type
type Mockaddressable struct {
	mock.Mock
}

// Address provides a mock function with given fields:
func (_m *Mockaddressable) Address() string {
	ret := _m.Called()

	var r0 string
	if rf, ok := ret.Get(0).(func() string); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(string)
	}

	return r0
}

// Name provides a mock function with given fields:
func (_m *Mockaddressable) Name() string {
	ret := _m.Called()

	var r0 string
	if rf, ok := ret.Get(0).(func() string); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(string)
	}

	return r0
}

// NewMockaddressable creates a new instance of Mockaddressable. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewMockaddressable(t interface {
	mock.TestingT
	Cleanup(func())
}) *Mockaddressable {
	mock := &Mockaddressable{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
