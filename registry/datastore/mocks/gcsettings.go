// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/docker/distribution/registry/datastore (interfaces: GCSettingsStore)
//
// Generated by this command:
//
//	mockgen -package mocks -destination mocks/gcsettings.go . GCSettingsStore
//

// Package mocks is a generated GoMock package.
package mocks

import (
	context "context"
	reflect "reflect"
	time "time"

	gomock "go.uber.org/mock/gomock"
)

// MockGCSettingsStore is a mock of GCSettingsStore interface.
type MockGCSettingsStore struct {
	ctrl     *gomock.Controller
	recorder *MockGCSettingsStoreMockRecorder
}

// MockGCSettingsStoreMockRecorder is the mock recorder for MockGCSettingsStore.
type MockGCSettingsStoreMockRecorder struct {
	mock *MockGCSettingsStore
}

// NewMockGCSettingsStore creates a new mock instance.
func NewMockGCSettingsStore(ctrl *gomock.Controller) *MockGCSettingsStore {
	mock := &MockGCSettingsStore{ctrl: ctrl}
	mock.recorder = &MockGCSettingsStoreMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockGCSettingsStore) EXPECT() *MockGCSettingsStoreMockRecorder {
	return m.recorder
}

// UpdateAllReviewAfterDefaults mocks base method.
func (m *MockGCSettingsStore) UpdateAllReviewAfterDefaults(arg0 context.Context, arg1 time.Duration) (bool, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "UpdateAllReviewAfterDefaults", arg0, arg1)
	ret0, _ := ret[0].(bool)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// UpdateAllReviewAfterDefaults indicates an expected call of UpdateAllReviewAfterDefaults.
func (mr *MockGCSettingsStoreMockRecorder) UpdateAllReviewAfterDefaults(arg0, arg1 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "UpdateAllReviewAfterDefaults", reflect.TypeOf((*MockGCSettingsStore)(nil).UpdateAllReviewAfterDefaults), arg0, arg1)
}
