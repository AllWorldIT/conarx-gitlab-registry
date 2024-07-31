// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/docker/distribution/registry/datastore (interfaces: Handler,Transactor,LoadBalancer,Connector,DNSResolver)
//
// Generated by this command:
//
//	mockgen -package mocks -destination mocks/db.go . Handler,Transactor,LoadBalancer,Connector,DNSResolver
//

// Package mocks is a generated GoMock package.
package mocks

import (
	context "context"
	sql "database/sql"
	net "net"
	reflect "reflect"

	datastore "github.com/docker/distribution/registry/datastore"
	models "github.com/docker/distribution/registry/datastore/models"
	gomock "go.uber.org/mock/gomock"
)

// MockHandler is a mock of Handler interface.
type MockHandler struct {
	ctrl     *gomock.Controller
	recorder *MockHandlerMockRecorder
}

// MockHandlerMockRecorder is the mock recorder for MockHandler.
type MockHandlerMockRecorder struct {
	mock *MockHandler
}

// NewMockHandler creates a new mock instance.
func NewMockHandler(ctrl *gomock.Controller) *MockHandler {
	mock := &MockHandler{ctrl: ctrl}
	mock.recorder = &MockHandlerMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockHandler) EXPECT() *MockHandlerMockRecorder {
	return m.recorder
}

// BeginTx mocks base method.
func (m *MockHandler) BeginTx(arg0 context.Context, arg1 *sql.TxOptions) (datastore.Transactor, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "BeginTx", arg0, arg1)
	ret0, _ := ret[0].(datastore.Transactor)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// BeginTx indicates an expected call of BeginTx.
func (mr *MockHandlerMockRecorder) BeginTx(arg0, arg1 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "BeginTx", reflect.TypeOf((*MockHandler)(nil).BeginTx), arg0, arg1)
}

// Close mocks base method.
func (m *MockHandler) Close() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Close")
	ret0, _ := ret[0].(error)
	return ret0
}

// Close indicates an expected call of Close.
func (mr *MockHandlerMockRecorder) Close() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Close", reflect.TypeOf((*MockHandler)(nil).Close))
}

// ExecContext mocks base method.
func (m *MockHandler) ExecContext(arg0 context.Context, arg1 string, arg2 ...any) (sql.Result, error) {
	m.ctrl.T.Helper()
	varargs := []any{arg0, arg1}
	for _, a := range arg2 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "ExecContext", varargs...)
	ret0, _ := ret[0].(sql.Result)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ExecContext indicates an expected call of ExecContext.
func (mr *MockHandlerMockRecorder) ExecContext(arg0, arg1 any, arg2 ...any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]any{arg0, arg1}, arg2...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ExecContext", reflect.TypeOf((*MockHandler)(nil).ExecContext), varargs...)
}

// QueryContext mocks base method.
func (m *MockHandler) QueryContext(arg0 context.Context, arg1 string, arg2 ...any) (*sql.Rows, error) {
	m.ctrl.T.Helper()
	varargs := []any{arg0, arg1}
	for _, a := range arg2 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "QueryContext", varargs...)
	ret0, _ := ret[0].(*sql.Rows)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// QueryContext indicates an expected call of QueryContext.
func (mr *MockHandlerMockRecorder) QueryContext(arg0, arg1 any, arg2 ...any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]any{arg0, arg1}, arg2...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "QueryContext", reflect.TypeOf((*MockHandler)(nil).QueryContext), varargs...)
}

// QueryRowContext mocks base method.
func (m *MockHandler) QueryRowContext(arg0 context.Context, arg1 string, arg2 ...any) *sql.Row {
	m.ctrl.T.Helper()
	varargs := []any{arg0, arg1}
	for _, a := range arg2 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "QueryRowContext", varargs...)
	ret0, _ := ret[0].(*sql.Row)
	return ret0
}

// QueryRowContext indicates an expected call of QueryRowContext.
func (mr *MockHandlerMockRecorder) QueryRowContext(arg0, arg1 any, arg2 ...any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]any{arg0, arg1}, arg2...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "QueryRowContext", reflect.TypeOf((*MockHandler)(nil).QueryRowContext), varargs...)
}

// Stats mocks base method.
func (m *MockHandler) Stats() sql.DBStats {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Stats")
	ret0, _ := ret[0].(sql.DBStats)
	return ret0
}

// Stats indicates an expected call of Stats.
func (mr *MockHandlerMockRecorder) Stats() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Stats", reflect.TypeOf((*MockHandler)(nil).Stats))
}

// MockTransactor is a mock of Transactor interface.
type MockTransactor struct {
	ctrl     *gomock.Controller
	recorder *MockTransactorMockRecorder
}

// MockTransactorMockRecorder is the mock recorder for MockTransactor.
type MockTransactorMockRecorder struct {
	mock *MockTransactor
}

// NewMockTransactor creates a new mock instance.
func NewMockTransactor(ctrl *gomock.Controller) *MockTransactor {
	mock := &MockTransactor{ctrl: ctrl}
	mock.recorder = &MockTransactorMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockTransactor) EXPECT() *MockTransactorMockRecorder {
	return m.recorder
}

// Commit mocks base method.
func (m *MockTransactor) Commit() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Commit")
	ret0, _ := ret[0].(error)
	return ret0
}

// Commit indicates an expected call of Commit.
func (mr *MockTransactorMockRecorder) Commit() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Commit", reflect.TypeOf((*MockTransactor)(nil).Commit))
}

// ExecContext mocks base method.
func (m *MockTransactor) ExecContext(arg0 context.Context, arg1 string, arg2 ...any) (sql.Result, error) {
	m.ctrl.T.Helper()
	varargs := []any{arg0, arg1}
	for _, a := range arg2 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "ExecContext", varargs...)
	ret0, _ := ret[0].(sql.Result)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ExecContext indicates an expected call of ExecContext.
func (mr *MockTransactorMockRecorder) ExecContext(arg0, arg1 any, arg2 ...any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]any{arg0, arg1}, arg2...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ExecContext", reflect.TypeOf((*MockTransactor)(nil).ExecContext), varargs...)
}

// QueryContext mocks base method.
func (m *MockTransactor) QueryContext(arg0 context.Context, arg1 string, arg2 ...any) (*sql.Rows, error) {
	m.ctrl.T.Helper()
	varargs := []any{arg0, arg1}
	for _, a := range arg2 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "QueryContext", varargs...)
	ret0, _ := ret[0].(*sql.Rows)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// QueryContext indicates an expected call of QueryContext.
func (mr *MockTransactorMockRecorder) QueryContext(arg0, arg1 any, arg2 ...any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]any{arg0, arg1}, arg2...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "QueryContext", reflect.TypeOf((*MockTransactor)(nil).QueryContext), varargs...)
}

// QueryRowContext mocks base method.
func (m *MockTransactor) QueryRowContext(arg0 context.Context, arg1 string, arg2 ...any) *sql.Row {
	m.ctrl.T.Helper()
	varargs := []any{arg0, arg1}
	for _, a := range arg2 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "QueryRowContext", varargs...)
	ret0, _ := ret[0].(*sql.Row)
	return ret0
}

// QueryRowContext indicates an expected call of QueryRowContext.
func (mr *MockTransactorMockRecorder) QueryRowContext(arg0, arg1 any, arg2 ...any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]any{arg0, arg1}, arg2...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "QueryRowContext", reflect.TypeOf((*MockTransactor)(nil).QueryRowContext), varargs...)
}

// Rollback mocks base method.
func (m *MockTransactor) Rollback() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Rollback")
	ret0, _ := ret[0].(error)
	return ret0
}

// Rollback indicates an expected call of Rollback.
func (mr *MockTransactorMockRecorder) Rollback() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Rollback", reflect.TypeOf((*MockTransactor)(nil).Rollback))
}

// MockLoadBalancer is a mock of LoadBalancer interface.
type MockLoadBalancer struct {
	ctrl     *gomock.Controller
	recorder *MockLoadBalancerMockRecorder
}

// MockLoadBalancerMockRecorder is the mock recorder for MockLoadBalancer.
type MockLoadBalancerMockRecorder struct {
	mock *MockLoadBalancer
}

// NewMockLoadBalancer creates a new mock instance.
func NewMockLoadBalancer(ctrl *gomock.Controller) *MockLoadBalancer {
	mock := &MockLoadBalancer{ctrl: ctrl}
	mock.recorder = &MockLoadBalancerMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockLoadBalancer) EXPECT() *MockLoadBalancerMockRecorder {
	return m.recorder
}

// Close mocks base method.
func (m *MockLoadBalancer) Close() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Close")
	ret0, _ := ret[0].(error)
	return ret0
}

// Close indicates an expected call of Close.
func (mr *MockLoadBalancerMockRecorder) Close() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Close", reflect.TypeOf((*MockLoadBalancer)(nil).Close))
}

// Primary mocks base method.
func (m *MockLoadBalancer) Primary() *datastore.DB {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Primary")
	ret0, _ := ret[0].(*datastore.DB)
	return ret0
}

// Primary indicates an expected call of Primary.
func (mr *MockLoadBalancerMockRecorder) Primary() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Primary", reflect.TypeOf((*MockLoadBalancer)(nil).Primary))
}

// RecordLSN mocks base method.
func (m *MockLoadBalancer) RecordLSN(arg0 context.Context, arg1 *models.Repository) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "RecordLSN", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

// RecordLSN indicates an expected call of RecordLSN.
func (mr *MockLoadBalancerMockRecorder) RecordLSN(arg0, arg1 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RecordLSN", reflect.TypeOf((*MockLoadBalancer)(nil).RecordLSN), arg0, arg1)
}

// Replica mocks base method.
func (m *MockLoadBalancer) Replica() *datastore.DB {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Replica")
	ret0, _ := ret[0].(*datastore.DB)
	return ret0
}

// Replica indicates an expected call of Replica.
func (mr *MockLoadBalancerMockRecorder) Replica() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Replica", reflect.TypeOf((*MockLoadBalancer)(nil).Replica))
}

// Replicas mocks base method.
func (m *MockLoadBalancer) Replicas() []*datastore.DB {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Replicas")
	ret0, _ := ret[0].([]*datastore.DB)
	return ret0
}

// Replicas indicates an expected call of Replicas.
func (mr *MockLoadBalancerMockRecorder) Replicas() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Replicas", reflect.TypeOf((*MockLoadBalancer)(nil).Replicas))
}

// MockConnector is a mock of Connector interface.
type MockConnector struct {
	ctrl     *gomock.Controller
	recorder *MockConnectorMockRecorder
}

// MockConnectorMockRecorder is the mock recorder for MockConnector.
type MockConnectorMockRecorder struct {
	mock *MockConnector
}

// NewMockConnector creates a new mock instance.
func NewMockConnector(ctrl *gomock.Controller) *MockConnector {
	mock := &MockConnector{ctrl: ctrl}
	mock.recorder = &MockConnectorMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockConnector) EXPECT() *MockConnectorMockRecorder {
	return m.recorder
}

// Open mocks base method.
func (m *MockConnector) Open(arg0 context.Context, arg1 *datastore.DSN, arg2 ...datastore.Option) (*datastore.DB, error) {
	m.ctrl.T.Helper()
	varargs := []any{arg0, arg1}
	for _, a := range arg2 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "Open", varargs...)
	ret0, _ := ret[0].(*datastore.DB)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Open indicates an expected call of Open.
func (mr *MockConnectorMockRecorder) Open(arg0, arg1 any, arg2 ...any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]any{arg0, arg1}, arg2...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Open", reflect.TypeOf((*MockConnector)(nil).Open), varargs...)
}

// MockDNSResolver is a mock of DNSResolver interface.
type MockDNSResolver struct {
	ctrl     *gomock.Controller
	recorder *MockDNSResolverMockRecorder
}

// MockDNSResolverMockRecorder is the mock recorder for MockDNSResolver.
type MockDNSResolverMockRecorder struct {
	mock *MockDNSResolver
}

// NewMockDNSResolver creates a new mock instance.
func NewMockDNSResolver(ctrl *gomock.Controller) *MockDNSResolver {
	mock := &MockDNSResolver{ctrl: ctrl}
	mock.recorder = &MockDNSResolverMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockDNSResolver) EXPECT() *MockDNSResolverMockRecorder {
	return m.recorder
}

// LookupHost mocks base method.
func (m *MockDNSResolver) LookupHost(arg0 context.Context, arg1 string) ([]string, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "LookupHost", arg0, arg1)
	ret0, _ := ret[0].([]string)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// LookupHost indicates an expected call of LookupHost.
func (mr *MockDNSResolverMockRecorder) LookupHost(arg0, arg1 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "LookupHost", reflect.TypeOf((*MockDNSResolver)(nil).LookupHost), arg0, arg1)
}

// LookupSRV mocks base method.
func (m *MockDNSResolver) LookupSRV(arg0 context.Context) ([]*net.SRV, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "LookupSRV", arg0)
	ret0, _ := ret[0].([]*net.SRV)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// LookupSRV indicates an expected call of LookupSRV.
func (mr *MockDNSResolverMockRecorder) LookupSRV(arg0 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "LookupSRV", reflect.TypeOf((*MockDNSResolver)(nil).LookupSRV), arg0)
}
