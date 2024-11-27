// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/docker/distribution/registry/datastore (interfaces: BackgroundMigrationStore)
//
// Generated by this command:
//
//	mockgen -package mocks -destination mocks/backgroundmigration.go . BackgroundMigrationStore
//

// Package mocks is a generated GoMock package.
package mocks

import (
	context "context"
	reflect "reflect"

	models "github.com/docker/distribution/registry/datastore/models"
	gomock "go.uber.org/mock/gomock"
)

// MockBackgroundMigrationStore is a mock of BackgroundMigrationStore interface.
type MockBackgroundMigrationStore struct {
	ctrl     *gomock.Controller
	recorder *MockBackgroundMigrationStoreMockRecorder
}

// MockBackgroundMigrationStoreMockRecorder is the mock recorder for MockBackgroundMigrationStore.
type MockBackgroundMigrationStoreMockRecorder struct {
	mock *MockBackgroundMigrationStore
}

// NewMockBackgroundMigrationStore creates a new mock instance.
func NewMockBackgroundMigrationStore(ctrl *gomock.Controller) *MockBackgroundMigrationStore {
	mock := &MockBackgroundMigrationStore{ctrl: ctrl}
	mock.recorder = &MockBackgroundMigrationStoreMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockBackgroundMigrationStore) EXPECT() *MockBackgroundMigrationStoreMockRecorder {
	return m.recorder
}

// AreFinished mocks base method.
func (m *MockBackgroundMigrationStore) AreFinished(arg0 context.Context, arg1 []string) (bool, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "AreFinished", arg0, arg1)
	ret0, _ := ret[0].(bool)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// AreFinished indicates an expected call of AreFinished.
func (mr *MockBackgroundMigrationStoreMockRecorder) AreFinished(arg0, arg1 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "AreFinished", reflect.TypeOf((*MockBackgroundMigrationStore)(nil).AreFinished), arg0, arg1)
}

// CreateNewJob mocks base method.
func (m *MockBackgroundMigrationStore) CreateNewJob(arg0 context.Context, arg1 *models.BackgroundMigrationJob) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CreateNewJob", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

// CreateNewJob indicates an expected call of CreateNewJob.
func (mr *MockBackgroundMigrationStoreMockRecorder) CreateNewJob(arg0, arg1 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CreateNewJob", reflect.TypeOf((*MockBackgroundMigrationStore)(nil).CreateNewJob), arg0, arg1)
}

// ExistsColumn mocks base method.
func (m *MockBackgroundMigrationStore) ExistsColumn(arg0 context.Context, arg1, arg2, arg3 string) (bool, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ExistsColumn", arg0, arg1, arg2, arg3)
	ret0, _ := ret[0].(bool)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ExistsColumn indicates an expected call of ExistsColumn.
func (mr *MockBackgroundMigrationStoreMockRecorder) ExistsColumn(arg0, arg1, arg2, arg3 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ExistsColumn", reflect.TypeOf((*MockBackgroundMigrationStore)(nil).ExistsColumn), arg0, arg1, arg2, arg3)
}

// ExistsTable mocks base method.
func (m *MockBackgroundMigrationStore) ExistsTable(arg0 context.Context, arg1, arg2 string) (bool, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ExistsTable", arg0, arg1, arg2)
	ret0, _ := ret[0].(bool)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ExistsTable indicates an expected call of ExistsTable.
func (mr *MockBackgroundMigrationStoreMockRecorder) ExistsTable(arg0, arg1, arg2 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ExistsTable", reflect.TypeOf((*MockBackgroundMigrationStore)(nil).ExistsTable), arg0, arg1, arg2)
}

// FindAll mocks base method.
func (m *MockBackgroundMigrationStore) FindAll(arg0 context.Context) (models.BackgroundMigrations, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "FindAll", arg0)
	ret0, _ := ret[0].(models.BackgroundMigrations)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// FindAll indicates an expected call of FindAll.
func (mr *MockBackgroundMigrationStoreMockRecorder) FindAll(arg0 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "FindAll", reflect.TypeOf((*MockBackgroundMigrationStore)(nil).FindAll), arg0)
}

// FindById mocks base method.
func (m *MockBackgroundMigrationStore) FindById(arg0 context.Context, arg1 int) (*models.BackgroundMigration, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "FindById", arg0, arg1)
	ret0, _ := ret[0].(*models.BackgroundMigration)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// FindById indicates an expected call of FindById.
func (mr *MockBackgroundMigrationStoreMockRecorder) FindById(arg0, arg1 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "FindById", reflect.TypeOf((*MockBackgroundMigrationStore)(nil).FindById), arg0, arg1)
}

// FindByName mocks base method.
func (m *MockBackgroundMigrationStore) FindByName(arg0 context.Context, arg1 string) (*models.BackgroundMigration, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "FindByName", arg0, arg1)
	ret0, _ := ret[0].(*models.BackgroundMigration)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// FindByName indicates an expected call of FindByName.
func (mr *MockBackgroundMigrationStoreMockRecorder) FindByName(arg0, arg1 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "FindByName", reflect.TypeOf((*MockBackgroundMigrationStore)(nil).FindByName), arg0, arg1)
}

// FindJobEndFromJobStart mocks base method.
func (m *MockBackgroundMigrationStore) FindJobEndFromJobStart(arg0 context.Context, arg1, arg2 string, arg3, arg4, arg5 int) (int, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "FindJobEndFromJobStart", arg0, arg1, arg2, arg3, arg4, arg5)
	ret0, _ := ret[0].(int)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// FindJobEndFromJobStart indicates an expected call of FindJobEndFromJobStart.
func (mr *MockBackgroundMigrationStoreMockRecorder) FindJobEndFromJobStart(arg0, arg1, arg2, arg3, arg4, arg5 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "FindJobEndFromJobStart", reflect.TypeOf((*MockBackgroundMigrationStore)(nil).FindJobEndFromJobStart), arg0, arg1, arg2, arg3, arg4, arg5)
}

// FindJobWithEndID mocks base method.
func (m *MockBackgroundMigrationStore) FindJobWithEndID(arg0 context.Context, arg1, arg2 int) (*models.BackgroundMigrationJob, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "FindJobWithEndID", arg0, arg1, arg2)
	ret0, _ := ret[0].(*models.BackgroundMigrationJob)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// FindJobWithEndID indicates an expected call of FindJobWithEndID.
func (mr *MockBackgroundMigrationStoreMockRecorder) FindJobWithEndID(arg0, arg1, arg2 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "FindJobWithEndID", reflect.TypeOf((*MockBackgroundMigrationStore)(nil).FindJobWithEndID), arg0, arg1, arg2)
}

// FindJobWithStatus mocks base method.
func (m *MockBackgroundMigrationStore) FindJobWithStatus(arg0 context.Context, arg1 int, arg2 models.BackgroundMigrationStatus) (*models.BackgroundMigrationJob, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "FindJobWithStatus", arg0, arg1, arg2)
	ret0, _ := ret[0].(*models.BackgroundMigrationJob)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// FindJobWithStatus indicates an expected call of FindJobWithStatus.
func (mr *MockBackgroundMigrationStoreMockRecorder) FindJobWithStatus(arg0, arg1, arg2 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "FindJobWithStatus", reflect.TypeOf((*MockBackgroundMigrationStore)(nil).FindJobWithStatus), arg0, arg1, arg2)
}

// FindLastJob mocks base method.
func (m *MockBackgroundMigrationStore) FindLastJob(arg0 context.Context, arg1 *models.BackgroundMigration) (*models.BackgroundMigrationJob, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "FindLastJob", arg0, arg1)
	ret0, _ := ret[0].(*models.BackgroundMigrationJob)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// FindLastJob indicates an expected call of FindLastJob.
func (mr *MockBackgroundMigrationStoreMockRecorder) FindLastJob(arg0, arg1 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "FindLastJob", reflect.TypeOf((*MockBackgroundMigrationStore)(nil).FindLastJob), arg0, arg1)
}

// FindNext mocks base method.
func (m *MockBackgroundMigrationStore) FindNext(arg0 context.Context) (*models.BackgroundMigration, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "FindNext", arg0)
	ret0, _ := ret[0].(*models.BackgroundMigration)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// FindNext indicates an expected call of FindNext.
func (mr *MockBackgroundMigrationStoreMockRecorder) FindNext(arg0 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "FindNext", reflect.TypeOf((*MockBackgroundMigrationStore)(nil).FindNext), arg0)
}

// FindNextByStatus mocks base method.
func (m *MockBackgroundMigrationStore) FindNextByStatus(arg0 context.Context, arg1 models.BackgroundMigrationStatus) (*models.BackgroundMigration, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "FindNextByStatus", arg0, arg1)
	ret0, _ := ret[0].(*models.BackgroundMigration)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// FindNextByStatus indicates an expected call of FindNextByStatus.
func (mr *MockBackgroundMigrationStoreMockRecorder) FindNextByStatus(arg0, arg1 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "FindNextByStatus", reflect.TypeOf((*MockBackgroundMigrationStore)(nil).FindNextByStatus), arg0, arg1)
}

// IncrementJobAttempts mocks base method.
func (m *MockBackgroundMigrationStore) IncrementJobAttempts(arg0 context.Context, arg1 int) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "IncrementJobAttempts", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

// IncrementJobAttempts indicates an expected call of IncrementJobAttempts.
func (mr *MockBackgroundMigrationStoreMockRecorder) IncrementJobAttempts(arg0, arg1 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "IncrementJobAttempts", reflect.TypeOf((*MockBackgroundMigrationStore)(nil).IncrementJobAttempts), arg0, arg1)
}

// Lock mocks base method.
func (m *MockBackgroundMigrationStore) Lock(arg0 context.Context) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Lock", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// Lock indicates an expected call of Lock.
func (mr *MockBackgroundMigrationStoreMockRecorder) Lock(arg0 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Lock", reflect.TypeOf((*MockBackgroundMigrationStore)(nil).Lock), arg0)
}

// Pause mocks base method.
func (m *MockBackgroundMigrationStore) Pause(arg0 context.Context) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Pause", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// Pause indicates an expected call of Pause.
func (mr *MockBackgroundMigrationStoreMockRecorder) Pause(arg0 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Pause", reflect.TypeOf((*MockBackgroundMigrationStore)(nil).Pause), arg0)
}

// Resume mocks base method.
func (m *MockBackgroundMigrationStore) Resume(arg0 context.Context) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Resume", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// Resume indicates an expected call of Resume.
func (mr *MockBackgroundMigrationStoreMockRecorder) Resume(arg0 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Resume", reflect.TypeOf((*MockBackgroundMigrationStore)(nil).Resume), arg0)
}

// SyncLock mocks base method.
func (m *MockBackgroundMigrationStore) SyncLock(arg0 context.Context) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SyncLock", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// SyncLock indicates an expected call of SyncLock.
func (mr *MockBackgroundMigrationStoreMockRecorder) SyncLock(arg0 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SyncLock", reflect.TypeOf((*MockBackgroundMigrationStore)(nil).SyncLock), arg0)
}

// UpdateJobStatus mocks base method.
func (m *MockBackgroundMigrationStore) UpdateJobStatus(arg0 context.Context, arg1 *models.BackgroundMigrationJob) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "UpdateJobStatus", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

// UpdateJobStatus indicates an expected call of UpdateJobStatus.
func (mr *MockBackgroundMigrationStoreMockRecorder) UpdateJobStatus(arg0, arg1 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "UpdateJobStatus", reflect.TypeOf((*MockBackgroundMigrationStore)(nil).UpdateJobStatus), arg0, arg1)
}

// UpdateStatus mocks base method.
func (m *MockBackgroundMigrationStore) UpdateStatus(arg0 context.Context, arg1 *models.BackgroundMigration) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "UpdateStatus", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

// UpdateStatus indicates an expected call of UpdateStatus.
func (mr *MockBackgroundMigrationStoreMockRecorder) UpdateStatus(arg0, arg1 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "UpdateStatus", reflect.TypeOf((*MockBackgroundMigrationStore)(nil).UpdateStatus), arg0, arg1)
}

// ValidateMigrationTableAndColumn mocks base method.
func (m *MockBackgroundMigrationStore) ValidateMigrationTableAndColumn(arg0 context.Context, arg1, arg2 string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ValidateMigrationTableAndColumn", arg0, arg1, arg2)
	ret0, _ := ret[0].(error)
	return ret0
}

// ValidateMigrationTableAndColumn indicates an expected call of ValidateMigrationTableAndColumn.
func (mr *MockBackgroundMigrationStoreMockRecorder) ValidateMigrationTableAndColumn(arg0, arg1, arg2 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ValidateMigrationTableAndColumn", reflect.TypeOf((*MockBackgroundMigrationStore)(nil).ValidateMigrationTableAndColumn), arg0, arg1, arg2)
}