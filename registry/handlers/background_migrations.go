package handlers

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/docker/distribution/registry/api/errcode"
	v2 "github.com/docker/distribution/registry/api/v2"
	"github.com/docker/distribution/registry/datastore"
	"github.com/docker/distribution/registry/datastore/models"
	"github.com/gorilla/handlers"
)

// BackgroundMigrationResponse represents a single background migration in API responses.
type BackgroundMigrationResponse struct {
	ID               int      `json:"id"`
	Name             string   `json:"name"`
	Status           string   `json:"status"`
	JobSignatureName string   `json:"job_signature_name"`
	TargetTable      string   `json:"target_table"`
	TargetColumn     string   `json:"target_column"`
	BatchSize        int      `json:"batch_size"`
	MinValue         int      `json:"min_value"`
	MaxValue         int      `json:"max_value"`
	BatchingStrategy string   `json:"batching_strategy"`
	TotalTupleCount  *int64   `json:"total_tuple_count,omitempty"`
	ErrorCode        *int     `json:"error_code,omitempty"`
	Capped           *bool    `json:"capped,omitempty"`
	FinishedJobs     *int64   `json:"finished_jobs,omitempty"`
	Progress         *float64 `json:"progress,omitempty"`
}

// BackgroundMigrationsListResponse is the response for listing all background migrations.
type BackgroundMigrationsListResponse struct {
	Migrations []BackgroundMigrationResponse `json:"migrations"`
}

// BackgroundMigrationGetResponse is the response for getting a single BBM migration.
type BackgroundMigrationGetResponse struct {
	Migration BackgroundMigrationResponse `json:"migration"`
}

// BackgroundMigrationActionResponse is the response for action endpoints
// (pause/resume/restart).
type BackgroundMigrationActionResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message,omitempty"`
}

// BackgroundMigrationRunRequest is the request body for the run endpoint.
type BackgroundMigrationRunRequest struct {
	MaxJobRetry *int `json:"max_job_retry,omitempty"`
}

// BackgroundMigrationRunResponse is the response for the run endpoint.
type BackgroundMigrationRunResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
}

// backgroundMigrationsHandler handles background migration API requests.
type backgroundMigrationsHandler struct {
	*Context
}

// backgroundMigrationsDispatcher routes requests to the appropriate handler for the status endpoint.
func backgroundMigrationsDispatcher(ctx *Context, _ *http.Request) http.Handler {
	handler := &backgroundMigrationsHandler{
		Context: ctx,
	}

	return handlers.MethodHandler{
		http.MethodGet: http.HandlerFunc(handler.GetBackgroundMigrations),
	}
}

// backgroundMigrationsStatusDispatcher routes requests to the appropriate handler for the status endpoint.
func backgroundMigrationDispatcher(ctx *Context, _ *http.Request) http.Handler {
	handler := &backgroundMigrationsHandler{
		Context: ctx,
	}

	return handlers.MethodHandler{
		http.MethodGet: http.HandlerFunc(handler.GetBackgroundMigration),
	}
}

// backgroundMigrationsPauseDispatcher routes requests to the appropriate handler for the pause endpoint.
func backgroundMigrationsPauseDispatcher(ctx *Context, _ *http.Request) http.Handler {
	handler := &backgroundMigrationsHandler{
		Context: ctx,
	}

	return handlers.MethodHandler{
		http.MethodPost: http.HandlerFunc(handler.PauseBackgroundMigrations),
	}
}

// backgroundMigrationsResumeDispatcher routes requests to the appropriate handler for the resume endpoint.
func backgroundMigrationsResumeDispatcher(ctx *Context, _ *http.Request) http.Handler {
	handler := &backgroundMigrationsHandler{
		Context: ctx,
	}

	return handlers.MethodHandler{
		http.MethodPost: http.HandlerFunc(handler.ResumeBackgroundMigrations),
	}
}

// backgroundMigrationsRestartDispatcher routes requests to the appropriate handler for the restart endpoint.
func backgroundMigrationsRestartDispatcher(ctx *Context, _ *http.Request) http.Handler {
	handler := &backgroundMigrationsHandler{
		Context: ctx,
	}

	return handlers.MethodHandler{
		http.MethodPost: http.HandlerFunc(handler.RestartBackgroundMigration),
	}
}

// GetBackgroundMigrations handles GET requests to get the status of all
// background migrations.
func (h *backgroundMigrationsHandler) GetBackgroundMigrations(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	store := datastore.NewBackgroundMigrationStore(h.db.Primary())

	migrations, err := store.FindWithProgress(r.Context(), nil)
	if err != nil {
		h.Errors = append(h.Errors, errcode.FromUnknownError(err))
		return
	}
	resp := BackgroundMigrationsListResponse{
		Migrations: convertToBackgroundMigrationResponses(migrations),
	}

	enc := json.NewEncoder(w)
	if err := enc.Encode(resp); err != nil {
		h.Errors = append(h.Errors, errcode.FromUnknownError(err))
		return
	}
}

// GetBackgroundMigration handles GET requests to get the status of a single
// background migration.
func (h *backgroundMigrationsHandler) GetBackgroundMigration(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	bbmId, err := getBBMId(r.Context())
	if err != nil {
		h.Errors = append(
			h.Errors,
			v2.ErrorCodeBBMIdInvalid.WithDetail(err),
		)
		return
	}

	store := datastore.NewBackgroundMigrationStore(h.db.Primary())

	migration, err := store.FindWithProgress(r.Context(), &bbmId)
	if err != nil {
		h.Errors = append(h.Errors, errcode.FromUnknownError(err))
		return
	}
	if len(migration) != 1 {
		h.Errors = append(
			h.Errors,
			v2.ErrorCodeBBMNotFound.WithDetail(fmt.Sprintf("BBM with ID %d not found", bbmId)),
		)
		return
	}

	resp := BackgroundMigrationGetResponse{
		Migration: convertToBackgroundMigrationResponse(migration[0]),
	}

	enc := json.NewEncoder(w)
	if err := enc.Encode(resp); err != nil {
		h.Errors = append(h.Errors, errcode.FromUnknownError(err))
		return
	}
}

// PauseBackgroundMigrations handles POST requests to pause all running or active background migrations.
func (h *backgroundMigrationsHandler) PauseBackgroundMigrations(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	err := datastore.NewBackgroundMigrationStore(h.db.Primary()).
		Pause(r.Context())
	if err != nil {
		h.Errors = append(h.Errors, errcode.FromUnknownError(err))
		return
	}

	resp := BackgroundMigrationActionResponse{
		Success: true,
		Message: "All eligible background migrations have been paused",
	}

	enc := json.NewEncoder(w)
	if err := enc.Encode(resp); err != nil {
		h.Errors = append(h.Errors, errcode.FromUnknownError(err))
		return
	}
}

// ResumeBackgroundMigrations handles POST requests to resume all paused background migrations.
func (h *backgroundMigrationsHandler) ResumeBackgroundMigrations(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	err := datastore.NewBackgroundMigrationStore(h.db.Primary()).
		Resume(r.Context())
	if err != nil {
		h.Errors = append(h.Errors, errcode.FromUnknownError(err))
		return
	}

	resp := BackgroundMigrationActionResponse{
		Success: true,
		Message: "All eligible background migrations have been resumed",
	}

	enc := json.NewEncoder(w)
	if err := enc.Encode(resp); err != nil {
		h.Errors = append(h.Errors, errcode.FromUnknownError(err))
		return
	}
}

// RestartBackgroundMigration handles POST requests to restart a specific background migration.
// It resets the failure_error_code to NULL and status to active (1).
func (h *backgroundMigrationsHandler) RestartBackgroundMigration(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	bbmId, err := getBBMId(r.Context())
	if err != nil {
		h.Errors = append(
			h.Errors,
			v2.ErrorCodeBBMIdInvalid.WithDetail(err),
		)
		return
	}

	// First, fetch the BBM to ensure it exists
	bbm, err := datastore.NewBackgroundMigrationStore(h.db.Primary()).
		FindById(r.Context(), bbmId)
	if err != nil {
		h.Errors = append(h.Errors, errcode.FromUnknownError(err))
		return
	}
	if bbm == nil {
		h.Errors = append(
			h.Errors,
			v2.ErrorCodeBBMNotFound.WithDetail(fmt.Sprintf("BBM with ID %d does not exist", bbmId)),
		)
		return
	}

	// Set status to active and clear error code
	bbm.Status = models.BackgroundMigrationActive
	bbm.ErrorCode = models.NullErrCode

	// Update the BBM status
	err = datastore.NewBackgroundMigrationStore(h.db.Primary()).
		UpdateStatus(r.Context(), bbm)
	if err != nil {
		h.Errors = append(h.Errors, errcode.FromUnknownError(err))
		return
	}

	resp := BackgroundMigrationActionResponse{
		Success: true,
		Message: fmt.Sprintf("Background migration %d has been restarted", bbmId),
	}

	enc := json.NewEncoder(w)
	if err := enc.Encode(resp); err != nil {
		h.Errors = append(h.Errors, errcode.FromUnknownError(err))
		return
	}
}

// convertToBackgroundMigrationResponse converts a single models.BackgroundMigration to API response format.
func convertToBackgroundMigrationResponse(m *models.BackgroundMigrationWithProgress) BackgroundMigrationResponse {
	resp := BackgroundMigrationResponse{
		ID:               m.ID,
		Name:             m.Name,
		Status:           m.Status.String(),
		JobSignatureName: m.JobName,
		TargetTable:      m.TargetTable,
		TargetColumn:     m.TargetColumn,
		BatchSize:        m.BatchSize,
		MinValue:         m.StartID,
		MaxValue:         m.EndID,
		BatchingStrategy: m.BatchingStrategy.String,
		Capped:           &m.Capped,
		FinishedJobs:     &m.FinishedJobs,
		Progress:         &m.Progress,
	}

	// Only include total_tuple_count if it's been set (not null)
	if m.TotalTupleCount.Valid {
		count := m.TotalTupleCount.Int64
		resp.TotalTupleCount = &count
	}

	// Only include error_code if it's been set (not null)
	if m.ErrorCode.Valid {
		code := int(m.ErrorCode.Int16)
		resp.ErrorCode = &code
	}

	return resp
}

// convertToBackgroundMigrationResponses converts a slice of models.BackgroundMigration to API response format.
func convertToBackgroundMigrationResponses(migrations models.BackgroundMigrationsWithProgress) []BackgroundMigrationResponse {
	responses := make([]BackgroundMigrationResponse, 0, len(migrations))

	for i := range migrations {
		responses = append(responses, convertToBackgroundMigrationResponse(migrations[i]))
	}

	return responses
}
