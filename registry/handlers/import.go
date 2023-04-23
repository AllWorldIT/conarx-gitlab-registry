package handlers

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/docker/distribution/log"
	"github.com/docker/distribution/registry/api/errcode"
	v1 "github.com/docker/distribution/registry/api/gitlab/v1"
	v2 "github.com/docker/distribution/registry/api/v2"
	"github.com/docker/distribution/registry/datastore"
	"github.com/docker/distribution/registry/datastore/models"
	"github.com/docker/distribution/registry/handlers/internal/metrics"
	"github.com/docker/distribution/registry/internal/migration"
	"github.com/docker/distribution/registry/storage"
	ghandlers "github.com/gorilla/handlers"
	"github.com/hashicorp/go-multierror"
	"gitlab.com/gitlab-org/labkit/correlation"
	"gitlab.com/gitlab-org/labkit/errortracking"
)

var (
	// OngoingImportCheckIntervalSeconds is the interval in seconds at which the import handler would
	// check if an ongoing import has been manually canceled by the DELETE method. This variable
	// is public so it can be overridden in tests.
	// TODO: make this part of the importHandler and add it to the configuration settings
	// https://gitlab.com/gitlab-org/container-registry/-/issues/626
	OngoingImportCheckIntervalSeconds = 5 * time.Second
)

// cancelableStatuses is the list of migration repository statuses that are
// allowed to be canceled.
var cancelableStatuses = map[migration.RepositoryStatus]bool{
	migration.RepositoryStatusPreImportInProgress: true,
	migration.RepositoryStatusImportInProgress:    true,
}

// importHandler handles http operations on repository imports
type importHandler struct {
	*Context

	datastore.RepositoryStore
	preImport bool
	timeout   time.Duration
}

// importDispatcher takes the request context and builds the
// appropriate handler for handling import requests.
func importDispatcher(ctx *Context, r *http.Request) http.Handler {
	ih := &importHandler{
		Context:         ctx,
		RepositoryStore: datastore.NewRepositoryStore(ctx.App.db),
		timeout:         ctx.App.Config.Migration.ImportTimeout,
	}

	ihandler := ghandlers.MethodHandler{
		http.MethodGet: http.HandlerFunc(ih.GetImport),
	}

	if !ctx.readOnly {
		ihandler[http.MethodPut] = ih.maxConcurrentImportsMiddleware(http.HandlerFunc(ih.StartRepositoryImport))
		ihandler[http.MethodDelete] = http.HandlerFunc(ih.CancelRepositoryImport)
	}

	return ihandler
}

type RepositoryImportStatus struct {
	Name   string                     `json:"name"`
	Path   string                     `json:"path"`
	Status migration.RepositoryStatus `json:"status"`
	Detail string                     `json:"detail"`
}

func (ih *importHandler) GetImport(w http.ResponseWriter, r *http.Request) {
	dbRepo, err := ih.FindByPath(ih.Context, ih.Repository.Named().Name())
	if err != nil {
		ih.Errors = append(ih.Errors, errcode.FromUnknownError(err))
		return
	}

	if dbRepo == nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	rs := RepositoryImportStatus{
		Name:   dbRepo.Name,
		Path:   dbRepo.Path,
		Status: dbRepo.MigrationStatus,
		Detail: getImportDetail(dbRepo),
	}

	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)

	if err := enc.Encode(rs); err != nil {
		ih.Errors = append(ih.Errors, errcode.FromUnknownError(err))
		return
	}
}

const (
	importTypeQueryParamKey   = "import_type"
	importDeleteForceParamKey = "force"
)

// StartRepositoryImport begins a repository import.
func (ih *importHandler) StartRepositoryImport(w http.ResponseWriter, r *http.Request) {
	ih.acquireImportSemaphore()

	defer func() {
		if len(ih.Errors) > 0 {
			// make sure we release the resource if this handler returned an error
			ih.releaseImportSemaphore()
		}
	}()

	l := log.GetLogger(log.WithContext(ih)).WithFields(log.Fields{
		"repository":             ih.Repository.Named().Name(),
		"current_import_count":   len(ih.importSemaphore),
		"max_concurrent_imports": cap(ih.importSemaphore),
		"delay_s":                ih.App.Config.Migration.TestSlowImport.Seconds(),
		"tag_concurrency":        ih.App.Config.Migration.TagConcurrency,
	})
	l.Debug("ImportRepository")

	if ih.App.Config.Migration.TestSlowImport > 0 {
		l.Warn("testing slow import, this should never happen in production")
	}

	dbRepo, err := ih.FindByPath(ih.Context, ih.Repository.Named().Name())
	if err != nil {
		ih.Errors = append(ih.Errors, errcode.FromUnknownError(err))
		return
	}

	ih.preImport, err = isImportTypePre(r)
	if err != nil {
		detail := v1.InvalidQueryParamValueErrorDetail(importTypeQueryParamKey, []string{"pre", "final"})
		ih.Errors = append(ih.Errors, v1.ErrorCodeInvalidQueryParamValue.WithDetail(detail))
		return
	}

	if ih.preImport {
		ih.timeout = ih.App.Config.Migration.PreImportTimeout
	}

	l = l.WithFields(log.Fields{"pre_import": ih.preImport, "timeout_s": ih.timeout.Seconds()})

	// Set up metrics reporting
	var report metrics.ImportReportFunc
	if ih.preImport {
		report = metrics.PreImport()
	} else {
		report = metrics.Import()
	}

	shouldImport, err := ih.shouldImport(dbRepo)
	if err != nil {
		ih.Errors = append(ih.Errors, err)
		// do not report import failure if the target repository was not found in the old registry
		if errors.Is(err, v2.ErrorCodeNameUnknown) {
			report(false, nil)
		} else {
			report(false, err)
		}
		return
	}

	if !shouldImport {
		l.Info("repository already imported, skipping import")
		w.WriteHeader(http.StatusOK)

		report(false, nil)
		ih.releaseImportSemaphore()
		return
	}

	if dbRepo != nil {
		// Cleanup migration error when retrying a failed import
		dbRepo.MigrationError = sql.NullString{}
	}

	dbRepo, err = ih.createOrUpdateRepo(dbRepo)
	if err != nil {
		err = errcode.FromUnknownError(err)
		ih.Errors = append(ih.Errors, err)

		report(false, err)
		return
	}

	go func() {
		defer ih.panicRecoverer(dbRepo)
		defer ih.releaseImportSemaphore()

		importer := datastore.NewImporter(
			ih.App.db,
			ih.App.registry,
			datastore.WithTagConcurrency(ih.App.Config.Migration.TagConcurrency),
			// This should ALWAYS be set to zero during production.
			datastore.WithTestSlowImport(ih.App.Config.Migration.TestSlowImport),
		)

		correlationID := correlation.ExtractFromContext(ih.Context)

		importCtx, importCtxCancel := context.WithTimeout(context.Background(), ih.timeout)
		defer importCtxCancel()

		// ensure correlation ID is forwarded to the import
		importCtx = correlation.ContextWithCorrelation(importCtx, correlationID)

		// Add parent logger to worker context to preserve request-specific fields.
		l := log.GetLogger(log.WithContext(ih.Context))
		importCtx = log.WithLogger(importCtx, l)

		done := make(chan bool)
		defer close(done)

		go ih.checkOngoingImportStatus(importCtx, done, importCtxCancel)

		// Make a best effort attempt to stop ongoing imports during graceful shutdowns.
		if err := ih.App.ongoingImports.add(correlationID, &repositoryImport{
			path: dbRepo.Path,
			cancelFn: func() {
				dbRepo, err := ih.FindByPath(importCtx, dbRepo.Path)
				if err != nil {
					l.WithError(err).Error("stopping ongoing import for shutdown: error finding repository")
					return
				}

				if dbRepo == nil {
					l.WithError(err).Error("stopping ongoing import for shutdown: repository not found in database")
					return
				}

				if cancelable := cancelableStatuses[dbRepo.MigrationStatus]; !cancelable {
					l.WithFields(log.Fields{"migration_status": dbRepo.MigrationStatus}).Warn("repository not in cancelable state")
					return
				}

				forceDelete := true
				if err := ih.cancelImport(importCtx, dbRepo, forceDelete); err != nil {
					l.WithError(err).Error("stopping ongoing import for shutdown: canceling import")
					return
				}
			},
		}); err != nil {
			// Don't stop the import due to this error, this instance will likely not
			// get shutdown during import and if it does, rails will be able to pick
			// it back up, it will only take a bit longer than if we could cancel it.
			l.WithError(err).WithFields(log.Fields{"repository": dbRepo.Path}).Error("registering repository import failed")
			errortracking.Capture(err, errortracking.WithContext(importCtx), errortracking.WithRequest(r))
		}
		defer ih.App.ongoingImports.remove(correlationID)

		err = ih.runImport(importCtx, importer, dbRepo)
		if err != nil {
			l.WithError(err).WithFields(log.Fields{"repository": dbRepo.Path}).Error("repository import failed")
			errortracking.Capture(err, errortracking.WithContext(importCtx), errortracking.WithRequest(r))
		}
		report(true, err)

		notifCtx, notifCtxCancel := ih.newNotificationContext(l, correlationID)
		defer notifCtxCancel()

		ih.sendImportNotification(notifCtx)
	}()

	w.WriteHeader(http.StatusAccepted)
}

func (ih *importHandler) newNotificationContext(l log.Logger, correlationID string) (context.Context, func()) {
	notifCtx, notifCtxCancel := context.WithTimeout(context.Background(), ih.Config.Migration.ImportNotification.Timeout)

	// ensure correlation ID is forwarded to the notifier
	notifCtx = correlation.ContextWithCorrelation(notifCtx, correlationID)
	// ensure the context has the logger and fields for the import notification
	notifCtx = log.WithLogger(notifCtx, l)

	return notifCtx, notifCtxCancel
}

func (ih *importHandler) shouldImport(dbRepo *models.Repository) (bool, error) {
	if dbRepo != nil {
		switch status := dbRepo.MigrationStatus; {
		// Do not begin an import for a repository which has already completed final import.
		case status.OnDatabase():
			return false, nil

		// Do not begin an import with a repository that already has
		//	an import operation ongoing.
		case status == migration.RepositoryStatusPreImportInProgress:
			detail := v1.ErrorCodePreImportInProgressErrorDetail(ih.Repository)
			return false, v1.ErrorCodePreImportInProgress.WithDetail(detail)

		case status == migration.RepositoryStatusImportInProgress:
			detail := v1.ErrorCodeImportInProgressErrorDetail(ih.Repository)
			return false, v1.ErrorCodeImportInProgress.WithDetail(detail)

		// Do not begin a final import for a repository that failed to pre import, allow
		// additional pre import attempts.
		case status == migration.RepositoryStatusPreImportFailed && !ih.preImport:
			detail := v1.ErrorCodePreImportFailedErrorDetail(ih.Repository)
			return false, v1.ErrorCodePreImportFailed.WithDetail(detail)

		case status == migration.RepositoryStatusPreImportCanceled:
			// Wait at least OngoingImportCheckIntervalSeconds after a repository has been
			// updated before allowing another import attempt after cancelation. This
			// allows the checkOngoingImportStatus goroutine to cancel the import.
			if dbRepo.UpdatedAt.Valid && time.Now().Before(dbRepo.UpdatedAt.Time.Add(OngoingImportCheckIntervalSeconds)) {
				detail := v1.ErrorCodeImportRepositoryNotReadyDetail(ih.Repository)
				return false, v1.ErrorCodeImportRepositoryNotReady.WithDetail(detail)
			}

			// Do not begin a final import for repository that was canceled during pre import.
			if !ih.preImport {
				detail := v1.ErrorCodePreImportCanceledErrorDetail(ih.Repository)
				return false, v1.ErrorCodePreImportCanceled.WithDetail(detail)
			}
		}
	}

	validator, ok := ih.Repository.(storage.RepositoryValidator)
	if !ok {
		return false, errcode.FromUnknownError(fmt.Errorf("repository does not implement RepositoryValidator interface"))
	}

	// check if repository exists in the old storage prefix before attempting import
	exists, err := validator.Exists(ih)
	if err != nil {
		return false, errcode.FromUnknownError(fmt.Errorf("unable to determine if repository exists on old storage prefix: %w", err))
	}

	if !exists {
		return false, v2.ErrorCodeNameUnknown
	}

	// Do not begin a final import for a repository that has not been pre imported. We do the check here to allow
	// raising an "unknown repository" error if it does not exist on the old storage prefix.
	if dbRepo == nil && !ih.preImport {
		detail := v1.ErrorCodePreImportRequiredDetail(ih.Repository)
		return false, v1.ErrorCodePreImportRequired.WithDetail(detail)
	}

	return true, nil
}

func (ih *importHandler) createOrUpdateRepo(dbRepo *models.Repository) (*models.Repository, error) {
	var status migration.RepositoryStatus
	if ih.preImport {
		status = migration.RepositoryStatusPreImportInProgress
	} else {
		status = migration.RepositoryStatusImportInProgress
	}

	var err error
	if dbRepo == nil {
		// Although here we already know that the repo does not exist, we have to account for the possibility of it
		// existing but being soft-deleted (thus invisible to the previous find query). In such case we have to undo
		// the soft-delete and update the migration status from `native` to `(pre_)import_in_progress`. Therefore,
		// we reuse the existing CreateOrFindByPath method (same as used at the API level) instead of CreateByPath.
		dbRepo, err = ih.CreateOrFindByPath(ih.Context, ih.Repository.Named().Name(), datastore.WithMigrationStatus(status))
		if err != nil {
			return dbRepo, fmt.Errorf("creating repository for import: %w", err)
		}
	} else {
		dbRepo.MigrationStatus = status
		if err := ih.Update(ih.Context, dbRepo); err != nil {
			return dbRepo, fmt.Errorf("updating migration status before import: %w", err)
		}
	}

	return dbRepo, nil
}

func (ih *importHandler) runImport(ctx context.Context, importer *datastore.Importer, dbRepo *models.Repository) error {
	multiErrs := &multierror.Error{}

	if ih.preImport {
		if err := importer.PreImport(ctx, dbRepo.Path); err != nil {
			ih.updateRepoWithError(ctx, dbRepo, err, multiErrs)
			return multiErrs.ErrorOrNil()
		}

		dbRepo.MigrationStatus = migration.RepositoryStatusPreImportComplete
		ih.updateSuccessfulRepo(ctx, dbRepo, multiErrs)

		return multiErrs.ErrorOrNil()
	}

	if err := importer.Import(ctx, dbRepo.Path); err != nil {
		ih.updateRepoWithError(ctx, dbRepo, err, multiErrs)
		return multiErrs.ErrorOrNil()
	}

	dbRepo.MigrationStatus = migration.RepositoryStatusImportComplete
	ih.updateSuccessfulRepo(ctx, dbRepo, multiErrs)

	return multiErrs.ErrorOrNil()
}

// The API spec for this route only specifies 'pre' or 'final'.
func isImportTypePre(r *http.Request) (bool, error) {
	importTypeValue := r.URL.Query().Get(importTypeQueryParamKey)

	switch importTypeValue {
	case "pre":
		return true, nil
	case "final":
		return false, nil
	default:
		return false, fmt.Errorf("import_type value must be 'pre' or 'final', got %s", importTypeValue)
	}
}

func (ih *importHandler) sendImportNotification(ctx context.Context) {
	if ih.App.importNotifier == nil {
		return
	}
	l := log.GetLogger(log.WithContext(ih))

	dbRepo, err := ih.FindByPath(ctx, ih.Repository.Named().Name())
	if err != nil {
		l.WithError(err).Error("finding repository before sending import notification")
		errortracking.Capture(err, errortracking.WithContext(ctx))
		return
	}

	if dbRepo == nil {
		err := fmt.Errorf("repository was nil: %q", ih.Repository.Named().Name())
		l.WithError(err).Error("sending import notification")
		errortracking.Capture(err, errortracking.WithContext(ctx))
		return
	}

	importNotification := &migration.Notification{
		Name:   dbRepo.Name,
		Path:   dbRepo.Path,
		Status: string(dbRepo.MigrationStatus),
		Detail: getImportDetail(dbRepo),
	}

	if err := ih.App.importNotifier.Notify(ctx, importNotification); err != nil {
		l.WithError(err).Error("failed to send import notification")
		errortracking.Capture(err, errortracking.WithContext(ctx))
	}
}

func getImportDetail(dbRepo *models.Repository) string {
	if dbRepo.MigrationError.String != "" {
		return dbRepo.MigrationError.String
	}

	switch dbRepo.MigrationStatus {
	case migration.RepositoryStatusImportComplete:
		return "final import completed successfully"
	case migration.RepositoryStatusPreImportComplete:
		return "pre import completed successfully"
	case migration.RepositoryStatusPreImportCanceled:
		return "pre import was canceled manually"
	case migration.RepositoryStatusImportCanceled:
		return "final import was canceled manually"
	default:
		return string(dbRepo.MigrationStatus)
	}
}

// maxConcurrentImportsMiddleware is a middleware that checks the configured `maxconcurrentimports`
// and does not allow requests to begin an import if the limit has been reached.
func (ih *importHandler) maxConcurrentImportsMiddleware(handler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capacity := cap(ih.importSemaphore)
		// the length of the semaphore tells us how many resources are being currently used
		length := len(ih.importSemaphore)

		p := (float64(length) * 100) / float64(capacity)

		metrics.ImportWorkerSaturation(p)

		if length < capacity {
			handler.ServeHTTP(w, r)
			return
		}

		log.GetLogger(log.WithContext(ih.Context)).WithFields(log.Fields{
			"repository":             ih.Repository.Named().Name(),
			"max_concurrent_imports": capacity,
		}).Warn("registry instance is already running maximum concurrent imports")

		detail := v1.ErrorCodeImportRateLimitedDetail(ih.Repository)
		ih.Errors = append(ih.Errors, v1.ErrorCodeImportRateLimited.WithDetail(detail))
		return
	})
}

func (ih *importHandler) acquireImportSemaphore() {
	ih.importSemaphore <- struct{}{}
}

func (ih *importHandler) releaseImportSemaphore() {
	<-ih.importSemaphore
}

// CancelRepositoryImport will attempt to update the repository status of an ongoing (pre)import to
// migration.RepositoryStatusPreImportCanceled or migration.RepositoryStatusImportCanceled depending on the status of
// given repository.
func (ih *importHandler) CancelRepositoryImport(w http.ResponseWriter, r *http.Request) {
	l := log.GetLogger(log.WithContext(ih)).WithFields(log.Fields{
		"repository": ih.Repository.Named().Name(),
	})
	l.Debug("CancelImportRepository")

	dbRepo, err := ih.FindByPath(ih.Context, ih.Repository.Named().Name())
	if err != nil {
		ih.Errors = append(ih.Errors, errcode.FromUnknownError(err))
		return
	}

	if dbRepo == nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	forceDelete := false
	if r.URL.Query().Get(importDeleteForceParamKey) == "true" {
		if dbRepo.MigrationStatus == migration.RepositoryStatusNative {
			detail := v1.ErrorCodeImportCannotBeCanceledDetail(ih.Repository, string(dbRepo.MigrationStatus))
			ih.Errors = append(ih.Errors, v1.ErrorCodeImportCannotBeCanceled.WithDetail(detail))
			return
		}

		forceDelete = true
	}

	if cancelable := cancelableStatuses[dbRepo.MigrationStatus]; !forceDelete && !cancelable {
		detail := v1.ErrorCodeImportCannotBeCanceledDetail(ih.Repository, string(dbRepo.MigrationStatus))
		ih.Errors = append(ih.Errors, v1.ErrorCodeImportCannotBeCanceled.WithDetail(detail))
		return
	}

	if err := ih.cancelImport(ih.Context, dbRepo, forceDelete); err != nil {
		ih.Errors = append(ih.Errors, errcode.FromUnknownError(err))
		return
	}

	w.WriteHeader(http.StatusAccepted)
}

func (ih *importHandler) cancelImport(ctx context.Context, dbRepo *models.Repository, forced bool) error {
	toStatus := migration.RepositoryStatusImportCanceled
	detail := "final import canceled"
	if dbRepo.MigrationStatus == migration.RepositoryStatusPreImportInProgress ||
		dbRepo.MigrationStatus == migration.RepositoryStatusPreImportComplete {
		toStatus = migration.RepositoryStatusPreImportCanceled
		detail = "pre import canceled"
	}

	if forced {
		detail = "forced cancelation"
	}

	dbRepo.MigrationStatus = toStatus
	dbRepo.MigrationError = sql.NullString{String: detail, Valid: true}

	if err := ih.Update(ctx, dbRepo); err != nil {
		return fmt.Errorf("updating migration status trying to cancel import: %w", err)
	}

	return nil
}

// checkOngoingImportStatus starts a ticker that will check every OngoingImportCheckIntervalSeconds if the repository
// currently being imported has been canceled by a DELETE operation.
// If it has, it will cancel the importCtx calling cancelFn letting the rest of the (pre)import operations fail.
// It returns if the done chanel is closed by the import having finished.
func (ih *importHandler) checkOngoingImportStatus(importCtx context.Context, done chan bool, cancelFn func()) {
	l := log.GetLogger(log.WithContext(ih.Context)).WithFields(log.Fields{
		"repository": ih.Repository.Named().Name(),
		"interval_s": OngoingImportCheckIntervalSeconds.Seconds(),
	})

	ticker := time.NewTicker(OngoingImportCheckIntervalSeconds)
	for {
		select {
		case <-ticker.C:
			l.Info("checking if ongoing (pre)import has been canceled")

			dbRepo, err := ih.FindByPath(importCtx, ih.Repository.Named().Name())
			if err != nil {
				ih.Errors = append(ih.Errors, errcode.FromUnknownError(err))
				return
			}

			if dbRepo == nil {
				l.WithError(v2.ErrorCodeNameUnknown).Error("repository was nil checking if ongoing (pre)import has been canceled")
				errortracking.Capture(v2.ErrorCodeNameUnknown, errortracking.WithContext(importCtx))
				return
			}

			if dbRepo.MigrationStatus == migration.RepositoryStatusPreImportCanceled ||
				dbRepo.MigrationStatus == migration.RepositoryStatusImportCanceled {
				// importCtxCancel ongoing import
				l.Warn("canceling ongoing (pre)import")
				cancelFn()
				return
			}

		case <-done:
			return
		}
	}
}

// updateSuccessfulRepo is called after a successful (pre)import
func (ih *importHandler) updateSuccessfulRepo(importCtx context.Context, dbRepo *models.Repository, multiErrs *multierror.Error) {
	// Make sure the context hasn't been canceled to avoid logging a misleading error.
	if errors.Is(importCtx.Err(), context.Canceled) {
		return
	}

	if err := ih.Update(importCtx, dbRepo); err != nil {
		errStr := "updating migration status after successful final import"
		if ih.preImport {
			errStr = "updating migration status after successful pre import"
		}
		updateErr := fmt.Errorf("%s: %w", errStr, err)
		multiErrs = multierror.Append(multiErrs, updateErr)

		log.GetLogger(log.WithContext(importCtx)).WithError(updateErr).Error("failed to update migration status at the end of import")
		// try to update one last time to catch edge cases where importCtx might get canceled by the time we get here
		ih.updateRepoWithError(importCtx, dbRepo, updateErr, multiErrs)
	}
}

// updateRepoWithError is called when a repository has failed to (pre)import. A possible failure is a
// context.Deadline so we need to make sure that the repository is updated in the database correctly by using
// a new context with its own timeout.
func (ih *importHandler) updateRepoWithError(importCtx context.Context, dbRepo *models.Repository, importErr error, multiErrs *multierror.Error) {
	if importErr == nil ||
		// A context.Canceled or datastore.Err(Pre)ImportCanceled importErr is not a reason to mark the
		// repository migration status as failed, because this repository was manually canceled by
		// the DELETE endpoint, so this error should be skipped in these cases.
		errors.Is(importErr, context.Canceled) ||
		errors.Is(importErr, datastore.ErrPreImportCanceled) || errors.Is(importErr, datastore.ErrImportCanceled) {
		return
	}

	multiErrs = multierror.Append(multiErrs, importErr)

	errStr := "updating repository after failed final import"
	dbRepo.MigrationError = sql.NullString{String: importErr.Error(), Valid: true}
	dbRepo.MigrationStatus = migration.RepositoryStatusImportFailed
	if ih.preImport {
		errStr = "updating repository after failed pre import"
		dbRepo.MigrationStatus = migration.RepositoryStatusPreImportFailed
	}

	updateCtx, updateCtxCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer updateCtxCancel()

	updateCtx = correlation.ContextWithCorrelation(updateCtx, correlation.ExtractFromContext(importCtx))

	if err := ih.Update(updateCtx, dbRepo); err != nil {
		updateErr := fmt.Errorf("%s: %w", errStr, err)
		multiErrs = multierror.Append(multiErrs, updateErr)

		log.GetLogger(log.WithContext(importCtx)).WithError(updateErr).Error("failed to update migration status at the end of import")
	}
}

func (ih *importHandler) panicRecoverer(dbRepo *models.Repository) func() {
	return func() {
		if r := recover(); r != nil {
			multiErr := &multierror.Error{}
			ih.updateRepoWithError(ih.Context, dbRepo, fmt.Errorf("%v", r), multiErr)
			log.GetLogger(log.WithContext(ih.Context)).WithFields(log.Fields{"r": r}).WithError(multiErr.ErrorOrNil()).Error("recovered from panic inside import handler")
		}
	}
}
