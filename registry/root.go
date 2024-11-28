package registry

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/docker/distribution/configuration"
	dcontext "github.com/docker/distribution/context"
	"github.com/docker/distribution/internal/feature"
	"github.com/docker/distribution/registry/bbm"
	"github.com/docker/distribution/registry/datastore"
	"github.com/docker/distribution/registry/datastore/migrations"
	"github.com/docker/distribution/registry/storage"
	"github.com/docker/distribution/registry/storage/driver/factory"
	"github.com/docker/distribution/version"
	"github.com/docker/libtrust"
	"github.com/olekukonko/tablewriter"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

func init() {
	RootCmd.AddCommand(ServeCmd)
	RootCmd.AddCommand(GCCmd)
	RootCmd.AddCommand(DBCmd)
	RootCmd.AddCommand(BBMCmd)
	RootCmd.Flags().BoolVarP(&showVersion, "version", "v", false, "show the version and exit")

	GCCmd.Flags().BoolVarP(&dryRun, "dry-run", "d", false, "do everything except remove the blobs")
	GCCmd.Flags().BoolVarP(&removeUntagged, "delete-untagged", "m", false, "delete manifests that are not currently referenced via tag")
	GCCmd.Flags().StringVarP(&debugAddr, "debug-server", "s", "", "run a pprof debug server at <address:port>")

	MigrateCmd.AddCommand(MigrateVersionCmd)
	MigrateStatusCmd.Flags().BoolVarP(&upToDateCheck, "up-to-date", "u", false, "check if all known migrations are applied")
	MigrateStatusCmd.Flags().BoolVarP(&skipPostDeployment, "skip-post-deployment", "s", false, "ignore post deployment migrations")
	MigrateCmd.AddCommand(MigrateStatusCmd)
	MigrateUpCmd.Flags().BoolVarP(&dryRun, "dry-run", "d", false, "do not commit changes to the database")
	MigrateUpCmd.Flags().VarP(nullableInt{&maxNumMigrations}, "limit", "n", "limit the number of migrations (all by default)")
	MigrateUpCmd.Flags().BoolVarP(&skipPostDeployment, "skip-post-deployment", "s", false, "do not apply post deployment migrations")
	MigrateCmd.AddCommand(MigrateUpCmd)
	MigrateDownCmd.Flags().BoolVarP(&force, "force", "f", false, "no confirmation message")
	MigrateDownCmd.Flags().BoolVarP(&dryRun, "dry-run", "d", false, "do not commit changes to the database")
	MigrateDownCmd.Flags().VarP(nullableInt{&maxNumMigrations}, "limit", "n", "limit the number of migrations (all by default)")
	MigrateCmd.AddCommand(MigrateDownCmd)
	DBCmd.AddCommand(MigrateCmd)

	DBCmd.AddCommand(ImportCmd)
	ImportCmd.Flags().BoolVarP(&dryRun, "dry-run", "d", false, "do not commit changes to the database")
	ImportCmd.Flags().BoolVarP(&rowCount, "row-count", "c", false, "count and log number of rows across relevant database tables on (pre)import completion")
	ImportCmd.Flags().BoolVarP(&preImport, "pre-import", "p", false, "import immutable repository-scoped data to speed up a following import")
	ImportCmd.Flags().BoolVarP(&preImport, "step-one", "1", false, "perform step one of a multi-step import: alias for `pre-import`")
	ImportCmd.Flags().BoolVarP(&importAllRepos, "all-repositories", "r", false, "import all repository-scoped data")
	ImportCmd.Flags().BoolVarP(&importAllRepos, "step-two", "2", false, "perform step two of a multi-step import: alias for `all-repositories`")
	ImportCmd.Flags().BoolVarP(&importCommonBlobs, "common-blobs", "B", false, "import all blob metadata from common storage")
	ImportCmd.Flags().BoolVarP(&importCommonBlobs, "step-three", "3", false, "perform step three of a multi-step import: alias for `common-blobs`")
	ImportCmd.Flags().BoolVarP(&logToSTDOUT, "log-to-stdout", "l", false, "write detailed log to std instead of showing progress bars")
	ImportCmd.Flags().BoolVarP(&dynamicMediaTypes, "dynamic-media-types", "m", true, "record unknown media types during import")
	ImportCmd.Flags().StringVarP(&debugAddr, "debug-server", "s", "", "run a pprof debug server at <address:port>")
	ImportCmd.Flags().VarP(nullableInt{&tagConcurrency}, "tag-concurrency", "t", "limit the number of tags to retrieve concurrently, only applicable on gcs backed storage")

	BBMCmd.AddCommand(BBMStatusCmd)
	BBMCmd.AddCommand(BBMPauseCmd)
	BBMCmd.AddCommand(BBMResumeCmd)
	BBMCmd.AddCommand(BBMRunCmd)
	BBMRunCmd.Flags().VarP(nullableInt{&maxBBMJobRetry}, "max-job-retry", "r", "Set the maximum number of job retry attempts (default 2, must be between 1 and 10)")

	RootCmd.SetFlagErrorFunc(func(c *cobra.Command, err error) error {
		return fmt.Errorf("%w\n\n%s", err, c.UsageString())
	})
}

// Command flag vars
var (
	debugAddr          string
	dryRun             bool
	force              bool
	maxNumMigrations   *int
	removeUntagged     bool
	showVersion        bool
	skipPostDeployment bool
	upToDateCheck      bool
	preImport          bool
	rowCount           bool
	importCommonBlobs  bool
	importAllRepos     bool
	tagConcurrency     *int
	logToSTDOUT        bool
	dynamicMediaTypes  bool
	maxBBMJobRetry     *int
)

var parallelwalkKey = "parallelwalk"

// nullableInt implements spf13/pflag#Value as a custom nullable integer to capture spf13/cobra command flags.
// https://pkg.go.dev/github.com/spf13/pflag?tab=doc#Value
type nullableInt struct {
	ptr **int
}

func (f nullableInt) String() string {
	if *f.ptr == nil {
		return "0"
	}
	return strconv.Itoa(**f.ptr)
}

func (nullableInt) Type() string {
	return "int"
}

func (f nullableInt) Set(s string) error {
	v, err := strconv.Atoi(s)
	if err != nil {
		return err
	}
	*f.ptr = &v
	return nil
}

// RootCmd is the main command for the 'registry' binary.
var RootCmd = &cobra.Command{
	Use:           "registry",
	Short:         "`registry`",
	Long:          "`registry`",
	SilenceErrors: true,
	SilenceUsage:  true,
	RunE: func(cmd *cobra.Command, _ []string) error {
		if showVersion {
			version.PrintVersion()
			return nil
		}
		return cmd.Usage()
	},
}

// GCCmd is the cobra command that corresponds to the garbage-collect subcommand
var GCCmd = &cobra.Command{
	Use:   "garbage-collect <config>",
	Short: "`garbage-collect` deletes layers not referenced by any manifests",
	Long:  "`garbage-collect` deletes layers not referenced by any manifests",
	RunE: func(_ *cobra.Command, args []string) error {
		config, err := resolveConfiguration(args)
		if err != nil {
			return fmt.Errorf("configuration error: %w", err)
		}

		if config.Database.Enabled {
			return errors.New("the garbage-collect command is not compatible with database metadata, please use online garbage collection instead")
		}

		maxParallelManifestGets := 1
		parameters := config.Storage.Parameters()
		if v, ok := (parameters[parallelwalkKey]).(bool); ok && v {
			maxParallelManifestGets = 10
		}

		driver, err := factory.Create(config.Storage.Type(), parameters)
		if err != nil {
			return fmt.Errorf("failed to construct %s driver: %w", config.Storage.Type(), err)
		}

		ctx := dcontext.Background()
		ctx, err = configureLogging(ctx, config)
		if err != nil {
			return fmt.Errorf("unable to configure logging with config: %w", err)
		}

		logrus.Debugf("getting a maximum of %d manifests in parallel per repository during the mark phase", maxParallelManifestGets)

		k, err := libtrust.GenerateECP256PrivateKey()
		if err != nil {
			return fmt.Errorf("generating ECP256 private key: %w", err)
		}

		registry, err := storage.NewRegistry(ctx, driver, storage.Schema1SigningKey(k))
		if err != nil {
			return fmt.Errorf("failed to construct registry: %w", err)
		}

		if debugAddr != "" {
			go func() {
				dcontext.GetLoggerWithField(ctx, "address", debugAddr).Info("debug server listening")
				// nolint: gosec // this is just a debug server
				if err := http.ListenAndServe(debugAddr, nil); err != nil {
					dcontext.GetLoggerWithField(ctx, "error", err).Fatal("error listening on debug interface")
				}
			}()
		}

		err = storage.MarkAndSweep(ctx, driver, registry, storage.GCOpts{
			DryRun:                  dryRun,
			RemoveUntagged:          removeUntagged,
			MaxParallelManifestGets: maxParallelManifestGets,
		})
		if err != nil {
			return fmt.Errorf("failed to garbage collect: %w", err)
		}
		return nil
	},
}

// DBCmd is the root of the `database` command.
var DBCmd = &cobra.Command{
	Use:   "database",
	Short: "Manages the registry metadata database",
	Long:  "Manages the registry metadata database",
	RunE: func(cmd *cobra.Command, args []string) error {
		return cmd.Usage()
	},
}

// MigrateCmd is the `migrate` sub-command of `database` that manages database migrations.
var MigrateCmd = &cobra.Command{
	Use:   "migrate",
	Short: "Manage migrations",
	Long:  "Manage migrations",
	RunE: func(cmd *cobra.Command, _ []string) error {
		return cmd.Usage()
	},
}

var MigrateUpCmd = &cobra.Command{
	Use:   "up",
	Short: "Apply up migrations",
	Long:  "Apply up migrations",
	RunE: func(_ *cobra.Command, args []string) error {
		config, err := resolveConfiguration(args, configuration.WithoutStorageValidation())
		if err != nil {
			return fmt.Errorf("configuration error: %w", err)
		}

		if maxNumMigrations == nil {
			var all int
			maxNumMigrations = &all
		} else if *maxNumMigrations < 1 {
			return errors.New("limit must be greater than or equal to 1")
		}

		db, err := migrationDBFromConfig(config)
		if err != nil {
			return fmt.Errorf("failed to construct database connection: %w", err)
		}

		opts := make([]migrations.MigratorOption, 0)
		if skipPostDeployment {
			opts = append(opts, migrations.SkipPostDeployment())
		}
		m := migrations.NewMigrator(db, opts...)

		plan, err := m.UpNPlan(*maxNumMigrations)
		if err != nil {
			return fmt.Errorf("failed to prepare Up plan: %w", err)
		}

		if len(plan) > 0 {
			_, _ = fmt.Println(strings.Join(plan, "\n"))
		}

		if !dryRun {
			start := time.Now()
			mr, err := m.UpN(*maxNumMigrations)
			if err != nil {
				return fmt.Errorf("failed to run database migrations: %w", err)
			}
			fmt.Printf("OK: applied %d migrations and %d background migrations in %.3fs\n", mr.AppliedCount, mr.AppliedBBMCount, time.Since(start).Seconds())
		}
		return nil
	},
}

var MigrateDownCmd = &cobra.Command{
	Use:   "down",
	Short: "Apply down migrations",
	Long:  "Apply down migrations",
	RunE: func(_ *cobra.Command, args []string) error {
		config, err := resolveConfiguration(args, configuration.WithoutStorageValidation())
		if err != nil {
			return fmt.Errorf("configuration error: %w", err)
		}

		if maxNumMigrations == nil {
			var all int
			maxNumMigrations = &all
		} else if *maxNumMigrations < 1 {
			return errors.New("limit must be greater than or equal to 1")
		}

		db, err := migrationDBFromConfig(config)
		if err != nil {
			return fmt.Errorf("failed to construct database connection: %w", err)
		}

		m := migrations.NewMigrator(db)
		plan, err := m.DownNPlan(*maxNumMigrations)
		if err != nil {
			return fmt.Errorf("failed to prepare Down plan: %w", err)
		}

		if len(plan) > 0 {
			_, _ = fmt.Println(strings.Join(plan, "\n"))
		}

		if !dryRun && len(plan) > 0 {
			if !force {
				var response string
				_, _ = fmt.Print("Preparing to apply the above down migrations. Are you sure? [y/N] ")
				_, err := fmt.Scanln(&response)
				if err != nil && errors.Is(err, io.EOF) {
					return fmt.Errorf("failed to scan user input: %w", err)
				}
				if !regexp.MustCompile(`(?i)^y(es)?$`).MatchString(response) {
					return nil
				}
			}

			start := time.Now()
			n, err := m.DownN(*maxNumMigrations)
			if err != nil {
				return fmt.Errorf("failed to run database migrations: %w", err)
			}
			fmt.Printf("OK: applied %d migrations in %.3fs\n", n, time.Since(start).Seconds())
		}
		return nil
	},
}

// MigrateVersionCmd is the `version` sub-command of `database migrate` that shows the current migration version.
var MigrateVersionCmd = &cobra.Command{
	Use:   "version",
	Short: "Show current migration version",
	Long:  "Show current migration version",
	RunE: func(_ *cobra.Command, args []string) error {
		config, err := resolveConfiguration(args, configuration.WithoutStorageValidation())
		if err != nil {
			return fmt.Errorf("configuration error: %w", err)
		}

		db, err := migrationDBFromConfig(config)
		if err != nil {
			return fmt.Errorf("failed to construct database connection: %w", err)
		}

		m := migrations.NewMigrator(db)
		v, err := m.Version()
		if err != nil {
			return fmt.Errorf("failed to detect database version: %w", err)
		}
		if v == "" {
			v = "Unknown"
		}

		fmt.Printf("%s\n", v)
		return nil
	},
}

// MigrateStatusCmd is the `status` sub-command of `database migrate` that shows the migrations status.
var MigrateStatusCmd = &cobra.Command{
	Use:   "status",
	Short: "Show migration status",
	Long:  "Show migration status",
	RunE: func(_ *cobra.Command, args []string) error {
		config, err := resolveConfiguration(args, configuration.WithoutStorageValidation())
		if err != nil {
			return fmt.Errorf("configuration error: %w", err)
		}

		db, err := migrationDBFromConfig(config)
		if err != nil {
			return fmt.Errorf("failed to construct database connection: %w", err)
		}

		m := migrations.NewMigrator(db)
		statuses, err := m.Status()
		if err != nil {
			return fmt.Errorf("failed to detect database status: %w", err)
		}

		if upToDateCheck {
			upToDate := true
			for _, s := range statuses {
				if s.AppliedAt == nil {
					if !s.PostDeployment || !skipPostDeployment {
						upToDate = false
						break
					}
				}
			}
			_, err = fmt.Println(upToDate)
			if err != nil {
				return fmt.Errorf("printing line: %w", err)
			}
			return nil
		}

		table := tablewriter.NewWriter(os.Stdout)
		table.SetHeader([]string{"Migration", "Applied"})
		table.SetColWidth(80)

		// Display table rows sorted by migration ID
		var ids []string
		for id := range statuses {
			ids = append(ids, id)
		}
		sort.Strings(ids)

		for _, id := range ids {
			if statuses[id].PostDeployment && skipPostDeployment {
				continue
			}
			name := id
			if statuses[id].Unknown {
				name += " (unknown)"
			}

			if statuses[id].PostDeployment {
				name += " (post deployment)"
			}

			var appliedAt string
			if statuses[id].AppliedAt != nil {
				appliedAt = statuses[id].AppliedAt.String()
			}

			table.Append([]string{name, appliedAt})
		}

		table.Render()
		return nil
	},
}

// ImportCmd is the `import` sub-command of `database` that imports metadata from the filesystem into the database.
var ImportCmd = &cobra.Command{
	Use:   "import",
	Short: "Import filesystem metadata into the database",
	Long: "Import filesystem metadata into the database.\n" +
		"Untagged manifests are not imported.\n " +
		"This tool can not be used with the parallelwalk storage configuration enabled.",
	RunE: func(_ *cobra.Command, args []string) error {
		// Ensure no more than one step flag is set.
		if preImport && (importAllRepos || importCommonBlobs) {
			return errors.New("steps two or three can't be used with step one")
		}

		if importAllRepos && importCommonBlobs {
			return errors.New("step three can't be used with step two")
		}

		config, err := resolveConfiguration(args)
		if err != nil {
			return fmt.Errorf("configuration error: %w", err)
		}

		if tagConcurrency != nil && (*tagConcurrency < 1 || *tagConcurrency > 5) {
			return errors.New("tag-concurrency must be between 1 and 5")
		}

		parameters := config.Storage.Parameters()
		if (parameters[parallelwalkKey]).(bool) {
			parameters[parallelwalkKey] = false
			logrus.Info("the 'parallelwalk' configuration parameter has been disabled")
		}

		driver, err := factory.Create(config.Storage.Type(), parameters)
		if err != nil {
			return fmt.Errorf("failed to construct %s driver: %w", config.Storage.Type(), err)
		}

		ctx := dcontext.Background()
		ctx, err = configureLogging(ctx, config)
		if err != nil {
			return fmt.Errorf("unable to configure logging with config: %w", err)
		}

		k, err := libtrust.GenerateECP256PrivateKey()
		if err != nil {
			return fmt.Errorf("generatibng ECP256 private key")
		}

		registry, err := storage.NewRegistry(ctx, driver, storage.Schema1SigningKey(k))
		if err != nil {
			return fmt.Errorf("failed to construct registry: %w", err)
		}

		db, err := dbFromConfig(config)
		if err != nil {
			return fmt.Errorf("failed to construct database connection: %w", err)
		}

		m := migrations.NewMigrator(db)
		pending, err := m.HasPending()
		if err != nil {
			return fmt.Errorf("failed to check database migrations status: %w", err)
		}
		if pending {
			return errors.New("there are pending database migrations, use the 'registry database migrate' CLI " +
				"command to check and apply them")
		}

		if debugAddr != "" {
			go func() {
				dcontext.GetLoggerWithField(ctx, "address", debugAddr).Info("debug server listening")
				// nolint: gosec // this is just a debug server
				if err := http.ListenAndServe(debugAddr, nil); err != nil {
					dcontext.GetLogger(ctx).WithError(err).Fatal("error listening on debug interface")
				}
			}()
		}

		var opts []datastore.ImporterOption
		if dryRun {
			opts = append(opts, datastore.WithDryRun)
		}
		if rowCount {
			opts = append(opts, datastore.WithRowCount)
		}
		if tagConcurrency != nil {
			if config.Storage.Type() != "gcs" {
				return errors.New("the tag concurrency option is only compatible with a gcs backed registry storage")
			}
			opts = append(opts, datastore.WithTagConcurrency(*tagConcurrency))
		}
		if !logToSTDOUT {
			opts = append(opts, datastore.WithProgressBar)
		}

		err = os.Setenv(feature.DynamicMediaTypes.EnvVariable, strconv.FormatBool(dynamicMediaTypes))
		if err != nil {
			return fmt.Errorf("failed to set environment variable %s: %w",
				feature.DynamicMediaTypes.EnvVariable, err)
		}

		p := datastore.NewImporter(db, registry, opts...)

		switch {
		case preImport:
			err = p.PreImportAll(ctx)
		case importAllRepos:
			err = p.ImportAllRepositories(ctx)
		case importCommonBlobs:
			err = p.ImportBlobs(ctx)
		default:
			err = p.FullImport(ctx)
		}

		if err != nil {
			return fmt.Errorf("failed to import metadata: %w", err)
		}
		return nil
	},
}

// BBMCmd is the cobra command that corresponds to the background-migrate subcommand
var BBMCmd = &cobra.Command{
	Use:   "background-migrate <config> {status|pause|run}",
	Short: "Manage batched background migrations",
	Long:  "Manage batched background migrations",
	RunE: func(cmd *cobra.Command, _ []string) error {
		return cmd.Usage()
	},
}

// BBMStatusCmd is the `status` sub-command of `background-migrate` that shows the batched background migrations status.
var BBMStatusCmd = &cobra.Command{
	Use:   "status <config>",
	Short: "Show the current status of all batched background migrations",
	Long:  "Show the current status of all batched background migrations.",
	RunE: func(_ *cobra.Command, args []string) error {
		config, err := resolveConfiguration(args, configuration.WithoutStorageValidation())
		if err != nil {
			return fmt.Errorf("configuration error: %w", err)
		}

		db, err := migrationDBFromConfig(config)
		if err != nil {
			return fmt.Errorf("failed to construct database connection: %w", err)
		}

		bbmw := bbm.NewWorker(nil, bbm.WithDB(db))
		bbMigrations, err := bbmw.AllMigrations(dcontext.Background())
		if err != nil {
			return fmt.Errorf("failed to fetch background migrations: %w", err)
		}

		table := tablewriter.NewWriter(os.Stdout)
		table.SetHeader([]string{"Batched Background Migration", "Status"})
		table.SetColWidth(80)

		// Display table rows
		for _, bbm := range bbMigrations {
			table.Append([]string{bbm.Name, bbm.Status.String()})
		}

		table.Render()
		return nil
	},
}

// BBMPauseCmd is the `pause` sub-command of `background-migrate` that pauses a batched background migrations.
var BBMPauseCmd = &cobra.Command{
	Use:   "pause <config>",
	Short: "Pause all running or active batched background migrations",
	Long:  "Pause all running or active batched background migrations",
	RunE: func(_ *cobra.Command, args []string) error {
		config, err := resolveConfiguration(args, configuration.WithoutStorageValidation())
		if err != nil {
			return fmt.Errorf("configuration error: %w", err)
		}

		db, err := migrationDBFromConfig(config)
		if err != nil {
			return fmt.Errorf("failed to construct database connection: %w", err)
		}

		bbmw := bbm.NewWorker(nil, bbm.WithDB(db))
		err = bbmw.PauseEligibleMigrations(dcontext.Background())
		if err != nil {
			return fmt.Errorf("failed to pause background migrations: %w", err)
		}
		return nil
	},
}

// BBMResumeCmd is the `resume` sub-command of `background-migrate` that resumes all previously paused batched background migrations.
var BBMResumeCmd = &cobra.Command{
	Use:   "resume",
	Short: "Resume all paused batched background migrations",
	Long:  "Resume all paused batched background migrations",
	RunE: func(_ *cobra.Command, args []string) error {
		config, err := resolveConfiguration(args, configuration.WithoutStorageValidation())
		if err != nil {
			return fmt.Errorf("configuration error: %w", err)
		}

		db, err := migrationDBFromConfig(config)
		if err != nil {
			return fmt.Errorf("failed to construct database connection: %w", err)
		}

		bbmw := bbm.NewWorker(nil, bbm.WithDB(db))
		err = bbmw.ResumeEligibleMigrations(dcontext.Background())
		if err != nil {
			return fmt.Errorf("failed to resume background migrations: %w", err)
		}
		return nil
	},
}

// BBMRunCmd is the `run` sub-command of `background-migrate` that runs unfinished background migration.
var BBMRunCmd = &cobra.Command{
	Use:   "run <config> [--max-job-retry <n>]",
	Short: "Run all unfinished batched background migrations",
	Long:  "Run all unfinished batched background migrations",
	RunE: func(_ *cobra.Command, args []string) error {
		config, err := resolveConfiguration(args, configuration.WithoutStorageValidation())
		if err != nil {
			return fmt.Errorf("configuration error: %w", err)
		}

		db, err := migrationDBFromConfig(config)
		if err != nil {
			return fmt.Errorf("failed to construct database connection: %w", err)
		}

		// Set default max job retry if not set, and validate its range
		if maxBBMJobRetry == nil {
			defaultBBMJobRetry := 2
			maxBBMJobRetry = &defaultBBMJobRetry
		} else if *maxBBMJobRetry < 1 || *maxBBMJobRetry > 10 {
			return errors.New("limit must be greater than 0 and less than 10")
		}

		// Create a new sync worker with the database and max job attempt options, and run it
		wk := bbm.NewSyncWorker(db, bbm.WithSyncMaxJobAttempt(*maxBBMJobRetry))

		// Unpause any paused background migrations so they can be processed by the worker in `run` below
		err = wk.ResumeEligibleMigrations(dcontext.Background())
		if err != nil {
			return fmt.Errorf("failed to resume background migrations: %w", err)
		}

		retryRunInterval := 10 * time.Second
		for {
			err := wk.Run(dcontext.Background())
			if err != nil {
				_, _ = fmt.Fprintf(os.Stderr, "running background migrations failed: %v\n", err)

				// keep retrying to run at a fixed interval until user stops command.
				_, _ = fmt.Fprintf(os.Stdout, "retrying run in %v...\n", retryRunInterval)
				time.Sleep(retryRunInterval)
				continue
			}
			break
		}
		return nil
	},
}
