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
	format             string
	countTags          bool
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

func (f nullableInt) Type() string {
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
	Use:   "registry",
	Short: "`registry`",
	Long:  "`registry`",
	Run: func(cmd *cobra.Command, args []string) {
		if showVersion {
			version.PrintVersion()
			return
		}
		cmd.Usage()
	},
}

// GCCmd is the cobra command that corresponds to the garbage-collect subcommand
var GCCmd = &cobra.Command{
	Use:   "garbage-collect <config>",
	Short: "`garbage-collect` deletes layers not referenced by any manifests",
	Long:  "`garbage-collect` deletes layers not referenced by any manifests",
	Run: func(cmd *cobra.Command, args []string) {
		config, err := resolveConfiguration(args)
		if err != nil {
			fmt.Fprintf(os.Stderr, "configuration error: %v\n", err)
			cmd.Usage()
			os.Exit(1)
		}

		if config.Database.Enabled {
			fmt.Fprintf(os.Stderr, "the garbage-collect command is not compatible with database metadata, please use online garbage collection instead")
			os.Exit(1)
		}

		maxParallelManifestGets := 1
		parameters := config.Storage.Parameters()
		if parameters[parallelwalkKey] == true {
			maxParallelManifestGets = 10
		}

		driver, err := factory.Create(config.Storage.Type(), parameters)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to construct %s driver: %v", config.Storage.Type(), err)
			os.Exit(1)
		}

		ctx := dcontext.Background()
		ctx, err = configureLogging(ctx, config)
		if err != nil {
			fmt.Fprintf(os.Stderr, "unable to configure logging with config: %s", err)
			os.Exit(1)
		}

		logrus.Debugf("getting a maximum of %d manifests in parallel per repository during the mark phase", maxParallelManifestGets)

		k, err := libtrust.GenerateECP256PrivateKey()
		if err != nil {
			fmt.Fprint(os.Stderr, err)
			os.Exit(1)
		}

		registry, err := storage.NewRegistry(ctx, driver, storage.Schema1SigningKey(k))
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to construct registry: %v", err)
			os.Exit(1)
		}

		if debugAddr != "" {
			go func() {
				dcontext.GetLoggerWithField(ctx, "address", debugAddr).Info("debug server listening")
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
			fmt.Fprintf(os.Stderr, "failed to garbage collect: %v", err)
			os.Exit(1)
		}
	},
}

// DBCmd is the root of the `database` command.
var DBCmd = &cobra.Command{
	Use:   "database",
	Short: "Manages the registry metadata database",
	Long:  "Manages the registry metadata database",
	Run: func(cmd *cobra.Command, args []string) {
		cmd.Usage()
	},
}

// MigrateCmd is the `migrate` sub-command of `database` that manages database migrations.
var MigrateCmd = &cobra.Command{
	Use:   "migrate",
	Short: "Manage migrations",
	Long:  "Manage migrations",
	Run: func(cmd *cobra.Command, args []string) {
		cmd.Usage()
	},
}

var MigrateUpCmd = &cobra.Command{
	Use:   "up",
	Short: "Apply up migrations",
	Long:  "Apply up migrations",
	Run: func(cmd *cobra.Command, args []string) {
		config, err := resolveConfiguration(args, configuration.WithoutStorageValidation())
		if err != nil {
			fmt.Fprintf(os.Stderr, "configuration error: %v\n", err)
			cmd.Usage()
			os.Exit(1)
		}

		if maxNumMigrations == nil {
			var all int
			maxNumMigrations = &all
		} else if *maxNumMigrations < 1 {
			fmt.Fprintf(os.Stderr, "limit must be greater than or equal to 1")
			os.Exit(1)
		}

		db, err := migrationDBFromConfig(config)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to construct database connection: %v", err)
			os.Exit(1)
		}

		m := migrations.NewMigrator(db)
		if skipPostDeployment {
			migrations.SkipPostDeployment(m)
		}

		plan, err := m.UpNPlan(*maxNumMigrations)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to prepare Up plan: %v", err)
			os.Exit(1)
		}

		if len(plan) > 0 {
			fmt.Println(strings.Join(plan, "\n"))
		}

		if !dryRun {
			start := time.Now()
			mr, err := m.UpN(*maxNumMigrations)
			if err != nil {
				fmt.Fprintf(os.Stderr, "failed to run database migrations: %v", err)
				os.Exit(1)
			}
			fmt.Printf("OK: applied %d migrations and %d background migrations in %.3fs\n", mr.AppliedCount, mr.AppliedBBMCount, time.Since(start).Seconds())
		}
	},
}

var MigrateDownCmd = &cobra.Command{
	Use:   "down",
	Short: "Apply down migrations",
	Long:  "Apply down migrations",
	Run: func(cmd *cobra.Command, args []string) {
		config, err := resolveConfiguration(args, configuration.WithoutStorageValidation())
		if err != nil {
			fmt.Fprintf(os.Stderr, "configuration error: %v\n", err)
			cmd.Usage()
			os.Exit(1)
		}

		if maxNumMigrations == nil {
			var all int
			maxNumMigrations = &all
		} else if *maxNumMigrations < 1 {
			fmt.Fprintf(os.Stderr, "limit must be greater than or equal to 1")
			os.Exit(1)
		}

		db, err := migrationDBFromConfig(config)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to construct database connection: %v", err)
			os.Exit(1)
		}

		m := migrations.NewMigrator(db)
		plan, err := m.DownNPlan(*maxNumMigrations)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to prepare Down plan: %v", err)
			os.Exit(1)
		}

		if len(plan) > 0 {
			fmt.Println(strings.Join(plan, "\n"))
		}

		if !dryRun && len(plan) > 0 {
			if !force {
				var response string
				fmt.Print("Preparing to apply the above down migrations. Are you sure? [y/N] ")
				_, err := fmt.Scanln(&response)
				if err != nil && errors.Is(err, io.EOF) {
					fmt.Fprintf(os.Stderr, "failed to scan user input: %v", err)
					os.Exit(1)
				}
				if !regexp.MustCompile(`(?i)^y(es)?$`).MatchString(response) {
					return
				}
			}

			start := time.Now()
			n, err := m.DownN(*maxNumMigrations)
			if err != nil {
				fmt.Fprintf(os.Stderr, "failed to run database migrations: %v", err)
				os.Exit(1)
			}
			fmt.Printf("OK: applied %d migrations in %.3fs\n", n, time.Since(start).Seconds())
		}
	},
}

// MigrateVersionCmd is the `version` sub-command of `database migrate` that shows the current migration version.
var MigrateVersionCmd = &cobra.Command{
	Use:   "version",
	Short: "Show current migration version",
	Long:  "Show current migration version",
	Run: func(cmd *cobra.Command, args []string) {
		config, err := resolveConfiguration(args, configuration.WithoutStorageValidation())
		if err != nil {
			fmt.Fprintf(os.Stderr, "configuration error: %v\n", err)
			cmd.Usage()
			os.Exit(1)
		}

		db, err := migrationDBFromConfig(config)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to construct database connection: %v", err)
			os.Exit(1)
		}

		m := migrations.NewMigrator(db)
		v, err := m.Version()
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to detect database version: %v", err)
			os.Exit(1)
		}
		if v == "" {
			v = "Unknown"
		}

		fmt.Printf("%s\n", v)
	},
}

// MigrateStatusCmd is the `status` sub-command of `database migrate` that shows the migrations status.
var MigrateStatusCmd = &cobra.Command{
	Use:   "status",
	Short: "Show migration status",
	Long:  "Show migration status",
	Run: func(cmd *cobra.Command, args []string) {
		config, err := resolveConfiguration(args, configuration.WithoutStorageValidation())
		if err != nil {
			fmt.Fprintf(os.Stderr, "configuration error: %v\n", err)
			cmd.Usage()
			os.Exit(1)
		}

		db, err := migrationDBFromConfig(config)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to construct database connection: %v", err)
			os.Exit(1)
		}

		m := migrations.NewMigrator(db)
		statuses, err := m.Status()
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to detect database status: %v", err)
			os.Exit(1)
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
			fmt.Println(upToDate)
			return
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
	},
}

// ImportCmd is the `import` sub-command of `database` that imports metadata from the filesystem into the database.
var ImportCmd = &cobra.Command{
	Use:   "import",
	Short: "Import filesystem metadata into the database",
	Long: "Import filesystem metadata into the database.\n" +
		"Untagged manifests are not imported.\n " +
		"This tool can not be used with the parallelwalk storage configuration enabled.",
	Run: func(cmd *cobra.Command, args []string) {
		// Ensure no more than one step flag is set.
		if preImport && (importAllRepos || importCommonBlobs) {
			fmt.Fprint(os.Stderr, "steps two or three can't be used with step one\n")
			cmd.Usage()
			os.Exit(1)
		}

		if importAllRepos && importCommonBlobs {
			fmt.Fprint(os.Stderr, "step three can't be used with step two\n")
			cmd.Usage()
			os.Exit(1)
		}

		config, err := resolveConfiguration(args)
		if err != nil {
			fmt.Fprintf(os.Stderr, "configuration error: %v\n", err)
			cmd.Usage()
			os.Exit(1)
		}

		if tagConcurrency != nil && (*tagConcurrency < 1 || *tagConcurrency > 5) {
			fmt.Fprintf(os.Stderr, "tag-concurrency must be between 1 and 5")
			os.Exit(1)
		}

		parameters := config.Storage.Parameters()
		if parameters[parallelwalkKey] == true {
			parameters[parallelwalkKey] = false
			logrus.Info("the 'parallelwalk' configuration parameter has been disabled")
		}

		driver, err := factory.Create(config.Storage.Type(), parameters)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to construct %s driver: %v", config.Storage.Type(), err)
			os.Exit(1)
		}

		ctx := dcontext.Background()
		ctx, err = configureLogging(ctx, config)
		if err != nil {
			fmt.Fprintf(os.Stderr, "unable to configure logging with config: %s", err)
			os.Exit(1)
		}

		k, err := libtrust.GenerateECP256PrivateKey()
		if err != nil {
			fmt.Fprint(os.Stderr, err)
			os.Exit(1)
		}

		registry, err := storage.NewRegistry(ctx, driver, storage.Schema1SigningKey(k))
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to construct registry: %v", err)
			os.Exit(1)
		}

		db, err := dbFromConfig(config)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to construct database connection: %v", err)
			os.Exit(1)
		}

		m := migrations.NewMigrator(db)
		pending, err := m.HasPending()
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to check database migrations status: %v", err)
			os.Exit(1)
		}
		if pending {
			fmt.Fprintf(os.Stderr, "there are pending database migrations, use the 'registry database migrate' CLI "+
				"command to check and apply them")
			os.Exit(1)
		}

		if debugAddr != "" {
			go func() {
				dcontext.GetLoggerWithField(ctx, "address", debugAddr).Info("debug server listening")
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
				fmt.Fprintf(os.Stderr, "the tag concurrency option is only compatible with a gcs backed registry storage")
				os.Exit(1)
			}
			opts = append(opts, datastore.WithTagConcurrency(*tagConcurrency))
		}
		if !logToSTDOUT {
			opts = append(opts, datastore.WithProgressBar)
		}

		os.Setenv(feature.DynamicMediaTypes.EnvVariable, strconv.FormatBool(dynamicMediaTypes))

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
			fmt.Fprintf(os.Stderr, "failed to import metadata: %v", err)
			os.Exit(1)
		}
	},
}

// BBMCmd is the cobra command that corresponds to the background-migrate subcommand
var BBMCmd = &cobra.Command{
	Use:   "background-migrate <config> {status|pause|run}",
	Short: "Manage batched background migrations",
	Long:  "Manage batched background migrations",
	Run: func(cmd *cobra.Command, args []string) {
		cmd.Usage()
	},
}

// BBMStatusCmd is the `status` sub-command of `background-migrate` that shows the batched background migrations status.
var BBMStatusCmd = &cobra.Command{
	Use:   "status <config>",
	Short: "Show the current status of all batched background migrations",
	Long:  "Show the current status of all batched background migrations.",
	Run: func(cmd *cobra.Command, args []string) {
		config, err := resolveConfiguration(args, configuration.WithoutStorageValidation())
		if err != nil {
			fmt.Fprintf(os.Stderr, "configuration error: %v\n", err)
			cmd.Usage()
			os.Exit(1)
		}

		db, err := migrationDBFromConfig(config)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to construct database connection: %v", err)
			os.Exit(1)
		}

		bbmw := bbm.NewWorker(nil, bbm.WithDB(db))
		bbMigrations, err := bbmw.AllMigrations(dcontext.Background())
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to fetch background migrations: %v", err)
			os.Exit(1)
		}

		table := tablewriter.NewWriter(os.Stdout)
		table.SetHeader([]string{"Batched Background Migration", "Status"})
		table.SetColWidth(80)

		// Display table rows
		for _, bbm := range bbMigrations {
			table.Append([]string{bbm.Name, bbm.Status.String()})
		}

		table.Render()
	},
}

// BBMPauseCmd is the `pause` sub-command of `background-migrate` that pauses a batched background migrations.
var BBMPauseCmd = &cobra.Command{
	Use:   "pause <config>",
	Short: "Pause all running or active batched background migrations",
	Long:  "Pause all running or active batched background migrations",
	Run: func(cmd *cobra.Command, args []string) {
		config, err := resolveConfiguration(args, configuration.WithoutStorageValidation())
		if err != nil {
			fmt.Fprintf(os.Stderr, "configuration error: %v\n", err)
			cmd.Usage()
			os.Exit(1)
		}

		db, err := migrationDBFromConfig(config)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to construct database connection: %v", err)
			os.Exit(1)
		}

		bbmw := bbm.NewWorker(nil, bbm.WithDB(db))
		err = bbmw.PauseEligibleMigrations(dcontext.Background())
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to pause background migrations: %v", err)
			os.Exit(1)
		}
	},
}

// BBMResumeCmd is the `resume` sub-command of `background-migrate` that resumes all previously paused batched background migrations.
var BBMResumeCmd = &cobra.Command{
	Use:   "resume",
	Short: "Resume all paused batched background migrations",
	Long:  "Resume all paused batched background migrations",
	Run: func(cmd *cobra.Command, args []string) {
		config, err := resolveConfiguration(args, configuration.WithoutStorageValidation())
		if err != nil {
			fmt.Fprintf(os.Stderr, "configuration error: %v\n", err)
			cmd.Usage()
			os.Exit(1)
		}

		db, err := migrationDBFromConfig(config)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to construct database connection: %v", err)
			os.Exit(1)
		}

		bbmw := bbm.NewWorker(nil, bbm.WithDB(db))
		err = bbmw.ResumeEligibleMigrations(dcontext.Background())
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to resume background migrations: %v", err)
			os.Exit(1)
		}
	},
}

// BBMRunCmd is the `run` sub-command of `background-migrate` that runs unfinished background migration.
var BBMRunCmd = &cobra.Command{
	Use:   "run <config> [--max-job-retry <n>]",
	Short: "Run all unfinished batched background migrations",
	Long:  "Run all unfinished batched background migrations",
	Run: func(cmd *cobra.Command, args []string) {
		config, err := resolveConfiguration(args, configuration.WithoutStorageValidation())
		if err != nil {
			fmt.Fprintf(os.Stderr, "configuration error: %v\n", err)
			cmd.Usage()
			os.Exit(1)
		}

		db, err := migrationDBFromConfig(config)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to construct database connection: %v", err)
			os.Exit(1)
		}

		// Set default max job retry if not set, and validate its range
		if maxBBMJobRetry == nil {
			*maxBBMJobRetry = 2
		} else if *maxBBMJobRetry < 1 || *maxBBMJobRetry > 10 {
			fmt.Fprintf(os.Stderr, "limit must be greater than 0 and less than 10")
			os.Exit(1)
		}

		// Create a new sync worker with the database and max job attempt options, and run it
		if err := bbm.NewSyncWorker(db, bbm.WithSyncMaxJobAttempt(*maxBBMJobRetry)).Run(dcontext.Background()); err != nil {
			fmt.Fprintf(os.Stderr, "running background migrations failed: %v", err)
			os.Exit(1)
		}
	},
}
