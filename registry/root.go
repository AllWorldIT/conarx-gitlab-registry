package registry

import (
	"encoding/json"
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

	"github.com/jszwec/csvutil"

	"github.com/docker/distribution/configuration"
	dcontext "github.com/docker/distribution/context"
	"github.com/docker/distribution/registry/datastore"
	"github.com/docker/distribution/registry/datastore/migrations"
	"github.com/docker/distribution/registry/storage"
	"github.com/docker/distribution/registry/storage/driver/factory"
	"github.com/docker/distribution/registry/storage/inventory"
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
	RootCmd.AddCommand(InventoryCmd)
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
	ImportCmd.Flags().StringVarP(&repoPath, "repository", "r", "", "import a specific repository (all by default)")
	ImportCmd.Flags().StringVarP(&blobTransferDest, "blob-transfer-destination", "t", "", "copy imported blobs to separate bucket (GCS) or root directory (filesystem)")
	ImportCmd.Flags().BoolVarP(&dryRun, "dry-run", "d", false, "do not commit changes to the database")
	ImportCmd.Flags().BoolVarP(&importDanglingBlobs, "dangling-blobs", "b", false, "import all blobs, regardless of whether they are referenced by a manifest or not")
	ImportCmd.Flags().BoolVarP(&importDanglingManifests, "dangling-manifests", "m", false, "import all manifests, regardless of whether they are tagged or not")
	ImportCmd.Flags().BoolVarP(&requireEmptyDatabase, "require-empty-database", "e", false, "abort import if the database is not empty")
	ImportCmd.Flags().BoolVarP(&preImport, "pre-import", "p", false, "import immutable data to speed up a following full import, may only be used in conjunction with the `--repository` option")
	ImportCmd.Flags().BoolVarP(&rowCount, "row-count", "c", false, "count and log number of rows across relevant database tables on (pre)import completion")

	InventoryCmd.Flags().StringVarP(&format, "format", "f", "text", "which format to write output to, text output produces an additional summary for convenience, options: text, json, csv")
	InventoryCmd.Flags().BoolVarP(&countTags, "tag-count", "t", true, "count repository tags, set this to false to increase inventory speed")
}

// Command flag vars
var (
	requireEmptyDatabase    bool
	blobTransferDest        string
	debugAddr               string
	dryRun                  bool
	force                   bool
	importDanglingBlobs     bool
	importDanglingManifests bool
	maxNumMigrations        *int
	removeUntagged          bool
	repoPath                string
	showVersion             bool
	skipPostDeployment      bool
	upToDateCheck           bool
	preImport               bool
	format                  string
	countTags               bool
	rowCount                bool
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

		db, err := dbFromConfig(config)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to construct database connection: %v", err)
			os.Exit(1)
		}

		m := migrations.NewMigrator(db.DB)
		if skipPostDeployment {
			migrations.SkipPostDeployment(m)
		}

		plan, err := m.UpNPlan(*maxNumMigrations)
		if len(plan) > 0 {
			fmt.Println(strings.Join(plan, "\n"))
		}

		if !dryRun {
			start := time.Now()
			n, err := m.UpN(*maxNumMigrations)
			if err != nil {
				fmt.Fprintf(os.Stderr, "failed to run database migrations: %v", err)
				os.Exit(1)
			}
			fmt.Printf("OK: applied %d migrations in %.3fs\n", n, time.Since(start).Seconds())
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

		db, err := dbFromConfig(config)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to construct database connection: %v", err)
			os.Exit(1)
		}

		m := migrations.NewMigrator(db.DB)
		plan, err := m.DownNPlan(*maxNumMigrations)
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

		db, err := dbFromConfig(config)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to construct database connection: %v", err)
			os.Exit(1)
		}

		m := migrations.NewMigrator(db.DB)
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

		db, err := dbFromConfig(config)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to construct database connection: %v", err)
			os.Exit(1)
		}

		m := migrations.NewMigrator(db.DB)
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
		"By default, dangling blobs and untagged manifests are not imported.\n " +
		"Individual repositories may be imported via the --repository option.\n " +
		"This tool can not be used with the parallelwalk storage configuration enabled.",
	Run: func(cmd *cobra.Command, args []string) {
		config, err := resolveConfiguration(args)
		if err != nil {
			fmt.Fprintf(os.Stderr, "configuration error: %v\n", err)
			cmd.Usage()
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

		// Skip postdeployment migrations to prevent pending post deployment
		// migrations from preventing the registry from starting.
		m := migrations.NewMigrator(db.DB, migrations.SkipPostDeployment)
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

		var opts []datastore.ImporterOption
		if importDanglingBlobs {
			opts = append(opts, datastore.WithImportDanglingBlobs)
		}
		if importDanglingManifests {
			opts = append(opts, datastore.WithImportDanglingManifests)
		}
		if dryRun {
			opts = append(opts, datastore.WithDryRun)
		}
		if requireEmptyDatabase {
			opts = append(opts, datastore.WithRequireEmptyDatabase)
		}
		if rowCount {
			opts = append(opts, datastore.WithRowCount)
		}

		if blobTransferDest != "" {
			destParameters := parameters
			switch driver.Name() {
			case "gcs":
				destParameters["bucket"] = blobTransferDest
			case "filesystem":
				destParameters["rootdirectory"] = blobTransferDest
			default:
				fmt.Fprintf(os.Stderr, "%s driver does not support blob transfer", driver.Name())
				os.Exit(1)
			}

			destDriver, err := factory.Create(config.Storage.Type(), destParameters)
			if err != nil {
				fmt.Fprintf(os.Stderr, "failed to construct %s driver for blob transfer: %v", config.Storage.Type(), err)
				os.Exit(1)
			}

			bts, err := storage.NewBlobTransferService(driver, destDriver)
			if err != nil {
				fmt.Fprintf(os.Stderr, "failed to construct blob transfer service: %v", err)
				os.Exit(1)
			}

			opts = append(opts, datastore.WithBlobTransferService(bts))
		}

		p := datastore.NewImporter(db, registry, opts...)

		switch {
		case repoPath == "" && preImport:
			err = errors.New("pre-import is only supported with the `--repository` flag")
		case repoPath == "" && !preImport:
			err = p.ImportAll(ctx)
		case repoPath != "" && preImport:
			err = p.PreImport(ctx, repoPath)
		case repoPath != "" && !preImport:
			err = p.Import(ctx, repoPath)
		}
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to import metadata: %v", err)
			os.Exit(1)
		}
	},
}

// InventoryCmd is a registry subcommand that collects registry data.
var InventoryCmd = &cobra.Command{
	Use:   "inventory <config>",
	Short: "Inventory the registry",
	Long:  "Inventory the registry, collecting information on repositories and (optionally) their tag totals",
	Run: func(cmd *cobra.Command, args []string) {
		config, err := resolveConfiguration(args)
		if err != nil {
			fmt.Fprintf(os.Stderr, "configuration error: %v\n", err)
			cmd.Usage()
			os.Exit(1)
		}

		parameters := config.Storage.Parameters()
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

		registry, err := storage.NewRegistry(ctx, driver)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to construct registry: %v", err)
			os.Exit(1)
		}

		it := inventory.NewTaker(registry, countTags)
		iv, err := it.Run(ctx)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to take inventory: %v", err)
			os.Exit(1)
		}

		switch format {
		case "csv":
			b, err := csvutil.Marshal(iv.Repositories)
			if err != nil {
				fmt.Fprintf(os.Stderr, "failed to marshal inventory: %v", err)
				os.Exit(1)
			}

			fmt.Fprintf(os.Stdout, "%s", b)
		case "json":
			b, err := json.Marshal(iv)
			if err != nil {
				fmt.Fprintf(os.Stderr, "failed to marshal inventory: %v", err)
				os.Exit(1)
			}

			fmt.Fprintf(os.Stdout, "%s", b)
		case "text":
			table := tablewriter.NewWriter(os.Stdout)
			table.SetHeader([]string{"Repository", "Tag Count"})
			table.SetColWidth(80)

			// Display table rows sorted by repository path.
			sort.Slice(iv.Repositories, func(i, j int) bool {
				return iv.Repositories[i].Path < iv.Repositories[j].Path
			})

			for _, repo := range iv.Repositories {
				tc := fmt.Sprintf("%d", repo.TagCount)
				if repo.TagCount == 0 {
					tc = ""
				}
				table.Append([]string{repo.Path, tc})
			}

			table.Render()

			table = tablewriter.NewWriter(os.Stdout)
			table.SetHeader([]string{"Group", "Repository Count", "Tag Count"})
			table.SetColWidth(80)

			groups := iv.Summary().Groups

			// Display table rows sorted by group.
			sort.Slice(groups, func(i, j int) bool {
				return groups[i].Name < groups[j].Name
			})

			for _, g := range groups {
				tc := fmt.Sprintf("%d", g.TagCount)
				if g.TagCount == 0 {
					tc = ""
				}
				table.Append([]string{g.Name, fmt.Sprintf("%d", g.RepositoryCount), tc})
			}

			table.Render()
		default:
			fmt.Fprintf(os.Stderr, "output option must be one of text, json, csv")
			os.Exit(1)
		}
	},
}
