package cmd

import (
	"github.com/spf13/cobra"
	pkgcmd "gitlab.com/gitlab-org/container-registry/devvm/go/pkg/cmd"
)

func newListCmd(_ *string) *cobra.Command {
	var zone string

	listCmd := &cobra.Command{
		Use:     "list",
		Aliases: []string{"show", "ls"},
		Short:   "List devvms",
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return pkgcmd.ListVMs(cmd.Context(), zone)
		},
	}

	listCmd.Flags().StringVarP(
		&zone, "zone", "z",
		"", "GCE zone to list devvms in, default is list from all zones")

	return listCmd
}
