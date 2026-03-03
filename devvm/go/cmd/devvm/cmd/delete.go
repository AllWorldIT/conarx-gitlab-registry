package cmd

import (
	"github.com/spf13/cobra"
	pkgcmd "gitlab.com/gitlab-org/container-registry/devvm/go/pkg/cmd"
)

func newDeleteCmd(_ *string) *cobra.Command {
	var zone string

	deleteCmd := &cobra.Command{
		Use:     "delete",
		Aliases: []string{"remove", "rm", "del"},
		Short:   "Delete a devvm",
		Args:    cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return pkgcmd.DeleteVM(cmd.Context(), zone, args)
		},
	}

	deleteCmd.Flags().StringVarP(&zone, "zone", "z", "", "GCE zone where the devvm resides")

	return deleteCmd
}
