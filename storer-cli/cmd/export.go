package cmd

import (
	"github.com/ln80/storer/storer-cli/internal/event"
	"github.com/spf13/cobra"
)

type exportCmd struct {
	Ctx    string
	Src    string
	Dest   string
	Module string
}

func (c *exportCmd) build() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "export",
		Short: "Export events from a source directory to an events shared repo.",
		RunE: func(cmd *cobra.Command, args []string) error {
			if _, err := event.Export(c.Ctx, c.Src, c.Dest, c.Module); err != nil {
				return err
			}
			return nil
		},
	}

	cmd.Flags().StringVarP(&c.Ctx, "ctx", "c", "", "Context aka namespace used as prefix to identify existing events.")
	cmd.Flags().StringVarP(&c.Src, "srcDir", "s", "", "Source directory which contains events to export.")
	cmd.Flags().StringVarP(&c.Dest, "destDir", "d", "", "Path to events shared repo to which events are exported.")
	cmd.Flags().StringVarP(&c.Module, "modulePath", "m", "", "The Go module path of the events shared repo.")

	return cmd
}

func init() {
	eventCmd.AddCommand((&exportCmd{}).build())
}
