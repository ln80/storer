package cmd

import (
	"github.com/spf13/cobra"
)

// eventCmd represents the event command
var eventCmd = &cobra.Command{
	Use: "event",
}

func init() {
	rootCmd.AddCommand(eventCmd)
}
