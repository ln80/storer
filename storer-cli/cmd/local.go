/*
Copyright © 2022 NAME HERE <EMAIL ADDRESS>

*/
package cmd

import (
	"github.com/spf13/cobra"
)

// localCmd represents the local command
var localCmd = &cobra.Command{
	Use:  "local",
	Args: cobra.MinimumNArgs(1),
}

func init() {
	rootCmd.AddCommand(localCmd)
}
