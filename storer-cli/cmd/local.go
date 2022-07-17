/*
Copyright © 2022 NAME HERE <EMAIL ADDRESS>

*/
package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

// localCmd represents the local command
var localCmd = &cobra.Command{
	Use: "local",
	// Short: "",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("local called")
	},
}

func init() {
	rootCmd.AddCommand(localCmd)
}
