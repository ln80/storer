/*
Copyright © 2022 NAME HERE <EMAIL ADDRESS>

*/
package cmd

import (
	"github.com/ln80/storer/storer-cli/internal/local"
	"github.com/spf13/cobra"
)

// runCmd represents the run command
// var runCmd = &cobra.Command{
// 	Use:   "run",
// 	Short: "A brief description of your command",
// 	Long: `A longer description that spans multiple lines and likely contains examples
// and usage of using your command. For example:

// Cobra is a CLI library for Go that empowers applications.
// This application is a tool to generate the needed files
// to quickly create a Cobra application.`,
// 	Run: func(cmd *cobra.Command, args []string) {
// 		fmt.Println("run called")
// 	},
// }

type runCmd struct {
	AppName          string
	DynamodbEndpoint string
	S3Endpoint       string
	SQSEndpoint      string
	Queues           string
}

func (c *runCmd) build() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "run",
		Short: "Export events from a src directory to an events shared repo.",
		RunE: func(cmd *cobra.Command, args []string) error {
			// ctx := cmd.Context()
			// if ctx == nil {
			// ctx := context.Background()
			// }
			return local.Run(c.AppName, c.DynamodbEndpoint, c.S3Endpoint, c.SQSEndpoint, c.Queues)
		},
	}

	cmd.Flags().StringVarP(&c.AppName, "appName", "", "", "The application name. The cmd will create aws local resources with Application name as prefix.")
	cmd.Flags().StringVarP(&c.DynamodbEndpoint, "dynamodbEndpoint", "", "", "dynamodb local endpoiny")
	cmd.Flags().StringVarP(&c.S3Endpoint, "s3Endpoint", "", "", "s3 local endpoint")
	cmd.Flags().StringVarP(&c.SQSEndpoint, "sqsEndpoint", "", "", "sqs local endpoint")
	cmd.Flags().StringVarP(&c.Queues, "queues", "", "", "a map of SQS queues to publish event to")

	return cmd
}

func init() {
	localCmd.AddCommand((&runCmd{}).build())
}
