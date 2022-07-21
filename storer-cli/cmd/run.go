package cmd

import (
	"context"
	"os/signal"
	"syscall"

	"github.com/ln80/storer/storer-cli/internal/local"
	"github.com/spf13/cobra"
)

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
		Short: "Run storer event store locally.",
		Run: func(cmd *cobra.Command, args []string) {
			ctx := cmd.Context()
			if ctx == nil {
				ctx = context.Background()
			}
			ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
			defer stop()

			local.RunEventStore(ctx, c.AppName, c.DynamodbEndpoint, c.S3Endpoint, c.SQSEndpoint, c.Queues)
		},
	}

	cmd.Flags().StringVarP(&c.AppName, "appName", "a", "", "The cmd will create aws local resources with Application name as prefix.")
	cmd.Flags().StringVarP(&c.DynamodbEndpoint, "dynamodbEndpoint", "d", "", "Dynamodb local endpoiny?")
	cmd.Flags().StringVarP(&c.S3Endpoint, "s3Endpoint", "s", "", "S3 local endpoint.")
	cmd.Flags().StringVarP(&c.SQSEndpoint, "sqsEndpoint", "q", "", "SQS local endpoint.")
	cmd.Flags().StringVarP(&c.Queues, "queues", "m", "", "A map of SQS queues to publish event to.")

	return cmd
}

func init() {
	localCmd.AddCommand((&runCmd{}).build())
}
