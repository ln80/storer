package dynamo

import (
	"errors"
	"strings"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go"
	"github.com/aws/smithy-go/transport/http"
)

func IsConditionCheckFailure(err error) bool {
	if strings.Contains(err.Error(), "ConditionalCheckFailedException") {
		return true
	}
	var oe *smithy.OperationError
	if errors.As(err, &oe) {
		var re *http.ResponseError
		if errors.As(err, &re) {
			var tce *types.TransactionCanceledException
			if errors.As(err, &tce) {
				for _, reason := range tce.CancellationReasons {
					if *reason.Code == "ConditionalCheckFailed" {
						return true
					}
				}
			}
		}
	}

	return false
}
