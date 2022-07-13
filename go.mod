module github.com/ln80/storer

go 1.17

require (
	github.com/aws/aws-sdk-go-v2 v1.16.4
	github.com/aws/aws-sdk-go-v2/config v1.15.8
	github.com/aws/aws-sdk-go-v2/credentials v1.12.3
	github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue v1.9.2
	github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression v1.4.8
	github.com/aws/aws-sdk-go-v2/service/dynamodb v1.15.5
	github.com/aws/smithy-go v1.11.2
	github.com/ln80/pii v0.2.1
	github.com/rs/xid v1.3.0
)

require (
	github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.12.5 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.1.11 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.4.5 // indirect
	github.com/aws/aws-sdk-go-v2/internal/ini v1.3.12 // indirect
	github.com/aws/aws-sdk-go-v2/service/dynamodbstreams v1.13.5 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.9.1 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/endpoint-discovery v1.7.5 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.9.5 // indirect
	github.com/aws/aws-sdk-go-v2/service/sso v1.11.6 // indirect
	github.com/aws/aws-sdk-go-v2/service/sts v1.16.6 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
)

require (
	github.com/Masterminds/semver/v3 v3.1.1
	github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream v1.2.0 // indirect
	github.com/aws/aws-sdk-go-v2/feature/s3/manager v1.9.1
	github.com/aws/aws-sdk-go-v2/service/internal/s3shared v1.11.0 // indirect
	github.com/aws/aws-sdk-go-v2/service/s3 v1.24.1
	github.com/aws/aws-sdk-go-v2/service/sqs v1.16.0
	github.com/davecgh/go-spew v1.1.1
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c
)

// replace github.com/ln80/pii v0.0.0 => ../pii
