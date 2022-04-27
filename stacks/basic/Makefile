
.PHONY: validate, build, package, publish, test, stackname

TEST_ENV = test
REGION = eu-west-1

# STACK_NAME = storer-basic-test-$(shell /bin/bash -c "echo $$RANDOM")

# stack name may be gen by CI
ifndef STACK_NAME
override STACK_NAME = storer-basic-test
endif

TEST_PARAMS = AppPrefix=$(STACK_NAME) S3PartitionMinSize=10

# mainly used by integ test runner
export AWS_REGION=$(REGION)

export DYNAMODB_TABLE=$(STACK_NAME)-event-table
export S3_BUCKET=$(STACK_NAME)-event-bucket
export SQS_QUEUE=$(STACK_NAME)-internal-queue
export LAMBDA_SCHEDULAR_FUNC=$(STACK_NAME)-SchedularFunction


stackname:
	echo $(STACK_NAME)

validate:
	sam validate

build:
	sam build --cached

package:
	sam package --output-template-file packaged.yaml --s3-bucket storer-packages

publish:
	sam publish --template packaged.yaml --region eu-west-1

unit-test/run:
	go test --tags=unit -race -cover ./...

integ-test/deploy:
	sam deploy \
		--no-confirm-changeset \
		--no-fail-on-empty-changeset \
		--stack-name $(STACK_NAME) \
		--config-env $(TEST_ENV) \
		--capabilities CAPABILITY_IAM\
		--region $(REGION) \
		--parameter-overrides $(TEST_PARAMS)

integ-test/clear:
	aws s3 rm s3://$(STACK_NAME)-event-bucket --recursive || true
	sam delete --no-prompts --stack-name $(STACK_NAME) --region $(REGION)

integ-test/run:
	go test --tags=integration -race -cover ./integ_test.go

# old integ test runner with local lambda
# sam local invoke \
	# 	--no-event \
	#  	IntegrationTest \
	#  	--config-env $(TEST_ENV) \
	# 	--region $(REGION) \
	# 	--parameter-overrides "$(TEST_PARAMS)" | grep -v errorMessage
