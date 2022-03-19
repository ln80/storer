
.PHONY: validate, build, package, publish, test, stackname

TEST_ENV = test
REGION = eu-west-1
# STACK_NAME = storer-basic-test-$(shell /bin/bash -c "echo $$RANDOM")
# STACK_NAME = storer-basic-test
ifndef STACK_NAME
override STACK_NAME = storer-basic-test
endif

TEST_PARAMS = "AppPrefix=$(STACK_NAME)"

stackname:
	echo $(STACK_NAME)

validate:
	sam validate

build:
	sam build --cached

test:
	go test -race -cover ./...

package:
	sam package --output-template-file packaged.yaml --s3-bucket storer-packages

publish:
	sam publish --template packaged.yaml --region eu-west-1

integration-test/deploy:
	sam deploy \
		--no-confirm-changeset \
		--no-fail-on-empty-changeset \
		--stack-name $(STACK_NAME) \
		--config-env $(TEST_ENV) \
		--capabilities CAPABILITY_IAM\
		--region $(REGION) \
		--parameter-overrides "$(TEST_PARAMS)"

integration-test/clear:
	aws s3 rm s3://$(STACK_NAME)-event-bucket --recursive
	sam delete --no-prompts --stack-name $(STACK_NAME) --region $(REGION)

integration-test/run:
	sam local invoke \
		--no-event \
	 	IntegrationTest \
	 	--config-env $(TEST_ENV) \
		--region $(REGION) \
		--parameter-overrides "$(TEST_PARAMS)" | grep -v errorMessage
