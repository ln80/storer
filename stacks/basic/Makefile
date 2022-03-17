
.PHONY: build, package

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

build:
	sam build --cached

# package:
# 	sam package

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
	sam delete --no-prompts --stack-name $(STACK_NAME) --region $(REGION)

integration-test/run:
	sam local invoke \
		--no-event \
	 	IntegrationTest \
	 	--config-env $(TEST_ENV) \
		--region $(REGION) \
		--parameter-overrides "$(TEST_PARAMS)" | grep -v errorMessage
