
.PHONY: build, package

TEST_ENV = test
REGION = eu-west-1
STACK_NAME = storer-basic-test
TEST_PARAMS = "AppPrefix=$(STACK_NAME)"

build:
	sam build --cached

# package:
# 	sam package

integration_test/deploy:
	sam deploy \
		--stack-name $(STACK_NAME) \
		--config-env $(TEST_ENV) \
		--capabilities CAPABILITY_IAM\
		--region $(REGION) \
		--parameter-overrides "$(TEST_PARAMS)"

integration_test/clear:
	sam delete  --stack-name $(STACK_NAME) --region $(REGION)

integration_test/run:
	sam local invoke \
		--no-event \
	 	IntegrationTest \
	 	--config-env $(TEST_ENV) \
		--region $(REGION) \
		--parameter-overrides "$(TEST_PARAMS)" | grep -v errorMessage
