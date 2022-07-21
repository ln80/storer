

# SETTINGS:

DOCKER_NETWORK = lambda-local

DYNAMODB_PORT  = 8070
DYNAMODB_VOLUME = dynamodb-local-v2.0

MINIO_PORT = 8071
MINIO_CONSOLE_PORT = 8072
MINIO_VOLUME = minio-local-v2.0

start-dynamodb:
	docker run -p $(DYNAMODB_PORT):8000 amazon/dynamodb-local -jar DynamoDBLocal.jar -inMemory

start-dynamodb/persist:
	docker volume create $(DYNAMODB_VOLUME)
	docker run --rm \
	-p $(DYNAMODB_PORT):8000 \
	-v $(DYNAMODB_VOLUME):/home/dynamodblocal  \
	--network=$(DOCKER_NETWORK) \
	--name dynamodb amazon/dynamodb-local -jar DynamoDBLocal.jar \
	-sharedDb -dbPath /home/dynamodblocal

start-s3:
	docker run \
	-p 8071:9000 \
	-p 8072:9001 \
  	minio/minio server /data --console-address ":9001" --address ":9000"

start-s3/persist:
	docker volume create $(MINIO_VOLUME)
	docker run \
	-p 8071:9000 \
	-p 8072:9001 \
	-v $(MINIO_VOLUME):/data  \
  	minio/minio server /data --console-address ":9001" --address ":9000"

test:
	go test -race -cover ./...

test/local: test/s3 test/dynamodb
	gotest -race -v -cover ./...

test/dynamodb: export DYNAMODB_ENDPOINT = http://localhost:$(DYNAMODB_PORT)
test/dynamodb:
	gotest -race -v -cover ./dynamo/...

test/s3: export S3_ENDPOINT = http://localhost:$(MINIO_PORT)
test/s3:
	gotest -race -v -cover ./s3/...

test/memory:
	gotest -race -v -cover ./memory/... 

examples:
	gotest -race -v -cover ./examples/...