
start-db:
	# docker rm $(docker ps -aq --filter name=/dynamodb)
	docker run --rm -p 8070:8000 -v dynamodb-local-v1.0:/home/dynamodblocal  --network=lambda-local  --name dynamodb amazon/dynamodb-local -jar DynamoDBLocal.jar -sharedDb -dbPath /home/dynamodblocal

start-s3:
	docker run -p 8071:9000 -p 8072:9001 \
  	quay.io/minio/minio server ./.minio --console-address ":8072" --address ":8071"

test-dynamodb: export DYNAMODB_ENDPOINT = http://localhost:8070
test-dynamodb:
	# docker run --volume=$(pwd):/ws --workdir=/ws champs/dev:test /bin/bash -c "GOPROXY=off go test -mod=vendor ./dynamodb"
	gotest -race -v -cover ./dynamo/...

test-s3: export S3_ENDPOINT = http://127.0.0.1:8071
test-s3:
	gotest -race -v -cover ./s3/...

test-memory:
	gotest -race -v -cover ./memory/... 

test-examples:
	gotest -race -v -cover ./examples/... 

# test: export DYNAMODB_ENDPOINT = http://localhost:8070
# test: export S3_ENDPOINT = http://127.0.0.1:8071
.PHONY: test
test:
	go test -race -cover ./...