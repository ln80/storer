package utils

import (
	"fmt"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// FromDynamoDBEventMap converts a map of Lambda Event DynamoDB
// AttributeValues, including all nested members, to a dynamodbstreams map of AttributeValue.
func FromDynamoDBEventAVMap(from map[string]events.DynamoDBAttributeValue) (to map[string]types.AttributeValue, err error) {
	to = make(map[string]types.AttributeValue, len(from))
	for field, value := range from {
		to[field], err = FromDynamoDBEventAV(value)
		if err != nil {
			return nil, err
		}
	}

	return to, nil
}

// FromDynamoDBEventList converts a slice of Lambda Event DynamoDB
// AttributeValues, including all nested members, to a slice ofdynamodbstreams AttributeValue.
func FromDynamoDBEventAVList(from []events.DynamoDBAttributeValue) (to []types.AttributeValue, err error) {
	to = make([]types.AttributeValue, len(from))
	for i := 0; i < len(from); i++ {
		to[i], err = FromDynamoDBEventAV(from[i])
		if err != nil {
			return nil, err
		}
	}

	return to, nil
}

// FromDynamoDBEvent converts a Lambda Event DynamoDB AttributeValue, including
// all nested members, to a dynamodbstreams AttributeValue.
func FromDynamoDBEventAV(from events.DynamoDBAttributeValue) (types.AttributeValue, error) {
	switch from.DataType() {
	case events.DataTypeNull:
		return &types.AttributeValueMemberNULL{Value: from.IsNull()}, nil

	case events.DataTypeBoolean:
		return &types.AttributeValueMemberBOOL{Value: from.Boolean()}, nil

	case events.DataTypeBinary:
		return &types.AttributeValueMemberB{Value: from.Binary()}, nil

	case events.DataTypeBinarySet:
		bs := make([][]byte, len(from.BinarySet()))
		for i := 0; i < len(from.BinarySet()); i++ {
			bs[i] = append([]byte{}, from.BinarySet()[i]...)
		}
		return &types.AttributeValueMemberBS{Value: bs}, nil

	case events.DataTypeNumber:
		return &types.AttributeValueMemberN{Value: from.Number()}, nil

	case events.DataTypeNumberSet:
		return &types.AttributeValueMemberNS{Value: append([]string{}, from.NumberSet()...)}, nil

	case events.DataTypeString:
		return &types.AttributeValueMemberS{Value: from.String()}, nil

	case events.DataTypeStringSet:
		return &types.AttributeValueMemberSS{Value: append([]string{}, from.StringSet()...)}, nil

	case events.DataTypeList:
		values, err := FromDynamoDBEventAVList(from.List())
		if err != nil {
			return nil, err
		}
		return &types.AttributeValueMemberL{Value: values}, nil

	case events.DataTypeMap:
		values, err := FromDynamoDBEventAVMap(from.Map())
		if err != nil {
			return nil, err
		}
		return &types.AttributeValueMemberM{Value: values}, nil

	default:
		return nil, fmt.Errorf("unknown AttributeValue union member, %T", from)
	}
}
