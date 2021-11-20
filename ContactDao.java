package com.bamtechmedia.engage.ecommerceconsumer.dao;

import com.bamtechmedia.engage.ecommerceconsumer.model.Contact;
import java.util.HashMap;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;

public class ContactDao {

  private final DynamoDbAsyncClient dynamoDbAsyncClient;
  private final String tableName;

  public ContactDao(DynamoDbAsyncClient dynamoDbAsyncClient, String tableName) {
    this.dynamoDbAsyncClient = dynamoDbAsyncClient;
    this.tableName = tableName;
  }

  public CompletableFuture<Optional<Contact>> asyncGetDefaultSendableContact(String accountId) {
    HashMap<String, String> attrNameAlias = new HashMap<>();
    attrNameAlias.put("#ACCTID", "accountId");
    attrNameAlias.put("#ISDEFAULT", "isDefault");

    HashMap<String, AttributeValue> attrValues = new HashMap<>();

    attrValues.put(":ACCTID", AttributeValue.builder().s(accountId).build());
    attrValues.put(":ISDEFAULT", AttributeValue.builder().s("true").build());

    QueryRequest queryRequest =
        QueryRequest.builder()
            .tableName(tableName)
            .indexName("AccntIdIsSendableIdx")
            .keyConditionExpression("#ACCTID = :ACCTID")
            .filterExpression("#ISDEFAULT = :ISDEFAULT and attribute_exists(email)")
            .expressionAttributeNames(attrNameAlias)
            .expressionAttributeValues(attrValues)
            .build();

    return dynamoDbAsyncClient
        .query(queryRequest)
        .thenApply(
            queryResponse ->
                queryResponse
                    .items()
                    .stream()
                    .findFirst()
                    .map(
                        item ->
                            new Contact(
                                item.getOrDefault("email", AttributeValue.builder().build()).s(),
                                item.getOrDefault("language", AttributeValue.builder().build()).s(),
                                item.getOrDefault("country", AttributeValue.builder().build())
                                    .s())));
  }
}
