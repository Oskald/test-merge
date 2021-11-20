package com.bamtechmedia.engage.ecommerceconsumer.hooks;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Date;
import java.util.Map;
import java.util.UUID;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.codedeploy.CodeDeployClient;
import software.amazon.awssdk.services.codedeploy.model.PutLifecycleEventHookExecutionStatusRequest;
import software.amazon.awssdk.services.lambda.LambdaClient;
import software.amazon.awssdk.services.lambda.model.InvokeRequest;
import software.amazon.awssdk.services.lambda.model.InvokeResponse;

public class EcommerceConsumerBT implements RequestHandler<Map<String, String>, Void> {

  protected static final Logger LOG = LogManager.getLogger(EcommerceConsumerBT.class);

  @Override
  public Void handleRequest(Map<String, String> event, Context context) {
    String lifecycleEventHookExecutionId = event.get("LifecycleEventHookExecutionId");
    String deploymentId = event.get("DeploymentId");
    LOG.info(
        "DeploymentId: {}, LifecycleEventHookExecutionId: {}",
        deploymentId,
        lifecycleEventHookExecutionId);

    String uuid = UUID.randomUUID().toString();

    // messaging-devs@disneystreaming.com
    // accountId
    //   prod: a82c3b08-5982-4a2f-a7de-bb82b14c6f43
    //   qa: fea26744-7ea8-4f7c-922b-4c0fcde797cd
    // identityId:
    //   prod: d8c3398f-5a34-4cf9-8608-d19572732a2a
    //   qa: 5551768a-c817-4f40-beec-d68ebcc1c37e

    String env = System.getenv("env");
    String accountId;

    if ("prod".equals(env)) {
      accountId = "a82c3b08-5982-4a2f-a7de-bb82b14c6f43";
    } else {
      accountId = "fea26744-7ea8-4f7c-922b-4c0fcde797cd";
    }

    String json =
        "{\n"
            + "\t\"transactionType\": \"RECURRING\",\n"
            + "\t\"headers\": {\n"
            + "\t\t\"isTest\": false,\n"
            + "\t\t\"accountId\": \""
            + accountId
            + "\",\n"
            + "\t\t\"partner\": \"disney\",\n"
            + "\t\t\"suppressEmail\": false\n"
            + "\t},\n"
            + "\t\"metadata\": {\n"
            + "\t\t\"version\": \"2.2.0\",\n"
            + "\t\t\"source\": \"swf/comm-worker\",\n"
            + "\t\t\"timestamp\": \"2021-08-10T23:26:23.456Z\",\n"
            + "\t\t\"transactionId\": \"urn:dss:transaction:disney:"
            + uuid
            + ":recurring:success:1000028620000:1000590850000:1999199999999917051999000_disney\"\n"
            + "\t},\n"
            + "\t\"payload\": {\n"
            + "\t\t\"status\": \"SUCCESS\",\n"
            + "\t\t\"paymentMethod\": {\n"
            + "\t\t\t\"creditCard\": {\n"
            + "\t\t\t\t\"lastFour\": \"0000\",\n"
            + "\t\t\t\t\"brand\": \"VISA\",\n"
            + "\t\t\t\t\"ownerFullName\": \"Test Email\",\n"
            + "\t\t\t\t\"first6\": \"000000\",\n"
            + "\t\t\t\t\"cardType\": \"DEBIT\",\n"
            + "\t\t\t\t\"issuer\": \"UNKNOWN\"\n"
            + "\t\t\t},\n"
            + "\t\t\t\"paymentMethodId\": \""
            + uuid
            + "\",\n"
            + "\t\t\t\"walletId\": \""
            + uuid
            + "\",\n"
            + "\t\t\t\"billingAddress\": {\n"
            + "\t\t\t\t\"city\": \"Town\",\n"
            + "\t\t\t\t\"state\": \"NJ\",\n"
            + "\t\t\t\t\"postalCode\": \"00000\",\n"
            + "\t\t\t\t\"country\": \"US\"\n"
            + "\t\t\t}\n"
            + "\t\t},\n"
            + "\t\t\"order\": {\n"
            + "\t\t\t\"ref\": \"urn:dss:disney:orders:"
            + uuid
            + "\",\n"
            + "\t\t\t\"date\": \"2020-01-10T19:26:14Z\"\n"
            + "\t\t},\n"
            + "\t\t\"invoiceRef\": \"1000590850000\",\n"
            + "\t\t\"invoiceLineItems\": [{\n"
            + "\t\t\t\"product\": {\n"
            + "\t\t\t\t\"sku\": \"1999199999999917051999000_disney\",\n"
            + "\t\t\t\t\"name\": \"Disney+, Hulu, and ESPN+\",\n"
            + "\t\t\t\t\"description\": \"Disney Bundle: Disney+, Hulu, and ESPN+\",\n"
            + "\t\t\t\t\"billingFrequency\": \"MONTH\",\n"
            + "\t\t\t\t\"categories\": [\"disneyplus_submgmt_products\", \"sash\", \"superbundle\", \"disney_do_not_upgrade\", \"espn_submgmt_products\", \"plan_picker_sb\"],\n"
            + "\t\t\t\t\"entitlements\": [\"DISNEY_HULU_ADS\", \"ESPN_PLUS\", \"DISNEY_PLUS\"]\n"
            + "\t\t\t},\n"
            + "\t\t\t\"pricing\": {\n"
            + "\t\t\t\t\"quantity\": 1,\n"
            + "\t\t\t\t\"unitPrice\": 13.99,\n"
            + "\t\t\t\t\"refundAmount\": 0.0,\n"
            + "\t\t\t\t\"totalDiscount\": 13.98,\n"
            + "\t\t\t\t\"subTotalCost\": 13.99,\n"
            + "\t\t\t\t\"taxAmount\": 0.0,\n"
            + "\t\t\t\t\"totalAmount\": 0.01,\n"
            + "\t\t\t\t\"currency\": \"USD\",\n"
            + "\t\t\t\t\"authCharge\": 0.0,\n"
            + "\t\t\t\t\"inclusiveTaxAmount\": 0.0\n"
            + "\t\t\t},\n"
            + "\t\t\t\"nextRenewalDate\": \"2021-09-10T23:26:16Z\",\n"
            + "\t\t\t\"term\": 1,\n"
            + "\t\t\t\"promotion\": {\n"
            + "\t\t\t\t\"type\": \"DISCOUNT\",\n"
            + "\t\t\t\t\"endDate\": \"2021-09-10T23:26:16Z\"\n"
            + "\t\t\t}\n"
            + "\t\t}],\n"
            + "\t\t\"orderCampaign\": {\n"
            + "\t\t\t\"campaignCode\": \"DISNEY_SB3_PURCHASE_CMPGN\",\n"
            + "\t\t\t\"voucherCode\": \"DISNEY_SB3_PURCHASE_VOCHR\"\n"
            + "\t\t},\n"
            + "\t\t\"paymentAttemptCount\": 1,\n"
            + "\t\t\"originalExpectedExecutionDate\": \"2021-08-10T23:26:16Z\",\n"
            + "\t\t\"invoiceTransactionRef\": \"1000590850000\",\n"
            + "\t\t\"taxReferenceNumber\": \"1000590850000\",\n"
            + "\t\t\"ftCharge\": \"Y\",\n"
            + "\t\t\"offerId\": [\""
            + uuid
            + "\"]\n"
            + "\t}\n"
            + "}";

    String currentVersion = System.getenv("CurrentVersion");
    LambdaClient lambdaClient = LambdaClient.create();
    CodeDeployClient codeDeployClient = CodeDeployClient.create();

    String kinesisEvent =
        "{"
            + "  \"Records\": ["
            + "    {"
            + "      \"eventID\": \"shardId-000000000000:49545115243490985018280067714973144582180062593244200961\","
            + "      \"eventVersion\": \"1.0\","
            + "      \"kinesis\": {"
            + "        \"partitionKey\": \"partitionKey-3\","
            + "        \"data\": \" "
            + Base64.getEncoder().encodeToString(json.getBytes(StandardCharsets.UTF_8))
            + "\","
            + "        \"kinesisSchemaVersion\": \"1.0\",\n"
            + "      \"approximateArrivalTimestamp\": "
            + Long.valueOf(new Date().getTime() / 1000).doubleValue()
            + ","
            + "        \"sequenceNumber\": \"49545115243490985018280067714973144582180062593244200961\""
            + "      },"
            + "      \"invokeIdentityArn\": \"identityarn\","
            + "      \"eventName\": \"aws:kinesis:record\","
            + "      \"eventSourceARN\": \"eventsourcearn\","
            + "      \"eventSource\": \"aws:kinesis\","
            + "      \"awsRegion\": \"us-east-1\""
            + "    }"
            + "  ]"
            + "}";

    InvokeRequest invokeRequest =
        InvokeRequest.builder()
            .functionName(currentVersion)
            .payload(
                SdkBytes.fromByteBuffer(
                    ByteBuffer.wrap(kinesisEvent.getBytes(StandardCharsets.UTF_8))))
            .build();
    InvokeResponse invokeResponse;
    String status = "Failed";
    try {
      invokeResponse = lambdaClient.invoke(invokeRequest);
      status =
          invokeResponse.statusCode() == 200 && invokeResponse.functionError() == null
              ? "Succeeded"
              : "Failed";
      LOG.info(
          "invoke status: "
              + invokeResponse.statusCode()
              + ", invoke response: "
              + invokeResponse.functionError());
    } finally {

      LOG.info("Status was: " + status);

      PutLifecycleEventHookExecutionStatusRequest putLifecycleEventHookExecutionStatusRequest =
          PutLifecycleEventHookExecutionStatusRequest.builder()
              .lifecycleEventHookExecutionId(lifecycleEventHookExecutionId)
              .deploymentId(deploymentId)
              .status(status)
              .build();

      codeDeployClient.putLifecycleEventHookExecutionStatus(
          putLifecycleEventHookExecutionStatusRequest);
    }
    return null;
  }
}
