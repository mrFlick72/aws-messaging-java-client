package it.valeriovaudi.aws.messaging.sqs;

import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

public class ReceiveMessageRequestFactory {

    private final String queueUrl;
    private final Integer maxNumberOfMessages;
    private final Integer visibilityTimeout;
    private final Integer waitTimeSeconds;

    public ReceiveMessageRequestFactory(String queueUrl,
                                        Integer maxNumberOfMessages,
                                        Integer visibilityTimeout,
                                        Integer waitTimeSeconds) {
        this.queueUrl = queueUrl;
        this.maxNumberOfMessages = maxNumberOfMessages;
        this.visibilityTimeout = visibilityTimeout;
        this.waitTimeSeconds = waitTimeSeconds;
    }

    public ReceiveMessageRequest makeAReceiveMessageRequest() {
        return ReceiveMessageRequest.builder()
                .maxNumberOfMessages(maxNumberOfMessages)
                .visibilityTimeout(visibilityTimeout)
                .waitTimeSeconds(waitTimeSeconds)
                .queueUrl(queueUrl)
                .build();
    }

    public DeleteMessageRequest makeADeleteMessageRequest(String receiptHandle) {
        return DeleteMessageRequest.builder()
                .receiptHandle(receiptHandle)
                .queueUrl(queueUrl)
                .build();
    }
}
