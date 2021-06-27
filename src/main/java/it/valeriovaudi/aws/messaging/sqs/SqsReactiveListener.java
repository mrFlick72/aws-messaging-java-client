package it.valeriovaudi.aws.messaging.sqs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageResponse;

import java.time.Duration;
import java.util.List;
import java.util.function.Function;

import static reactor.core.publisher.Mono.fromCompletionStage;

public class SqsReactiveListener {

    private static final Logger logger = LoggerFactory.getLogger(SqsReactiveListener.class);

    private final ReceiveMessageRequestFactory factory;
    private final Duration sleepingTime;
    private final Flux whileLoopFluxProvider;
    private final SqsAsyncClient sqsAsyncClient;
    private final Function<String, Void> handler;

    public SqsReactiveListener(
            Duration sleepingTime,
            Flux whileLoopFluxProvider,
            ReceiveMessageRequestFactory factory,
            SqsAsyncClient sqsAsyncClient, Function<String, Void> handler) {
        this.sleepingTime = sleepingTime;
        this.whileLoopFluxProvider = whileLoopFluxProvider;
        this.factory = factory;
        this.sqsAsyncClient = sqsAsyncClient;
        this.handler = handler;
    }

    public Flux listen() {
        return whileLoopFluxProvider
                .delayElements(sleepingTime)
                .log()
                .flatMap(req -> handleMessages())
                .doOnComplete(() -> logger.info("subscription completed"))
                .doOnCancel(() -> logger.info("subscription cancelled"))
                .doOnSubscribe((s) -> logger.info("subscription started"))
                .doOnError(Exception.class, (e) -> logger.error("subscription error: ", e));
    }

    private Mono<List<DeleteMessageResponse>> handleMessages() {
        return Flux.from(fromCompletionStage(sqsAsyncClient.receiveMessage(factory.makeAReceiveMessageRequest())))
                .flatMap(response -> Flux.fromIterable(response.messages()))
                .flatMap(message -> {
                    handler.apply(message.body());
                    return Mono.just(message);
                })
                .flatMap(message -> fromCompletionStage(sqsAsyncClient.deleteMessage(factory.makeADeleteMessageRequest(message.receiptHandle()))))
                .collectList()
                .flatMap(deleteMessageResponses -> deleteMessageResponses.size() == 0 ? Mono.empty() : Mono.just(deleteMessageResponses));
    }

    public void start() {
        listen().doOnError(Exception.class, (e) -> {
            try {
                this.start();
            } catch (Exception exception) {
                logger.error("subscription error: ", e);
                logger.error("going to resubscribe again ");
            }
        }).subscribe();
    }

}
