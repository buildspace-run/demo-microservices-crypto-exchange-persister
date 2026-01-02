package run.buildspace.crypto.price.consumer.infrastructure.adapters.in.messaging;


import com.rabbitmq.client.Channel;
import org.apache.commons.lang3.BooleanUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import run.buildspace.crypto.price.consumer.application.ports.in.ForStorePrice;
import run.buildspace.crypto.price.consumer.domain.model.PriceUpdate;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

@Component
public class RabbitMQConsumer {
    private static final Logger logger = LoggerFactory.getLogger(RabbitMQConsumer.class);
    private final ForStorePrice forStorePrice;

    private final AtomicInteger processedBatches = new AtomicInteger(0);
    private final int defaultBatchSize;
    private volatile int batchSize;
    private final BlockingQueue<MessageBatch> queue = new LinkedBlockingQueue<>();
    private final Map<Integer, SortedSet<MessageBatch>> unAnsweredAcks = new ConcurrentSkipListMap<>();
    private final Comparator<MessageBatch> sortByDeliveryTag = Comparator.comparingLong(MessageBatch::deliveryTag);

    @Autowired
    public RabbitMQConsumer(ForStorePrice forStorePrice, @Value("${rabbitmq.batch-size:25}") int defaultBatchSize) {
        this.forStorePrice = forStorePrice;
        this.defaultBatchSize = defaultBatchSize;
        this.batchSize = defaultBatchSize;
    }

    @RabbitListener(queues = "${rabbitmq.prices-queue}")
    public void receiveMessage(PriceUpdate priceUpdate, Channel channel, @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag) {
        logger.debug("Received priceUpdate: {}", priceUpdate);
        MessageBatch messageBatch = MessageBatch.builder().priceUpdate(priceUpdate).channel(channel).deliveryTag(deliveryTag).build();
        boolean insertedInQueue = queue.offer(messageBatch);
        logger.debug("Message inserted in queue: {}", insertedInQueue);
        unAnsweredAcks.computeIfAbsent(channel.getChannelNumber(), k -> Collections.synchronizedSortedSet(new TreeSet<>(sortByDeliveryTag))).add(messageBatch);
        if (queue.size() >= batchSize) {
            processBatch();
        }
    }

    @Scheduled(fixedDelay = 1000) // Flush cada segundo
    public void scheduledFlush() {
        processBatch();
    }

    @Scheduled(fixedDelay = 5000)
    public void adjustBatchSize() {
        int newBatchSize = batchSize;
        int processedLast5Seconds = processedBatches.getAndSet(0);
        if (processedLast5Seconds > 10) {
            newBatchSize = Math.min(newBatchSize * 2, 500);
        } else {
            newBatchSize = Math.max(batchSize / 2, defaultBatchSize);
        }
        batchSize = newBatchSize;
    }

    @Scheduled(fixedDelay = 1000)
    public void answerAcks() {
        unAnsweredAcks.forEach((channelId, messages) -> {
            processAcksForChannel(messages);
            if (messages.isEmpty()) {
                unAnsweredAcks.remove(channelId);
            }

        });

    }

    private void processBatch() {
        List<MessageBatch> currentBatch = new ArrayList<>();
        queue.drainTo(currentBatch, batchSize);
        if (!currentBatch.isEmpty()) {
            processedBatches.addAndGet(1);
            try {
                for (MessageBatch msg : currentBatch) {
                    forStorePrice.addPrice(msg.priceUpdate());
                    msg.success(true);
                }
                logger.info("Processed batch of {} messages", currentBatch.size());
            } catch (Exception e) {
                logger.error("Error processing batch", e);
                currentBatch.forEach(msg -> msg.success(false));
            }
        }
    }

    private void processAcksForChannel(Set<MessageBatch> messages) {
        List<MessageBatch> toRemove = new ArrayList<>();
        AckRange successRange = new AckRange();
        AckRange failureRange = new AckRange();

        for (MessageBatch msg : messages) {
            if (msg.success() == null) break;

            if (BooleanUtils.isTrue(msg.success())) {
                failureRange.flushAndReset(this::nackBatch);
                successRange.addOrFlush(msg, this::ackBatch);
            } else {
                successRange.flushAndReset(this::ackBatch);
                failureRange.addOrFlush(msg, this::nackBatch);
            }
            toRemove.add(msg);
        }

        // Flush final ranges
        successRange.flushAndReset(this::ackBatch);
        failureRange.flushAndReset(this::nackBatch);

        toRemove.forEach(messages::remove);
    }

    private void ackBatch(MessageBatch batchMessage) {
        // ACK múltiple del último mensaje confirma todos los anteriores
        if (batchMessage != null) {
            try {
                batchMessage.channel().basicAck(batchMessage.deliveryTag(), true);
                logger.debug("ACK batch up to deliveryTag: {}", batchMessage.deliveryTag());
            } catch (IOException e) {
                logger.error("Failed to ACK batch", e);
            }
        }

    }

    private void nackBatch(MessageBatch batchMessage) {
        if (batchMessage != null) {
            try {
                batchMessage.channel().basicNack(batchMessage.deliveryTag(), true, true); // multiple=true, requeue=true
                logger.debug("NACK batch up to deliveryTag: {}", batchMessage.deliveryTag());
            } catch (IOException e) {
                logger.error("Failed to NACK batch", e);
            }
        }
    }
    private static class AckRange {
        private MessageBatch last;

        void addOrFlush(MessageBatch msg, Consumer<MessageBatch> ackFn) {
            if (last == null) {
                last = msg;
            } else if (msg.deliveryTag() == last.deliveryTag() + 1) {
                last = msg;  // Extend range
            } else {
                ackFn.accept(last);  // Flush previous range
                last = msg;          // Start new range
            }
        }

        void flushAndReset(Consumer<MessageBatch> ackFn) {
            if (last != null) {
                ackFn.accept(last);
                last = null;
            }
        }
    }
}
