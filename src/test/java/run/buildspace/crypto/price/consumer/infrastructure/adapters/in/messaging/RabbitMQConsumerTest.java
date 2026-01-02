package run.buildspace.crypto.price.consumer.infrastructure.adapters.in.messaging;

import com.rabbitmq.client.Channel;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;
import run.buildspace.crypto.price.consumer.application.ports.in.ForStorePrice;
import run.buildspace.crypto.price.consumer.domain.model.PriceUpdate;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.Objects;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.then;
import static org.mockito.BDDMockito.willThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;

@ExtendWith(MockitoExtension.class)
class RabbitMQConsumerTest {


    private RabbitMQConsumer rabbitMQConsumer;

    @Mock
    private ForStorePrice forStorePrice;

    @Mock
    private Channel channel;
    private final Comparator<MessageBatch> sortByDeliveryTag = Comparator.comparingLong(MessageBatch::deliveryTag);
    private AtomicInteger processedBatches;
    private BlockingQueue<MessageBatch> queue;
    private Map<Integer, SortedSet<MessageBatch>> unAnsweredAcks;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        int defaultBatchSize = 25;
        rabbitMQConsumer = new RabbitMQConsumer(forStorePrice, defaultBatchSize);
        processedBatches = (AtomicInteger) ReflectionTestUtils.getField(rabbitMQConsumer, "processedBatches");
        queue = (BlockingQueue<MessageBatch>) ReflectionTestUtils.getField(rabbitMQConsumer, "queue");
        unAnsweredAcks = (Map<Integer, SortedSet<MessageBatch>>) ReflectionTestUtils.getField(rabbitMQConsumer, "unAnsweredAcks");
    }

    @Test
    void receiveMessageTest() {
        //when
        rabbitMQConsumer.receiveMessage(Mocks.priceUpdate(), channel, 0);
        //then
        assertEquals(1, queue.size());
        assertEquals(Mocks.priceUpdate(), Objects.requireNonNull(queue.poll()).priceUpdate());
    }
    @Test
    void receiveMessageExecuteBatchTest() {
        //given
        IntStream.range(0, 24).forEach(i -> queue.offer(Mocks.messageBatch(channel,i)));
        //when
        rabbitMQConsumer.receiveMessage(Mocks.priceUpdate(), channel, 0);
        then(forStorePrice).should(times(25)).addPrice(Mocks.priceUpdate());
        //then
        assertTrue(queue.isEmpty());
        unAnsweredAcks.values().forEach(currentChannel -> currentChannel.forEach(msg -> assertTrue(msg.success())));
    }

    @Test
    void receiveMessageExecuteBatchErrorTest() {
        //given
        IntStream.range(0, 24).forEach(i -> queue.offer(Mocks.messageBatch(channel,i)));
        willThrow(new RuntimeException()).given(forStorePrice).addPrice(any());
        //when
        rabbitMQConsumer.receiveMessage(Mocks.priceUpdate(), channel, 0);
        then(forStorePrice).should(times(1)).addPrice(Mocks.priceUpdate());
        //then
        assertTrue(queue.isEmpty());
        unAnsweredAcks.values().forEach(currentChannel -> currentChannel.forEach(msg -> assertFalse(msg.success())));
    }

    @Test
    void scheduledFlushTest() {
        //given
        queue.offer(Mocks.messageBatch(channel,0));

        //when
        rabbitMQConsumer.scheduledFlush();

        //then
        then(forStorePrice).should(times(1)).addPrice(Mocks.priceUpdate());
        assertTrue(queue.isEmpty());

    }

    @Test
    void adjustBatchSizeIncrementTest() {
        //given
        processedBatches.addAndGet(12);

        //when
        rabbitMQConsumer.adjustBatchSize();

        //then
        assertEquals(50, ReflectionTestUtils.getField(rabbitMQConsumer, "batchSize"));
        assertEquals(0, processedBatches.get());
    }

    @Test
    void adjustBatchSizeDencrementTest() {
        //given
        ReflectionTestUtils.setField(rabbitMQConsumer, "batchSize", 50);
        processedBatches.addAndGet(5);

        //when
        rabbitMQConsumer.adjustBatchSize();

        //then
        assertEquals(25, ReflectionTestUtils.getField(rabbitMQConsumer, "batchSize"));
        assertEquals(0, processedBatches.get());
    }

    @Test
    void answerOneSucessOneError() throws IOException {
        //given
        unAnsweredAcks.put(1, Collections.synchronizedSortedSet(new TreeSet<>(sortByDeliveryTag)));
        unAnsweredAcks.get(1).add(MessageBatch.builder().channel(channel).deliveryTag(1L).success(true).build());
        unAnsweredAcks.get(1).add(MessageBatch.builder().channel(channel).deliveryTag(2L).success(false).build());
        unAnsweredAcks.get(1).add(MessageBatch.builder().channel(channel).deliveryTag(3L).success(true).build());
        unAnsweredAcks.get(1).add(MessageBatch.builder().channel(channel).deliveryTag(4L).success(true).build());
        unAnsweredAcks.get(1).add(MessageBatch.builder().channel(channel).deliveryTag(6L).success(true).build());

        //when
        rabbitMQConsumer.answerAcks();

        //then
        then(channel).should(times(1)).basicAck(1L, true);
        then(channel).should(times(1)).basicNack(2L, true, true);
        then(channel).should(never()).basicAck(3L, true);
        then(channel).should(times(1)).basicAck(4L, true);
        then(channel).should(times(1)).basicAck(6L, true);
        assertTrue(unAnsweredAcks.isEmpty());
    }
    @Test
    void answerOneErrorOneSucces() throws IOException {
        //given
        unAnsweredAcks.put(1, Collections.synchronizedSortedSet(new TreeSet<>(sortByDeliveryTag)));
        unAnsweredAcks.get(1).add(MessageBatch.builder().channel(channel).deliveryTag(1L).success(false).build());
        unAnsweredAcks.get(1).add(MessageBatch.builder().channel(channel).deliveryTag(2L).success(true).build());
        unAnsweredAcks.get(1).add(MessageBatch.builder().channel(channel).deliveryTag(3L).success(false).build());
        unAnsweredAcks.get(1).add(MessageBatch.builder().channel(channel).deliveryTag(4L).success(false).build());
        unAnsweredAcks.get(1).add(MessageBatch.builder().channel(channel).deliveryTag(6L).success(false).build());

        //when
        rabbitMQConsumer.answerAcks();

        //then
        then(channel).should(times(1)).basicNack(1L, true, true);
        then(channel).should(times(1)).basicAck(2L, true);
        then(channel).should(never()).basicNack(3L, true, true);
        then(channel).should(times(1)).basicNack(4L, true, true);
        then(channel).should(times(1)).basicNack(6L, true, true);
        assertTrue(unAnsweredAcks.isEmpty());
    }

    private static class Mocks {
        private Mocks() {}

        private static final BigDecimal PRICE  = new BigDecimal(100);
        private static final String SYMBOL = "BTCUSDT";
        private static final Long TIMESTAMP = 1L;

        private static PriceUpdate priceUpdate(){
            return PriceUpdate.builder().price(PRICE).symbol(SYMBOL).timestamp(TIMESTAMP).build();
        }

        private static MessageBatch messageBatch(Channel channel, long deliveryTag){
            return MessageBatch.builder().priceUpdate(Mocks.priceUpdate()).channel(channel).deliveryTag(deliveryTag).build();
        }

    }
}