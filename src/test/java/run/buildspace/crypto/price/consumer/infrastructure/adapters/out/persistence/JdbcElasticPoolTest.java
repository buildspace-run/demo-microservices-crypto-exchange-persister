package run.buildspace.crypto.price.consumer.infrastructure.adapters.out.persistence;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.util.ReflectionTestUtils;
import run.buildspace.crypto.price.consumer.domain.model.PriceUpdate;
import uk.co.jemos.podam.api.PodamFactoryImpl;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.BDDMockito.given;
import static run.buildspace.crypto.price.consumer.infrastructure.adapters.out.persistence.JdbcElasticPool.INSERT_SQL;

@ExtendWith(MockitoExtension.class)
@Execution(ExecutionMode.SAME_THREAD)
class JdbcElasticPoolTest {

    @InjectMocks
    private JdbcElasticPool service;

    @Mock
    private NamedParameterJdbcTemplate namedParameterJdbcTemplate;


    private Map<String, JdbcElasticPool.PriceBatchWorker> pendingSymbols;
    private Map<JdbcElasticPool.PriceBatchWorker, Integer> assignedSymbols;
    private List<JdbcElasticPool.PriceBatchWorker> pool = new CopyOnWriteArrayList<>();

    @BeforeEach
    void setUp() {
        pendingSymbols = (Map<String, JdbcElasticPool.PriceBatchWorker>) ReflectionTestUtils.getField(service, "pendingSymbols");
        assignedSymbols = (Map<JdbcElasticPool.PriceBatchWorker, Integer>) ReflectionTestUtils.getField(service, "assignedSymbols");
        pool = (List<JdbcElasticPool.PriceBatchWorker>) ReflectionTestUtils.getField(service, "pool");

    }

    @AfterEach
    void tearDown() {
        pool.forEach(JdbcElasticPool.PriceBatchWorker::stop);
    }

    @Test
    void shouldAssignSymbolToLeastLoadedWorkerTest() {
        // given
        JdbcElasticPool.PriceBatchWorker idleWorker = new JdbcElasticPool.PriceBatchWorker(namedParameterJdbcTemplate);
        JdbcElasticPool.PriceBatchWorker busyWorker = new JdbcElasticPool.PriceBatchWorker(namedParameterJdbcTemplate);

        assignedSymbols.put(idleWorker, 0);
        assignedSymbols.put(busyWorker, 1000);

        // when
        service.save(new PriceUpdate("BTC", new BigDecimal(6000), System.currentTimeMillis()));

        // then
        assertEquals(idleWorker, pendingSymbols.get("BTC"));
        assertEquals(1, assignedSymbols.get(idleWorker));
    }

    @Test
    void requireMoreWorkersTest() {
        // given

        ReflectionTestUtils.setField(service, "messageCounter", new AtomicInteger(100));
        for (int i = 0; i < 100; i++) {
            pendingSymbols.put(Mocks.currency().symbol(), pool.get(0));
        }


        //when
        service.managePerformance();

        //then
        assertEquals(2, pool.size());
        assertEquals(0, ReflectionTestUtils.getField(service, "lowLoadTicks"));
        assertEquals(100, pendingSymbols.size());
        assertEquals(50, new ArrayList<>(assignedSymbols.values()).get(0));
        assertEquals(50, new ArrayList<>(assignedSymbols.values()).get(1));


    }

    @Test
    void shouldIncrementLowLoadTicksWithoutReducingPoolTest() {
        // given
        ReflectionTestUtils.setField(service, "lowLoadTicks", 2);
        pool.add(new JdbcElasticPool.PriceBatchWorker(namedParameterJdbcTemplate));
        pool.add(new JdbcElasticPool.PriceBatchWorker(namedParameterJdbcTemplate));
        //when
        service.managePerformance();

        //then
        assertEquals(3, pool.size());
        assertEquals(3, ReflectionTestUtils.getField(service, "lowLoadTicks"));

    }

    @Test
    void reduceWorkersTest() {
        // given
        ReflectionTestUtils.setField(service, "lowLoadTicks", 5);
        pool.add(new JdbcElasticPool.PriceBatchWorker(namedParameterJdbcTemplate));
        pool.add(new JdbcElasticPool.PriceBatchWorker(namedParameterJdbcTemplate));
        //when
        service.managePerformance();

        //then
        assertEquals(1, pool.size());

    }

    @Test
    void stopAllWorkersTest() {
        // given
        pool.add(new JdbcElasticPool.PriceBatchWorker(namedParameterJdbcTemplate));
        pool.add(new JdbcElasticPool.PriceBatchWorker(namedParameterJdbcTemplate));
        //when
        service.destroy();

        //then
        assertEquals(0, pool.size());
    }


    @Test
    void flushQueueTest() {
        // tested class
        JdbcElasticPool.PriceBatchWorker worker = new JdbcElasticPool.PriceBatchWorker(namedParameterJdbcTemplate);

        // Mocks
        List<PriceUpdate> currencies = IntStream.range(0, 60).mapToObj(i -> Mocks.currency()).toList();
        currencies.forEach(worker::enqueue);

        // Argument captors
        ArgumentCaptor<String> queryCator = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Map<String, ?>[]> batchCaptor = ArgumentCaptor.forClass(Map[].class);

        // given
        given(namedParameterJdbcTemplate.batchUpdate(queryCator.capture(), batchCaptor.capture())).willReturn(new int[0]);
        final ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1);
        executor.schedule(worker::stop, 500, TimeUnit.MILLISECONDS);
        //when
        worker.run();

        //then
        assertEquals(INSERT_SQL, queryCator.getValue());
        Map<String, Object>[] batch = (Map<String, Object>[]) batchCaptor.getValue();
        assertEquals(60, batch.length);
        for (int i = 0; i < 60; i++) {
            assertEquals(currencies.get(i).symbol(), batch[i].get("symbol"));
            assertEquals(currencies.get(i).price(), batch[i].get("price"));
            assertEquals(currencies.get(i).timestamp(), batch[i].get("ts"));
        }
    }

    @Test
    void shouldHandleConcurrentSaves() {
        // given
        int totalMessages = 1000;
        AtomicInteger messageCounter = (AtomicInteger) ReflectionTestUtils.getField(service, "messageCounter");

        // when -
        IntStream.range(0, totalMessages)
                .parallel()
                .forEach(i -> service.save(Mocks.currency()));

        // then
        assertEquals(totalMessages, messageCounter.get());


        int totalAssigned = assignedSymbols.values().stream()
                .mapToInt(Integer::intValue)
                .sum();
        assertEquals(totalMessages, totalAssigned);
    }

    private static class Mocks {
        private Mocks() {
        }

        private static PriceUpdate currency() {
            return new PodamFactoryImpl().manufacturePojo(PriceUpdate.class);
        }
    }
}
