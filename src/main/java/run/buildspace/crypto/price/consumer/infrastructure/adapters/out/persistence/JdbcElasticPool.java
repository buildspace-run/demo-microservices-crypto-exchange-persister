package run.buildspace.crypto.price.consumer.infrastructure.adapters.out.persistence;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import run.buildspace.crypto.price.consumer.application.ports.out.PriceEventStore;
import run.buildspace.crypto.price.consumer.domain.model.PriceUpdate;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Component
public class JdbcElasticPool implements PriceEventStore {


    private final Logger logger = LoggerFactory.getLogger(JdbcElasticPool.class);

    private static final int TARGET_LOAD_PER_WORKER = 60;
    private static final int COOLDOWN_SECONDS = 5;
    static final String INSERT_SQL = "INSERT INTO price_history (symbol, price, ts) VALUES (UPPER(:symbol), :price, :ts) ON CONFLICT DO NOTHING";

    private final NamedParameterJdbcTemplate jdbcTemplate;
    private final Map<String, PriceBatchWorker> pendingSymbols = new ConcurrentHashMap<>();
    private final Map<PriceBatchWorker, Integer> assignedSymbols = new ConcurrentHashMap<>();
    private final List<PriceBatchWorker> pool = new CopyOnWriteArrayList<>();
    private final AtomicInteger messageCounter = new AtomicInteger(0);

    private int lowLoadTicks = 0;

    @Autowired
    public JdbcElasticPool(NamedParameterJdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
        adjustWorkerPool(1);
    }

    @Override
    public void save(PriceUpdate pendingePriceUpdate) {
        messageCounter.incrementAndGet();

        PriceBatchWorker worker = pendingSymbols.computeIfAbsent(pendingePriceUpdate.symbol(), s -> assignedSymbols.entrySet().stream().min(Comparator.comparingInt(Map.Entry::getValue)).map(Map.Entry::getKey).orElse(pool.get(0)));
        assignedSymbols.merge(worker, 1, Integer::sum);
        worker.enqueue(pendingePriceUpdate);
    }

    @Scheduled(fixedRate = 1000)
    protected void managePerformance() {
        int currentLoad = messageCounter.getAndSet(0);
        int neededWorkers = (int) Math.ceil((double) currentLoad / TARGET_LOAD_PER_WORKER);
        neededWorkers = Math.max(1, neededWorkers);

        if (neededWorkers > pool.size()) {
            lowLoadTicks = 0;
            rebalance(neededWorkers);
        } else if (neededWorkers < pool.size()) {
            lowLoadTicks++;
            if (lowLoadTicks >= COOLDOWN_SECONDS) {
                rebalance(neededWorkers);
                lowLoadTicks = 0;
            }
        }
    }

    private synchronized void rebalance(int targetCount) {
        logger.info("Rebalance: {} -> {}", pool.size(), targetCount);
        List<String> allSymbols = new ArrayList<>(pendingSymbols.keySet());
        adjustWorkerPool(targetCount);
        // Reparto equitativo de símbolos

        if (allSymbols.isEmpty()) return;

        for (int i = 0; i < allSymbols.size(); i++) {
            String symbol = allSymbols.get(i);
            PriceBatchWorker targetWorker = pool.get(i % targetCount);
            pendingSymbols.put(symbol, targetWorker);
            assignedSymbols.merge(targetWorker, 1, Integer::sum);
        }
    }

    private void adjustWorkerPool(int targetCount) {
        pendingSymbols.clear();
        assignedSymbols.clear();

        // Añadir si faltan
        while (pool.size() < targetCount) {
            PriceBatchWorker worker = new PriceBatchWorker(jdbcTemplate);
            pool.add(worker);
            new Thread(worker, "PriceWorker-" + pool.size()).start();
        }
        // Marcar para borrar si sobran (el worker se cerrará solo al ver running=false)
        while (pool.size() > targetCount) {
            PriceBatchWorker worker = pool.remove(pool.size() - 1);
            worker.stop();
        }
    }

    @Override
    public void destroy() {
        pool.forEach(PriceBatchWorker::stop);
        pool.clear();
    }

    static class PriceBatchWorker implements Runnable {
        private final NamedParameterJdbcTemplate namedParameterJdbcTemplate;


        private final Logger logger = LoggerFactory.getLogger(PriceBatchWorker.class);
        private final BlockingQueue<PriceUpdate> queue = new LinkedBlockingQueue<>();
        private final List<PriceUpdate> buffer = new ArrayList<>();
        private volatile boolean running = true;

        public PriceBatchWorker(NamedParameterJdbcTemplate namedParameterJdbcTemplate) {
            this.namedParameterJdbcTemplate = namedParameterJdbcTemplate;
        }

        public void enqueue(PriceUpdate p) {
            boolean insertedInQueue = queue.offer(p);
            logger.debug("Message inserted in queue: {}", insertedInQueue);
        }

        public void stop() {
            this.running = false;
        }

        @Override
        public void run() {
            while (running || !queue.isEmpty()) {
                try {
                    PriceUpdate price = queue.poll(1, TimeUnit.SECONDS);
                    if (price != null) buffer.add(price);

                    if (buffer.size() >= TARGET_LOAD_PER_WORKER || (price == null && !buffer.isEmpty())) {
                        flush();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    logger.error("Worker error: {}", e.getMessage());
                }
            }
            flush();
            logger.info("{} stopped.", Thread.currentThread().getName());
        }

        private void flush() {
            if (!buffer.isEmpty()) {
                List<Map<String, Object>> batch = new ArrayList<>();
                for (PriceUpdate pendingPriceUpdate : buffer) {
                    batch.add(Map.of("symbol", pendingPriceUpdate.symbol(), "price", pendingPriceUpdate.price(), "ts", pendingPriceUpdate.timestamp()));
                }
                namedParameterJdbcTemplate.batchUpdate(INSERT_SQL, batch.toArray(new Map[0]));

                buffer.clear();
                // Meter transactionsTemplate
            }
        }
    }
}
