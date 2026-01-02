package run.buildspace.crypto.price.consumer;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import run.buildspace.crypto.price.consumer.domain.model.PriceUpdate;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers
@ActiveProfiles("test")
@Execution(ExecutionMode.CONCURRENT)
class AppITTest {

    private static final String GET_PRICES = "SELECT SYMBOL, PRICE, TS FROM PRICE_HISTORY WHERE SYMBOL = ?";
    private static final String COUNT_PRICES = "SELECT COUNT(*) FROM PRICE_HISTORY WHERE SYMBOL = ?";


    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:15-alpine").withDatabaseName("test").withUsername("test").withPassword("test");

    @Container
    static final RabbitMQContainer RABBITMQ = new RabbitMQContainer("rabbitmq:3-management-alpine");


    private final RabbitTemplate rabbitTemplate;

    private final JdbcTemplate jdbcTemplate;

    private final RabbitAdmin rabbitAdmin;

    @Value("${rabbitmq.exchange}")
    private String exchangeName;

    @Value("${rabbitmq.prices-queue}")
    private String pricesQueue;

    @Value("${rabbitmq.currency-update-routing-bind}")
    private String currencyUpdateRoutingBind;


    @Autowired
    public AppITTest(RabbitTemplate rabbitTemplate, JdbcTemplate jdbcTemplate, RabbitAdmin rabbitAdmin) {
        this.rabbitTemplate = rabbitTemplate;
        this.jdbcTemplate = jdbcTemplate;
        this.rabbitAdmin = rabbitAdmin;
    }


    @DynamicPropertySource
    static void configureDynamicProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("spring.rabbitmq.host", RABBITMQ::getHost);
        registry.add("spring.rabbitmq.port", RABBITMQ::getAmqpPort);

    }

    @Test
    void loadApplicationContextTest() {
        assertNotNull(rabbitTemplate, "RabbitTemplate should be initialized");
        assertNotNull(jdbcTemplate, "JdbcTemplate should be initialized");
        assertEquals(1, jdbcTemplate.queryForObject("SELECT 1", Integer.class), "Database should be accessible");
    }

    @Test
    void receiveTwoMessagesTest() {
        //given

        rabbitTemplate.convertAndSend(exchangeName, currencyUpdateRoutingBind.replace("#", Mocks.BITCOIN), PriceUpdate.builder().symbol(Mocks.BITCOIN).price(new BigDecimal(100)).timestamp(System.currentTimeMillis()).build());
        rabbitTemplate.convertAndSend(exchangeName, currencyUpdateRoutingBind.replace("#", Mocks.BITCOIN), PriceUpdate.builder().symbol(Mocks.BITCOIN).price(new BigDecimal(101)).timestamp(System.currentTimeMillis()+1).build());

        // then
        Awaitility.await().atMost(5, SECONDS).pollInterval(300, TimeUnit.MILLISECONDS).untilAsserted(() -> {
            List<Map<String, Object>> rows = jdbcTemplate.queryForList(GET_PRICES, Mocks.BITCOIN);
            assertEquals(2, rows.size(), Mocks.BITCOIN + " subscription should be persisted");
            assertEquals(Mocks.BITCOIN, rows.get(0).get("SYMBOL"));
            assertEquals(Mocks.BITCOIN, rows.get(1).get("SYMBOL"));
            assertEquals(0, new BigDecimal(100).compareTo((BigDecimal) rows.get(0).get("PRICE")));
            assertEquals(0, new BigDecimal(101).compareTo((BigDecimal) rows.get(1).get("PRICE")));
        });

        Properties queueProperties = rabbitAdmin.getQueueProperties(pricesQueue);
        assertNotNull(queueProperties, "Queue should exist");

        int messageCount = (Integer) queueProperties.get(RabbitAdmin.QUEUE_MESSAGE_COUNT);
        assertEquals(0, messageCount, "Queue should be empty (all messages processed and ACKed)");

    }

    @Test
    void receivedBurstOfMessagesTest() {
        //given
        AtomicLong baseTimestamp = new AtomicLong(System.currentTimeMillis());
        IntStream.range(0, 300).forEach(i -> {
            rabbitTemplate.convertAndSend(exchangeName, currencyUpdateRoutingBind.replace("#", Mocks.ETHEREUM), PriceUpdate.builder().symbol(Mocks.ETHEREUM).price(new BigDecimal(100)).timestamp(baseTimestamp.getAndIncrement()).build());
            rabbitTemplate.convertAndSend(exchangeName, currencyUpdateRoutingBind.replace("#", Mocks.LITECOIN), PriceUpdate.builder().symbol(Mocks.LITECOIN).price(new BigDecimal(100)).timestamp(baseTimestamp.get()).build());
        });

        // then
        Awaitility.await().atMost(10, SECONDS).pollInterval(500, TimeUnit.MILLISECONDS).untilAsserted(() -> {
            assertEquals(300, jdbcTemplate.queryForObject(COUNT_PRICES, Integer.class, Mocks.ETHEREUM), Mocks.ETHEREUM + " subscription should be persisted");
            assertEquals(300, jdbcTemplate.queryForObject(COUNT_PRICES, Integer.class, Mocks.LITECOIN), Mocks.LITECOIN + " subscription should be persisted");
        });

        Properties queueProperties = rabbitAdmin.getQueueProperties(pricesQueue);
        assertNotNull(queueProperties, "Queue should exist");

        int messageCount = (Integer) queueProperties.get(RabbitAdmin.QUEUE_MESSAGE_COUNT);
        assertEquals(0, messageCount, "Queue should be empty (all messages processed and ACKed)");
    }


    @TestConfiguration
    static class RabbitConfiguration {
        @Value("${rabbitmq.exchange}")
        private String exchangeName;

        @Value("${rabbitmq.prices-queue}")
        private String pricesQueue;

        @Value("${rabbitmq.currency-update-routing-bind}")
        private String currencyUpdateRoutingBind;

        @Bean
        TopicExchange exchange() {
            return new TopicExchange(exchangeName);
        }

        @Bean
        Queue queueCurrencyUpdate() {
            return new Queue(pricesQueue, true);
        }

        @Bean
        Binding bindingCurrencyUpdate(Queue queueCurrencyUpdate, TopicExchange exchange) {
            return BindingBuilder.bind(queueCurrencyUpdate).to(exchange).with(currencyUpdateRoutingBind);
        }

        @Bean
        public MessageConverter jsonMessageConverter() {
            return new Jackson2JsonMessageConverter();
        }
    }

    private static class Mocks {
        private Mocks() {
        }

        private static final String BITCOIN = "BTC";
        private static final String ETHEREUM = "ETH";
        private static final String LITECOIN = "LTC";

    }
}