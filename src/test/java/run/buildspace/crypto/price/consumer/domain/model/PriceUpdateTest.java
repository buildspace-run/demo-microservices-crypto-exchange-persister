package run.buildspace.crypto.price.consumer.domain.model;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import run.buildspace.crypto.price.consumer.domain.exception.InvalidPriceException;
import run.buildspace.crypto.price.consumer.domain.exception.InvalidSymbolException;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Execution(ExecutionMode.CONCURRENT)
class PriceUpdateTest {


    @Test
    void currencyCreationTest() {
        PriceUpdate priceUpdate = Mocks.priceUpdate();
        assertEquals(Mocks.SYMBOL, priceUpdate.symbol());
        assertEquals(Mocks.PRICE, priceUpdate.price());
    }

    @Test
    void currencyCreationInvalidSymbolTest() {
        InvalidSymbolException exception = assertThrows(InvalidSymbolException.class, Mocks::wrongSymbolPriceUpdate);
        assertEquals("Symbol cannot be empty", exception.getMessage());
    }

    @Test
    void currencyCreationInvalidPriceTest() {
        InvalidPriceException exception = assertThrows(InvalidPriceException.class, Mocks::wrongPriceUpdate);
        assertEquals("Price must be informed", exception.getMessage());
    }

    private static class Mocks {
        private Mocks() {
        }
        static final String SYMBOL = "BTC";
        static final BigDecimal PRICE = BigDecimal.valueOf(12345.67);

        static PriceUpdate priceUpdate() {
            return PriceUpdate.builder()
                    .symbol(SYMBOL)
                    .price(PRICE)
                    .timestamp(System.currentTimeMillis())
                    .build();
        }

        static void wrongSymbolPriceUpdate() {
            PriceUpdate.builder()
                    .price(PRICE)
                    .timestamp(System.currentTimeMillis())
                    .build();
        }

        static void wrongPriceUpdate() {
            PriceUpdate.builder()
                    .symbol(SYMBOL)
                    .build();
        }
    }
}
