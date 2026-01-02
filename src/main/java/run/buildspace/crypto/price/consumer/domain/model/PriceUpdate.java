package run.buildspace.crypto.price.consumer.domain.model;


import run.buildspace.crypto.price.consumer.domain.exception.InvalidPriceException;
import run.buildspace.crypto.price.consumer.domain.exception.InvalidSymbolException;
import lombok.Builder;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.math.BigDecimal;

@Builder
public record PriceUpdate(String symbol, BigDecimal price, Long timestamp) implements Serializable {
    public PriceUpdate {
        if (StringUtils.isBlank(symbol)) {
            throw new InvalidSymbolException("Symbol cannot be empty");
        }
        if (timestamp == null) {
            timestamp = System.currentTimeMillis();
        }
        validate("Price", price);

    }

    private void validate(String field, Object value) {
        if (value == null) {
            throw new InvalidPriceException(field + " must be informed");
        }
    }

}
