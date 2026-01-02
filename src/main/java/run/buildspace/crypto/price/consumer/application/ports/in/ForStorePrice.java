package run.buildspace.crypto.price.consumer.application.ports.in;

import run.buildspace.crypto.price.consumer.domain.model.PriceUpdate;

public interface ForStorePrice {
    void addPrice(PriceUpdate priceUpdate);
}
