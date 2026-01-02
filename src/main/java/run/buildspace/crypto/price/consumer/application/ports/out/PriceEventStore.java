package run.buildspace.crypto.price.consumer.application.ports.out;


import run.buildspace.crypto.price.consumer.domain.model.PriceUpdate;

public interface PriceEventStore {
    void save(PriceUpdate priceUpdate);
    void destroy();

}
