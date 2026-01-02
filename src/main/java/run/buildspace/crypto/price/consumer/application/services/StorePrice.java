package run.buildspace.crypto.price.consumer.application.services;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import run.buildspace.crypto.price.consumer.application.ports.in.ForStorePrice;
import run.buildspace.crypto.price.consumer.application.ports.out.PriceEventStore;
import run.buildspace.crypto.price.consumer.domain.model.PriceUpdate;

@Service
public class StorePrice implements ForStorePrice {

    private final PriceEventStore priceEventStore;

    @Autowired
    public StorePrice(PriceEventStore priceEventStore) {
        this.priceEventStore = priceEventStore;
    }

    @Override
    public void addPrice(PriceUpdate pendingPriceUpdate) {
        priceEventStore.save(pendingPriceUpdate);
    }
}
