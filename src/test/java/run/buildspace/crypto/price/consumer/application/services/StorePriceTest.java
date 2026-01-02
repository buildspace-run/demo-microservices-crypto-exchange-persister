package run.buildspace.crypto.price.consumer.application.services;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import run.buildspace.crypto.price.consumer.application.ports.out.PriceEventStore;
import run.buildspace.crypto.price.consumer.domain.model.PriceUpdate;
import uk.co.jemos.podam.api.PodamFactoryImpl;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(MockitoExtension.class)
class StorePriceTest {

    @Mock
    private PriceEventStore priceEventStore;

    @InjectMocks
    private StorePrice storePrice;

    @Test
    void addPrice() {
        //given
        PriceUpdate priceUpdate = new PodamFactoryImpl().manufacturePojo(PriceUpdate.class);

        //when
        storePrice.addPrice(priceUpdate);

        //then
        assertNotNull(priceUpdate);
    }



}