package run.buildspace.crypto.price.consumer.infrastructure.adapters.in.messaging;

import com.rabbitmq.client.Channel;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import run.buildspace.crypto.price.consumer.domain.model.PriceUpdate;

@Getter
@Accessors(fluent = true)
@Setter
@Builder
class MessageBatch{

    private PriceUpdate priceUpdate;
    private Channel channel;
    private long deliveryTag;
    private Boolean success;

}