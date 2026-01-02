#!/bin/bash
# RabbitMQ initialization script
# This script creates the exchange, queues, and bindings needed for the crypto price system

RABBITMQ_HOST="${RABBITMQ_HOST:-localhost}"
RABBITMQ_PORT="${RABBITMQ_PORT:-15672}"
RABBITMQ_USER="${RABBITMQ_USER:-myuser}"
RABBITMQ_PASSWORD="${RABBITMQ_PASSWORD:-secret}"

EXCHANGE="cryptocurrencies"
QUEUE_PRICE_UPDATE="currency-update"
QUEUE_SUBSCRIPTION="currency.subscription"
QUEUE_ERROR="currency-update-error"
QUEUE_DLQ="dead-letter-queue"
DLX="dead-letter-exchange"

echo "Waiting for RabbitMQ to be ready..."
until curl -s -u ${RABBITMQ_USER}:${RABBITMQ_PASSWORD} http://${RABBITMQ_HOST}:${RABBITMQ_PORT}/api/overview > /dev/null 2>&1; do
  sleep 2
  echo "RabbitMQ not ready yet..."
done

echo "RabbitMQ is ready! Starting configuration..."

# Create Dead Letter Exchange
echo "Creating Dead Letter Exchange: ${DLX}"
curl -i -u ${RABBITMQ_USER}:${RABBITMQ_PASSWORD} \
  -H "Content-Type: application/json" \
  -X PUT \
  -d '{"type":"topic","durable":true}' \
  http://${RABBITMQ_HOST}:${RABBITMQ_PORT}/api/exchanges/%2F/${DLX}

# Create Dead Letter Queue
echo "Creating Dead Letter Queue: ${QUEUE_DLQ}"
curl -i -u ${RABBITMQ_USER}:${RABBITMQ_PASSWORD} \
  -H "Content-Type: application/json" \
  -X PUT \
  -d '{"durable":true}' \
  http://${RABBITMQ_HOST}:${RABBITMQ_PORT}/api/queues/%2F/${QUEUE_DLQ}

# Bind DLQ to DLX
echo "Binding ${QUEUE_DLQ} to ${DLX}"
curl -i -u ${RABBITMQ_USER}:${RABBITMQ_PASSWORD} \
  -H "Content-Type: application/json" \
  -X POST \
  -d '{"routing_key":"#"}' \
  http://${RABBITMQ_HOST}:${RABBITMQ_PORT}/api/bindings/%2F/e/${DLX}/q/${QUEUE_DLQ}

# Create main exchange
echo "Creating main exchange: ${EXCHANGE}"
curl -i -u ${RABBITMQ_USER}:${RABBITMQ_PASSWORD} \
  -H "Content-Type: application/json" \
  -X PUT \
  -d '{"type":"topic","durable":true}' \
  http://${RABBITMQ_HOST}:${RABBITMQ_PORT}/api/exchanges/%2F/${EXCHANGE}

# Create currency-update queue with DLX
echo "Creating queue: ${QUEUE_PRICE_UPDATE}"
curl -i -u ${RABBITMQ_USER}:${RABBITMQ_PASSWORD} \
  -H "Content-Type: application/json" \
  -X PUT \
  -d "{\"durable\":true,\"arguments\":{\"x-dead-letter-exchange\":\"${DLX}\"}}" \
  http://${RABBITMQ_HOST}:${RABBITMQ_PORT}/api/queues/%2F/${QUEUE_PRICE_UPDATE}

# Bind currency-update queue
echo "Binding ${QUEUE_PRICE_UPDATE} to ${EXCHANGE} with routing key '#.price.update'"
curl -i -u ${RABBITMQ_USER}:${RABBITMQ_PASSWORD} \
  -H "Content-Type: application/json" \
  -X POST \
  -d '{"routing_key":"#.price.update"}' \
  http://${RABBITMQ_HOST}:${RABBITMQ_PORT}/api/bindings/%2F/e/${EXCHANGE}/q/${QUEUE_PRICE_UPDATE}

# Create subscription queue with DLX
echo "Creating queue: ${QUEUE_SUBSCRIPTION}"
curl -i -u ${RABBITMQ_USER}:${RABBITMQ_PASSWORD} \
  -H "Content-Type: application/json" \
  -X PUT \
  -d "{\"durable\":true,\"arguments\":{\"x-dead-letter-exchange\":\"${DLX}\"}}" \
  http://${RABBITMQ_HOST}:${RABBITMQ_PORT}/api/queues/%2F/${QUEUE_SUBSCRIPTION}

# Bind subscription queue
echo "Binding ${QUEUE_SUBSCRIPTION} to ${EXCHANGE} with routing key 'currency.subscription.#'"
curl -i -u ${RABBITMQ_USER}:${RABBITMQ_PASSWORD} \
  -H "Content-Type: application/json" \
  -X POST \
  -d '{"routing_key":"currency.subscription.#"}' \
  http://${RABBITMQ_HOST}:${RABBITMQ_PORT}/api/bindings/%2F/e/${EXCHANGE}/q/${QUEUE_SUBSCRIPTION}

# Create error queue with DLX
echo "Creating queue: ${QUEUE_ERROR}"
curl -i -u ${RABBITMQ_USER}:${RABBITMQ_PASSWORD} \
  -H "Content-Type: application/json" \
  -X PUT \
  -d "{\"durable\":true,\"arguments\":{\"x-dead-letter-exchange\":\"${DLX}\"}}" \
  http://${RABBITMQ_HOST}:${RABBITMQ_PORT}/api/queues/%2F/${QUEUE_ERROR}

# Bind error queue
echo "Binding ${QUEUE_ERROR} to ${EXCHANGE} with routing key '#.error'"
curl -i -u ${RABBITMQ_USER}:${RABBITMQ_PASSWORD} \
  -H "Content-Type: application/json" \
  -X POST \
  -d '{"routing_key":"#.error"}' \
  http://${RABBITMQ_HOST}:${RABBITMQ_PORT}/api/bindings/%2F/e/${EXCHANGE}/q/${QUEUE_ERROR}

echo ""
echo "✅ RabbitMQ configuration complete!"
echo ""
echo "Created:"
echo "  - Exchange: ${EXCHANGE} (topic)"
echo "  - Exchange: ${DLX} (topic, for dead letters)"
echo "  - Queue: ${QUEUE_PRICE_UPDATE} → #.price.update"
echo "  - Queue: ${QUEUE_SUBSCRIPTION} → currency.subscription.#"
echo "  - Queue: ${QUEUE_ERROR} → #.error"
echo "  - Queue: ${QUEUE_DLQ} (dead letter queue)"
echo ""
echo "Access RabbitMQ Management UI at: http://${RABBITMQ_HOST}:${RABBITMQ_PORT}"
echo "Username: ${RABBITMQ_USER}"
echo "Password: ${RABBITMQ_PASSWORD}"
