use lapin::{Channel, ExchangeKind, Result};
use lapin::options::{ExchangeDeclareOptions, QueueBindOptions, QueueDeclareOptions};
use lapin::types::{AMQPValue, FieldTable, LongString, ShortString};

pub const EVENT_EXCHANGE: &str = "homelab_events";

pub fn events_queue(service: &str) -> String {
    format!("{}.events", service)
}

pub fn dead_letter_queue(service: &str) -> String {
    format!("{}.dlq", service)
}

pub fn dead_letter_exchange(service: &str) -> String {
    format!("{}.dlx", service)
}

pub async fn declare_events_exchange (channel: &Channel) -> Result<()> {
    channel
        .exchange_declare(
            EVENT_EXCHANGE,
            ExchangeKind::Topic,
            ExchangeDeclareOptions {durable: true, ..Default::default()},
            FieldTable::default(),
        )
        .await
}

pub async fn declare_service_topology (
    channel: &Channel,
    service: &str,
    binding: &[&str]
) -> Result<String> {

    declare_events_exchange(channel).await?;

    let dlx = dead_letter_exchange(service);
    let dlq = dead_letter_queue(service);
    let queue = events_queue(service);

    channel
        .exchange_declare(
            &dlx,
            ExchangeKind::Fanout,
            ExchangeDeclareOptions {durable: true, ..Default::default()},
            FieldTable::default(),
        )
        .await?;

    channel
        .queue_declare(
            &dlq,
            QueueDeclareOptions {durable: true, ..Default::default()},
            FieldTable::default()
        )
        .await?;

    channel
        .queue_bind(&dlq, &dlx, "", QueueBindOptions::default(), FieldTable::default())
        .await?;

    let mut queue_args = FieldTable::default();
    queue_args.insert(
        ShortString::from("x-dead-letter-exchange"),
        AMQPValue::LongString(LongString::from(dlx.as_str()))
    );

    channel
        .queue_declare(
            &queue,
            QueueDeclareOptions {durable: true, ..Default::default()},
            queue_args
        )
        .await?;

    for pattern in binding {
        channel
            .queue_bind(
                &queue,
                EVENT_EXCHANGE,
                pattern,
                QueueBindOptions::default(),
                FieldTable::default(),
            )
            .await?;

        println!("🐰 {}: bound {} to {}", service, queue, pattern);
    }

    Ok(queue)
}