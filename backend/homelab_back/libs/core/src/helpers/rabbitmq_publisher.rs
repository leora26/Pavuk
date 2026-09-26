use lapin::{BasicProperties, Channel, Connection, ConnectionProperties};
use lapin::options::{BasicPublishOptions, ConfirmSelectOptions};
use lapin::publisher_confirm::Confirmation;
use serde::Serialize;
use thiserror::Error;
use crate::events::DomainEvent;
use crate::helpers::rabbitmq_topology::{declare_events_exchange, EVENT_EXCHANGE};

#[derive(Debug, Error)]
pub enum PublishError {
    #[error("amqp error: {0}")]
    Amqp(#[from] lapin::Error),
    #[error("failed to serialise event: {0}")]
    Serialisation(#[from] serde_json::Error),
    #[error("broker nacked {routing_key}")]
    Nacked { routing_key: String },
    #[error("{routing_key} is unroutable: no queue is bound for it ({reply_text})")]
    Unroutable { routing_key: String, reply_text: String },
    #[error("channel is not in confirm mode — confirm_select was not called")]
    ConfirmsDisabled,
}

#[derive(Clone)]
pub struct RabbitMqPublisher {
    channel: Channel,
}
const DELIVERY_MODE_PERSISTENT: u8 = 2;
impl RabbitMqPublisher {
    pub async fn new(connection_str: &str) -> lapin::Result<Self> {
        let connection =
            Connection::connect(connection_str, ConnectionProperties::default()).await?;

        let channel = connection.create_channel().await?;

        declare_events_exchange(&channel).await?;

        channel.confirm_select(ConfirmSelectOptions::default()).await?;

        Ok(Self { channel })
    }

    pub async fn publish<T> (&self, event: &T) -> Result<(), PublishError>
    where
        T: DomainEvent + Serialize,
    {
        let payload = serde_json::to_vec(event)?;
        let routing_key = event.routing_key();

        let confirmation = self
            .channel
            .basic_publish(
                EVENT_EXCHANGE,
                routing_key,
                BasicPublishOptions { mandatory: true, ..Default::default() },
                &payload,
                BasicProperties::default().with_delivery_mode(DELIVERY_MODE_PERSISTENT),
            )
            .await?
            .await?;

        match confirmation {
            Confirmation::Ack(None) => Ok(()),
            Confirmation::Ack(Some(returned)) => Err(PublishError::Unroutable {
                routing_key: returned.delivery.routing_key.to_string(),
                reply_text: returned.reply_text.to_string(),
            }),
            Confirmation::Nack(_) => Err(PublishError::Nacked {
                routing_key: event.routing_key().to_string(),
            }),
            Confirmation::NotRequested => Err(PublishError::ConfirmsDisabled),
        }
    }
}
