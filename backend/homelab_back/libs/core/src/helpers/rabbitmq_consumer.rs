use crate::helpers::event_handler::EventHandler;
use crate::helpers::rabbitmq_topology::{dead_letter_queue, declare_service_topology};
use lapin::options::BasicNackOptions;
use lapin::{
    options::{
        BasicAckOptions, BasicConsumeOptions, ExchangeDeclareOptions, QueueBindOptions,
        QueueDeclareOptions,
    },
    types::FieldTable,
    Connection, ConnectionProperties, ExchangeKind, Result,
};
use std::fmt::format;
use std::sync::Arc;
use tonic::codegen::tokio_stream::StreamExt;

pub struct RabbitMqConsumer;

impl RabbitMqConsumer {
    pub async fn start(
        connection_addr: &str,
        handler: Arc<dyn EventHandler>,
        listen_patterns: Vec<&str>,
        service_name: &str,
    ) -> Result<()> {
        let conn = Connection::connect(connection_addr, ConnectionProperties::default()).await?;
        let channel = conn.create_channel().await?;

        let queue_name = declare_service_topology(&channel, service_name, &listen_patterns).await?;
        let consumer_tag = format!("{}_consumer", service_name);

        let mut consumer = channel
            .basic_consume(
                &queue_name,
                "nas_generic_consumer",
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await?;

        println!("🚀 Generic Consumer started!");

        while let Some(delivery) = consumer.next().await {
            let delivery = delivery?;
            let routing_key = delivery.routing_key.as_str();

            match handler.handle(&routing_key, &delivery.data).await {
                Ok(_) => {
                    delivery.ack(BasicAckOptions::default()).await?;
                }
                Err(e) => {
                    eprintln!(
                        "❌ {}: {} failed ({}) — parking in {}",
                        service_name,
                        routing_key,
                        e,
                        dead_letter_queue(service_name)
                    );
                    delivery
                        .nack(BasicNackOptions {
                            requeue: false,
                            multiple: false,
                        })
                        .await?;
                }
            }
        }

        Ok(())
    }
}
