#![feature(const_trait_impl)]

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_nats::{Client, Subscriber};
use simple_config::Config;
use socketcan::{CanAnyFrame, CanFdFrame, EmbeddedFrame, Frame, Id, frame::AsPtr, tokio::CanFdSocket};

use south_common::{chell::{_internal::InternalChellDefinition, ChellDefinition, ChellValue}, definitions::{internal_msgs, telemetry}, types::Telecommand};
use tokio::time;
use tokio_stream::StreamExt;


fn cbor_serializer(
    value: &dyn erased_serde::Serialize,
) -> Result<Vec<u8>, erased_serde::Error> {
    let mut buffer = Vec::new();
    let mut serializer = minicbor_serde::Serializer::new(&mut buffer);
    value.erased_serialize(&mut <dyn erased_serde::Serializer>::erase(&mut serializer))?;
    Ok(buffer)
}

async fn can_sender(mut nats_subscription: Subscriber, can_sender: CanFdSocket) {

    loop {
        let nats_msg = nats_subscription.next().await.unwrap();
        match minicbor_serde::from_slice::<Telecommand>(&nats_msg.payload) {
            Ok(cmd) => {
                let mut buf = [0u8; internal_msgs::Telecommand::MAX_BYTE_SIZE];
                let len = cmd.write(&mut buf).unwrap();
                let frame = CanFdFrame::from_raw_id(internal_msgs::Telecommand.id() as u32, &buf[..len]).unwrap();
                if let Err(e) = can_sender.write_frame(&frame).await {
                    eprintln!("could not send (can): {}", &e);
                }
            },
            Err(e) => eprintln!("could not decode cmd: {}", &e),
        }
    }
}

async fn can_receiver(nats_sender: Client, can_receiver: CanFdSocket) {
    loop {
        let frame = can_receiver.read_frame().await.unwrap();
        let CanAnyFrame::Fd(frame) = frame else {
            continue;
        };
        let Id::Standard(id) = frame.id() else {
            continue;
        };

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_millis() as u64;

        if let Ok(def) = telemetry::from_id(id.as_raw()) {
            if let Ok(values) = def.reserialize(
                &frame.as_bytes(),
                &timestamp,
                &cbor_serializer,
            ) {
                for serialized_value in values {
                    if let Err(e) = nats_sender.publish(serialized_value.0, serialized_value.1.into()).await {
                        eprintln!("could not send (nats): {}", &e);
                    }
                }
            }
        }
    }
}

pub async fn run(config: UMBConfig) {

    let can_tx = CanFdSocket::open("vcan0").unwrap();
    let can_rx = CanFdSocket::open("vcan0").unwrap();

    let nats_client = loop {
        match async_nats::ConnectOptions::with_user_and_password(config.nats_user.clone(), config.nats_pwd.clone())
            .connect(config.nats_address.clone())
            .await {

            Ok(client) => {
                println!("[NATS] succesfully connected to NATS server on {} with user {}", config.nats_address, config.nats_user);
                break client;
            },
            Err(e) => eprintln!("[ERROR] Could not connect to NATS server: {:?}, retrying in 3s", e),
        }
        time::sleep(Duration::from_secs(3)).await;
    };

    let nats_subscription = nats_client.subscribe(internal_msgs::Telecommand.address())
        .await.unwrap();

    tokio::spawn(can_sender(nats_subscription, can_tx));
    tokio::spawn(can_receiver(nats_client, can_rx));

    std::future::pending::<()>().await;
}

#[derive(Config, Clone)]
pub struct UMBConfig {
    // -- Nats
    pub connect: bool,
    pub nats_address: String,
    pub nats_user: String,
    pub nats_pwd: String,
}
impl UMBConfig {
    /// Creates a new configuration with default values
    pub fn new() -> Self {
        Self {
            connect: true,
            nats_address: String::from("127.0.0.1"),
            nats_user: String::from("nats"),
            nats_pwd: String::from("nats"),
        }
    }
}

