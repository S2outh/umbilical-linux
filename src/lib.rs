#![feature(const_trait_impl)]

use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_nats::Client;
use simple_config::Config;
use socketcan::{CanFdFrame, EmbeddedFrame, Frame, Id, StandardId, tokio::CanFdSocket};

use south_common::{
    chell::ChellDefinition,
    definitions::{internal_msgs, telemetry},
    types::Telecommand,
};
use tokio::time;
use tokio_stream::StreamExt;

fn cbor_serializer(value: &dyn erased_serde::Serialize) -> Result<Vec<u8>, erased_serde::Error> {
    let mut buffer = Vec::new();
    let mut serializer = minicbor_serde::Serializer::new(&mut buffer);
    value.erased_serialize(&mut <dyn erased_serde::Serializer>::erase(&mut serializer))?;
    Ok(buffer)
}

type TelecommandChellUnion =
    south_common::chell::fd_compat_chell_union!(internal_msgs::Telecommand);

async fn telecommand_task(nats_client: Arc<Client>, can_sender: CanFdSocket) {
    let mut nats_subscription = nats_client
        .subscribe(internal_msgs::Telecommand.address())
        .await
        .unwrap();

    loop {
        let nats_msg = nats_subscription.next().await.unwrap();
        match minicbor_serde::from_slice::<Telecommand>(&nats_msg.payload) {
            Ok(cmd) => {
                println!("received command");
                let container =
                    TelecommandChellUnion::new(&internal_msgs::Telecommand, &cmd).unwrap();
                let id = Id::Standard(StandardId::new(container.id()).unwrap());
                let frame = CanFdFrame::new(id, container.fd_bytes()).unwrap();
                if let Err(e) = can_sender.write_frame(&frame).await {
                    eprintln!("could not send (can): {}", &e);
                }
            }
            Err(e) => eprintln!("could not decode cmd: {}", &e),
        }
    }
}

async fn telemetry_task(nats_sender: Arc<Client>, can_receiver: CanFdSocket) {
    loop {
        let frame = match can_receiver.read_frame().await {
            Ok(frame) => frame,
            Err(e) => {
                eprintln!("could not read (can): {}, retrying in 1s", &e);
                time::sleep(Duration::from_secs(1)).await;
                continue;
            }
        };

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_millis() as u64;

        if let Ok(def) = telemetry::from_id(frame.raw_id() as u16) {
            if let Ok(values) = def.reserialize(&frame.data(), &timestamp, &cbor_serializer) {
                for serialized_value in values {
                    if let Err(e) = nats_sender
                        .publish(serialized_value.0, serialized_value.1.into())
                        .await
                    {
                        eprintln!("could not send (nats): {}", &e);
                    }
                }
            }
        }
    }
}

async fn open_can_socket(name: &str) -> CanFdSocket {
    loop {
        match CanFdSocket::open(name) {
            Ok(socket) => break socket,
            Err(e) => eprintln!(
                "[ERROR] Could not open CAN socket {}: {}, retrying in 3s",
                name, &e
            ),
        }
        time::sleep(Duration::from_secs(3)).await;
    }
}

async fn open_nats_con(address: &str, user: &str, pwd: &str) -> Client {
    loop {
        match async_nats::ConnectOptions::with_user_and_password(
            String::from(user),
            String::from(pwd),
        )
        .connect(address)
        .await
        {
            Ok(client) => {
                println!(
                    "[NATS] succesfully connected to NATS server on {} with user {}",
                    address, user
                );
                break client;
            }
            Err(e) => eprintln!(
                "[ERROR] Could not connect to NATS server: {:?}, retrying in 3s",
                e
            ),
        }
        time::sleep(Duration::from_secs(3)).await;
    }
}

pub async fn run(config: UMBConfig) {
    let can_tx = open_can_socket(&config.can_socket).await;
    let can_rx = open_can_socket(&config.can_socket).await;

    let nats_client =
        Arc::new(open_nats_con(&config.nats_address, &config.nats_user, &config.nats_pwd).await);

    tokio::spawn(telecommand_task(nats_client.clone(), can_tx));
    tokio::spawn(telemetry_task(nats_client, can_rx));

    std::future::pending::<()>().await;
}

#[derive(Config, Clone)]
pub struct UMBConfig {
    // -- Nats
    pub connect: bool,
    pub nats_address: String,
    pub nats_user: String,
    pub nats_pwd: String,
    // -- CAN
    pub can_socket: String,
}
impl UMBConfig {
    /// Creates a new configuration with default values
    pub fn new() -> Self {
        Self {
            connect: true,
            nats_address: String::from("127.0.0.1"),
            nats_user: String::from("nats"),
            nats_pwd: String::from("nats"),
            can_socket: String::from("can0"),
        }
    }
}
