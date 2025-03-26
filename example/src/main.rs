use anyhow::Result;
use ctrlc;
use hedge_rs::*;
use log::*;
use std::fmt::Write as _;
use std::sync::mpsc::{Receiver, Sender, channel};
use std::time::Duration;
use std::{env, thread};

fn main() -> Result<()> {
    env_logger::init();
    let args: Vec<String> = env::args().collect();

    if args.len() < 4 {
        error!("provide the db, table, and id args");
        return Ok(());
    }

    let (tx, rx) = channel();
    ctrlc::set_handler(move || tx.send(()).unwrap())?;

    // We will use this channel for the 'send' and 'broadcast' features.
    // Use Sender as inputs, then we read replies through the Receiver.
    let (tx_comms, rx_comms): (Sender<Comms>, Receiver<Comms>) = channel();

    let mut op = OpBuilder::new()
        .db(args[1].clone())
        .table(args[2].clone())
        .name("hedge-rs".to_string())
        .id(args[3].to_string())
        .lease_ms(3_000)
        .tx_toleader(Some(tx_comms.clone()))
        .tx_broadcast(Some(tx_comms.clone()))
        .build();

    op.run()?;

    thread::spawn(move || {
        loop {
            match rx_comms.recv() {
                Ok(v) => match v {
                    // This is our 'send' handler. When a node is the leader, this handles
                    // all messages coming from other nodes using the send() API.
                    Comms::ToLeader { msg, tx } => {
                        info!("[send()] received: {}", String::from_utf8(msg).unwrap());

                        // Send our reply back using 'tx'. args[3] here is our id.
                        let mut reply = String::new();
                        write!(&mut reply, "hello from {}", args[3].to_string()).unwrap();
                        tx.send(reply.as_bytes().to_vec()).unwrap();
                    }
                    // This is our 'broadcast' handler. When a node broadcasts a message,
                    // through the broadcast() API, we reply here..
                    Comms::Broadcast { msg, tx } => {
                        info!("[broadcast()] received: {}", String::from_utf8(msg).unwrap());

                        // Send our reply back using 'tx'. args[3] here is our id.
                        let mut reply = String::new();
                        write!(&mut reply, "hello from {}", args[3].to_string()).unwrap();
                        tx.send(reply.as_bytes().to_vec()).unwrap();
                    }
                },
                Err(e) => {
                    error!("{e}");
                    continue;
                }
            }
        }
    });

    thread::sleep(Duration::from_millis(5000));

    // An example of sending message to the leader.
    match op.send("hello".as_bytes().to_vec()) {
        Ok(v) => info!("reply from leader: {}", String::from_utf8(v).unwrap()),
        Err(e) => error!("send failed: {e}"),
    }

    thread::sleep(Duration::from_millis(2000));

    // An example of broadcasting message to all nodes.
    let (tx_reply, rx_reply): (Sender<Broadcast>, Receiver<Broadcast>) = channel();
    op.broadcast("hello".as_bytes().to_vec(), tx_reply)?;

    // Read through all the replies from all nodes. An empty
    // id or message marks the end of the streaming reply.
    loop {
        match rx_reply.recv()? {
            Broadcast::ReplyStream { id, msg, error } => {
                if id == "" || msg.len() == 0 {
                    break;
                }

                if error {
                    error!("{:?}", String::from_utf8(msg)?);
                } else {
                    info!("{:?}", String::from_utf8(msg)?);
                }
            }
        }
    }

    // Wait for Ctrl-C.
    info!("Ctrl-C to exit:");
    rx.recv()?;

    op.close();
    thread::sleep(Duration::from_millis(500));

    Ok(())
}
