use anyhow::Result;
use ctrlc;
use hedge_rs::*;
use log::*;
use std::{
    env,
    fmt::Write as _,
    io::{BufReader, prelude::*},
    net::TcpListener,
    sync::{
        Arc, Mutex,
        mpsc::{Receiver, Sender, channel},
    },
    thread,
};

fn main() -> Result<()> {
    env_logger::init();
    let args: Vec<String> = env::args().collect();

    if args.len() < 5 {
        error!("provide the db, table, id, and test host:port args");
        return Ok(());
    }

    let (tx, rx) = channel();
    ctrlc::set_handler(move || tx.send(()).unwrap())?;

    // We will use this channel for the 'send' and 'broadcast' features.
    // Use Sender as inputs, then we read replies through the Receiver.
    let (tx_comms, rx_comms): (Sender<Comms>, Receiver<Comms>) = channel();

    let op = Arc::new(Mutex::new(
        OpBuilder::new()
            .db(args[1].clone())
            .table(args[2].clone())
            .name("hedge-rs".to_string())
            .id(args[3].to_string())
            .lease_ms(3_000)
            .tx_toleader(Some(tx_comms.clone()))
            .tx_broadcast(Some(tx_comms.clone()))
            .build(),
    ));

    {
        op.lock().unwrap().run()?;
    }

    // Start a new thread that will serve as handlers for both send() and broadcast() APIs.
    let id_handler = args[3].clone();
    thread::spawn(move || {
        loop {
            match rx_comms.recv() {
                Ok(v) => match v {
                    // This is our 'send' handler. When a node is the leader, this handles
                    // all messages coming from other nodes using the send() API.
                    Comms::ToLeader { msg, tx } => {
                        let msg_s = String::from_utf8(msg).unwrap();
                        info!("[send()] received: {msg_s}");

                        // Send our reply back using 'tx'.
                        let mut reply = String::new();
                        write!(&mut reply, "echo '{msg_s}' from leader:{}", id_handler.to_string()).unwrap();
                        tx.send(reply.as_bytes().to_vec()).unwrap();
                    }
                    // This is our 'broadcast' handler. When a node broadcasts a message,
                    // through the broadcast() API, we reply here..
                    Comms::Broadcast { msg, tx } => {
                        let msg_s = String::from_utf8(msg).unwrap();
                        info!("[broadcast()] received: {msg_s}");

                        // Send our reply back using 'tx'.
                        let mut reply = String::new();
                        write!(&mut reply, "echo '{msg_s}' from {}", id_handler.to_string()).unwrap();
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

    // Starts a new thread for our test TCP server.
    let op_tcp = op.clone();
    let host_port = args[4].clone();
    thread::spawn(move || {
        let listen = TcpListener::bind(host_port.to_string()).unwrap();
        for stream in listen.incoming() {
            match stream {
                Err(_) => break,
                Ok(v) => {
                    let mut reader = BufReader::new(&v);
                    let mut msg = String::new();
                    reader.read_line(&mut msg).unwrap();

                    if msg.starts_with("q") {
                        break;
                    }

                    if msg.starts_with("send") {
                        let send = msg[..msg.len() - 1].to_string();
                        match op_tcp.lock().unwrap().send(send.as_bytes().to_vec()) {
                            Ok(v) => info!("reply from leader: {}", String::from_utf8(v).unwrap()),
                            Err(e) => error!("send failed: {e}"),
                        }

                        continue;
                    }

                    if msg.starts_with("broadcast") {
                        let (tx_reply, rx_reply): (Sender<Broadcast>, Receiver<Broadcast>) = channel();
                        let send = msg[..msg.len() - 1].to_string();
                        op_tcp
                            .lock()
                            .unwrap()
                            .broadcast(send.as_bytes().to_vec(), tx_reply)
                            .unwrap();

                        // Read through all the replies from all nodes. An empty
                        // id or message marks the end of the streaming reply.
                        loop {
                            match rx_reply.recv().unwrap() {
                                Broadcast::ReplyStream { id, msg, error } => {
                                    if id == "" || msg.len() == 0 {
                                        break;
                                    }

                                    if error {
                                        error!("{:?}", String::from_utf8(msg).unwrap());
                                    } else {
                                        info!("{:?}", String::from_utf8(msg).unwrap());
                                    }
                                }
                            }
                        }

                        continue;
                    }

                    info!("{msg:?} not supported");
                }
            };
        }
    });

    rx.recv()?; // wait for Ctrl-C
    op.lock().unwrap().close();

    Ok(())
}
