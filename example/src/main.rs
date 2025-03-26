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

    let (tx_leader, rx_leader): (Sender<LeaderChannel>, Receiver<LeaderChannel>) = channel();

    let mut op = OpBuilder::new()
        .db(args[1].clone())
        .table(args[2].clone())
        .name("hedge-rs".to_string())
        .id(args[3].to_string())
        .lease_ms(3_000)
        .toleader(Some(tx_leader))
        .build();

    op.run()?;

    thread::spawn(move || {
        loop {
            match rx_leader.recv().unwrap() {
                LeaderChannel::ToLeader { msg, tx } => {
                    info!("[ToLeader] received: {:?}", String::from_utf8(msg));
                    let mut reply = String::new();
                    write!(&mut reply, "hello from {}", args[3].to_string()).unwrap();
                    tx.send(reply.as_bytes().to_vec()).unwrap();
                }
            }
        }
    });

    thread::sleep(Duration::from_millis(5000));
    match op.send("hello".as_bytes().to_vec()) {
        Ok(v) => info!("reply from leader: {}", String::from_utf8(v).unwrap()),
        Err(e) => error!("send failed: {e}"),
    }

    // Wait for Ctrl-C.
    info!("Ctrl-C to exit:");
    rx.recv()?;

    info!("cleaning up...");
    op.close();

    thread::sleep(Duration::from_millis(500));

    Ok(())
}
