use anyhow::Result;
use ctrlc;
use hedge_rs::*;
use log::*;
use std::sync::mpsc::channel;
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
    let mut op = OpBuilder::new()
        .db(args[1].clone())
        .table(args[2].clone())
        .name("hedge-rs".to_string())
        .id(args[3].to_string())
        .lease_ms(3_000)
        .build();

    op.run()?;

    // Wait for Ctrl-C.
    info!("Ctrl-C to exit:");
    rx.recv()?;

    info!("cleaning up...");
    op.close();

    thread::sleep(Duration::from_millis(500));

    Ok(())
}
