use anyhow::{Result, anyhow};
use crossbeam_channel::{Receiver, Sender, unbounded};
use exp_backoff::BackoffBuilder;
use google_cloud_spanner::client::Client;
use google_cloud_spanner::client::ClientConfig;
use google_cloud_spanner::statement::Statement;
use google_cloud_spanner::value::CommitTimestamp;
use log::*;
use spindle_rs::*;
use std::fmt::Write as _;
use std::io::{BufReader, prelude::*};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use time::OffsetDateTime;
use tokio::runtime::Runtime;
use uuid::Uuid;

mod protocol;

#[macro_use(defer)]
extern crate scopeguard;

extern crate num_cpus;

#[derive(Debug)]
struct DiffToken {
    diff: i64,
    token: i128,
}

#[derive(Debug)]
struct Record {
    name: String,
    heartbeat: i128,
    token: i128,
    writer: String,
}

#[derive(Debug)]
enum ProtoCtrl {
    Exit,
    Dummy(Sender<bool>),
    InitialLock(Sender<i128>),
    NextLockInsert { name: String, tx: Sender<i128> },
    NextLockUpdate { token: i128, tx: Sender<i128> },
    CheckLock(Sender<DiffToken>),
    CurrentToken(Sender<Record>),
    Heartbeat(Sender<i128>),
}

pub struct Op {
    db: String,
    table: String,
    name: String,
    id: String,
    lock: Vec<Arc<Mutex<Lock>>>,
    leader: Arc<AtomicUsize>,
    duration_ms: u64,
    active: Arc<AtomicUsize>,
    tx_ctrl: Vec<Sender<ProtoCtrl>>,
}

impl Op {
    /// Allows for discovery of the builder.
    pub fn builder() -> OpBuilder {
        OpBuilder::default()
    }

    pub fn run(&mut self) -> Result<()> {
        let mut lock_name = String::new();
        write!(&mut lock_name, "hedge/spindle/{}", self.name.clone()).unwrap();
        let (tx_ldr, rx_ldr) = mpsc::channel();
        self.lock = vec![Arc::new(Mutex::new(
            LockBuilder::new()
                .db(self.db.clone())
                .table(self.table.clone())
                .name(lock_name)
                .id(self.id.clone())
                .duration_ms(3000)
                .leader_tx(Some(tx_ldr))
                .build(),
        ))];

        {
            let lc = self.lock[0].clone();
            if let Ok(mut v) = lc.lock() {
                v.run()?;
            }
        }

        let leader = self.leader.clone();
        thread::spawn(move || {
            loop {
                let ldr = rx_ldr.recv();
                match ldr {
                    Ok(v) => leader.store(v, Ordering::Relaxed),
                    Err(_) => {}
                }
            }
        });

        let (tx, rx): (Sender<TcpStream>, Receiver<TcpStream>) = unbounded();
        let recvs = Arc::new(Mutex::new(Vec::new()));
        let cpus = num_cpus::get();

        for _ in 0..cpus {
            let recv = recvs.clone();

            {
                let mut rv = recv.lock().unwrap();
                rv.push(rx.clone());
            }
        }

        // Initialize our workers for our TCP server.
        for i in 0..cpus {
            let recv = recvs.clone();
            let leader = self.leader.clone();
            thread::spawn(move || {
                loop {
                    let rx = match recv.lock() {
                        Ok(v) => v,
                        Err(e) => {
                            error!("t{i}: lock failed: {e}");
                            continue;
                        }
                    };

                    let conn = rx[i].recv().unwrap();
                    drop(rx); // no need to wait

                    let start = Instant::now();

                    defer! {
                        info!("[t{i}] took {:?}", start.elapsed());
                    }

                    protocol::handle_protocol(i, conn, leader.load(Ordering::Acquire));
                }
            });
        }

        // Start our internal TCP server.
        let tx_tcp = tx.clone();
        let host = self.id.clone();
        thread::spawn(move || {
            info!("starting internal TCP server");
            let listen = TcpListener::bind(host).unwrap();
            for stream in listen.incoming() {
                let stream = match stream {
                    Ok(v) => v,
                    Err(e) => {
                        error!("stream failed: {e}");
                        continue;
                    }
                };

                tx_tcp.send(stream).unwrap();
            }
        });

        // Test our internal TCP server.
        let host_test = self.id.clone();
        thread::spawn(move || {
            let hp: Vec<&str> = host_test.split(":").collect();
            for i in 0..cpus {
                thread::sleep(Duration::from_secs(2));
                let mut host = String::new();
                write!(&mut host, "127.0.0.1:{}", hp[1]).unwrap();
                let mut stream = match TcpStream::connect(host) {
                    Ok(v) => v,
                    Err(e) => {
                        error!("[{i}]: connect failed: {e}");
                        continue;
                    }
                };

                let mut send = String::new();
                if i == 4 {
                    write!(&mut send, "{}\n", protocol::LDR).unwrap();
                } else {
                    write!(&mut send, "hello_{i}\n").unwrap();
                }
                if let Ok(_) = stream.write_all(send.as_bytes()) {
                    let mut reader = BufReader::new(&stream);
                    let mut data = String::new();
                    reader.read_line(&mut data).unwrap();
                    info!("[{i}]: response: {data:?}");
                }
            }
        });

        // Finally, set the system active.
        let active = self.active.clone();
        active.store(1, Ordering::Relaxed);

        Ok(())
    }

    pub fn close(&mut self) {
        let lc = self.lock[0].clone();
        if let Ok(mut v) = lc.lock() {
            v.close();
        }
    }
}

/// `LockBuilder` builds an instance of Lock with default values.
#[derive(Default)]
pub struct OpBuilder {
    db: String,
    table: String,
    name: String,
    id: String,
    duration_ms: u64,
}

impl OpBuilder {
    pub fn new() -> OpBuilder {
        OpBuilder::default()
    }

    pub fn db(mut self, db: String) -> OpBuilder {
        self.db = db;
        self
    }

    pub fn table(mut self, table: String) -> OpBuilder {
        self.table = table;
        self
    }

    pub fn name(mut self, name: String) -> OpBuilder {
        self.name = name;
        self
    }

    pub fn id(mut self, id: String) -> OpBuilder {
        self.id = id;
        self
    }

    pub fn duration_ms(mut self, ms: u64) -> OpBuilder {
        self.duration_ms = ms;
        self
    }

    pub fn build(self) -> Op {
        Op {
            db: self.db,
            table: self.table,
            name: self.name,
            id: if self.id != "" {
                self.id
            } else {
                let id = Uuid::new_v4();
                id.to_string()
            },
            lock: vec![],
            leader: Arc::new(AtomicUsize::new(0)),
            duration_ms: self.duration_ms,
            active: Arc::new(AtomicUsize::new(0)),
            tx_ctrl: vec![],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_run() {}
}
