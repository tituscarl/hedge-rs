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
use std::io::{BufReader, Error, ErrorKind, prelude::*};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use time::OffsetDateTime;
use tokio::runtime::Runtime;
use uuid::Uuid;

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
    duration_ms: u64,
    active: Arc<AtomicUsize>,
    token: Arc<AtomicU64>,
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

        let mut lock = LockBuilder::new()
            .db(self.db.clone())
            .table(self.table.clone())
            .name(lock_name)
            .duration_ms(3000)
            .callback(Some(|v| info!("callback: leader={v}")))
            .build();

        lock.run()?;

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

        for i in 0..cpus {
            let recv = recvs.clone();
            thread::spawn(move || {
                loop {
                    let rx = match recv.lock() {
                        Ok(v) => v,
                        Err(e) => {
                            error!("t{i}: lock failed: {e}");
                            continue;
                        }
                    };

                    let mut conn = rx[i].recv().unwrap();
                    drop(rx); // no need to wait

                    let start = Instant::now();

                    defer! {
                        info!("[t{i}] took {:?}", start.elapsed());
                    }

                    let mut reader = BufReader::new(&conn);
                    let mut data = String::new();
                    reader.read_line(&mut data).unwrap();

                    info!("[t{i}]: request: {data:?}");

                    if let Err(e) = conn.write_all(b"reply\n") {
                        error!("[t{i}]: write_all failed: {e}");
                    }
                }
            });
        }

        // Start our internal TCP server.
        let tx_tcp = tx.clone();
        thread::spawn(move || {
            info!("starting internal TCP server");
            let listen = TcpListener::bind("0.0.0.0:8080").unwrap();
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

        thread::sleep(Duration::from_secs(1));
        info!("start send");

        // Test our internal TCP server.
        thread::spawn(move || {
            for i in 0..cpus {
                let mut stream = match TcpStream::connect("127.0.0.1:8080") {
                    Ok(v) => v,
                    Err(e) => {
                        error!("[{i}]: connect failed: {e}");
                        return;
                    }
                };

                let mut send = String::new();
                write!(&mut send, "hello_{i}\n").unwrap();
                if let Ok(_) = stream.write_all(send.as_bytes()) {
                    let mut reader = BufReader::new(&stream);
                    let mut data = String::new();
                    reader.read_line(&mut data).unwrap();
                    info!("[{i}]: response: {data:?}");
                }
            }
        });

        Ok(())
    }

    pub fn close(&mut self) {}
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
            duration_ms: self.duration_ms,
            active: Arc::new(AtomicUsize::new(0)),
            token: Arc::new(AtomicU64::new(0)),
            tx_ctrl: vec![],
        }
    }
}

// This will be running on a separate thread. Caller thread will be requesting Spanner async calls
// here through ProtoCtrl commands using channels for exchanging information. This is an easier
// approach for allowing our main threads to have access to async function calls. Here, a single
// tokio runtime is being used to block on these async calls.
fn spanner_caller(
    db: String,
    table: String,
    name: String,
    id: String,
    rx_ctrl: Receiver<ProtoCtrl>,
    tx_ok: Sender<Result<()>>,
) {
    let rt = Runtime::new().unwrap();
    let (tx, rx): (Sender<Option<Client>>, Receiver<Option<Client>>) = unbounded();
    rt.block_on(async {
        let config = ClientConfig::default().with_auth().await;
        match config {
            Err(_) => tx.send(None).unwrap(),
            Ok(v) => {
                let client = Client::new(db, v).await;
                match client {
                    Err(_) => tx.send(None).unwrap(),
                    Ok(v) => tx.send(Some(v)).unwrap(),
                }
            }
        }
    });

    let read = rx.recv().unwrap();
    if read.is_none() {
        tx_ok.send(Err(anyhow!("client failed"))).unwrap();
        return;
    }

    let client = read.unwrap(); // shouldn't panic
    tx_ok.send(Ok(())).unwrap(); // inform main we're okay

    for code in rx_ctrl {
        match code {
            ProtoCtrl::Exit => {
                rt.block_on(async { client.close().await });
                return;
            }
            ProtoCtrl::Dummy(tx) => {
                tx.send(true).unwrap();
            }
            ProtoCtrl::InitialLock(tx) => {
                let start = Instant::now();
                let (tx_in, rx_in): (Sender<i128>, Receiver<i128>) = unbounded();
                rt.block_on(async {
                    let mut q = String::new();
                    write!(&mut q, "insert {} ", table).unwrap();
                    write!(&mut q, "(name, heartbeat, token, writer) ").unwrap();
                    write!(&mut q, "values (").unwrap();
                    write!(&mut q, "'{}',", name).unwrap();
                    write!(&mut q, "PENDING_COMMIT_TIMESTAMP(),").unwrap();
                    write!(&mut q, "PENDING_COMMIT_TIMESTAMP(),").unwrap();
                    write!(&mut q, "'{}')", id).unwrap();
                    let stmt = Statement::new(q);
                    let rwt = client.begin_read_write_transaction().await;
                    if rwt.is_err() {
                        tx.send(-1).unwrap();
                        return;
                    }

                    let mut t = rwt.unwrap();
                    let res = t.update(stmt).await;
                    let res = t.end(res, None).await;
                    match res {
                        Ok(s) => {
                            let ts = s.0.unwrap();
                            let dt = OffsetDateTime::from_unix_timestamp(ts.seconds)
                                .unwrap()
                                .replace_nanosecond(ts.nanos as u32)
                                .unwrap();
                            debug!("InitialLock commit timestamp: {dt}");
                            tx_in.send(dt.unix_timestamp_nanos()).unwrap();
                        }
                        Err(e) => {
                            error!("InitialLock DML failed: {e}");
                            tx_in.send(-1).unwrap();
                        }
                    };
                });

                if let Err(e) = tx.send(rx_in.recv().unwrap()) {
                    error!("InitialLock reply failed: {e}")
                }

                debug!("InitialLock took {:?}", start.elapsed());
            }
            ProtoCtrl::NextLockInsert { name, tx } => {
                let start = Instant::now();
                let (tx_in, rx_in): (Sender<i128>, Receiver<i128>) = unbounded();
                rt.block_on(async {
                    let mut q = String::new();
                    write!(&mut q, "insert {} ", table).unwrap();
                    write!(&mut q, "(name) ").unwrap();
                    write!(&mut q, "values ('{}')", name).unwrap();
                    let stmt = Statement::new(q);
                    let rwt = client.begin_read_write_transaction().await;
                    if rwt.is_err() {
                        tx.send(-1).unwrap();
                        return;
                    }

                    let mut t = rwt.unwrap();
                    let res = t.update(stmt).await;
                    let res = t.end(res, None).await;
                    match res {
                        Ok(s) => {
                            let ts = s.0.unwrap();
                            let dt = OffsetDateTime::from_unix_timestamp(ts.seconds)
                                .unwrap()
                                .replace_nanosecond(ts.nanos as u32)
                                .unwrap();
                            tx_in.send(dt.unix_timestamp_nanos()).unwrap();
                        }
                        Err(e) => {
                            error!("NextLockInsert DML failed: {e}");
                            tx_in.send(-1).unwrap();
                        }
                    };
                });

                if let Err(e) = tx.send(rx_in.recv().unwrap()) {
                    error!("NextLockInsert reply failed: {e}")
                }

                debug!("NextLockInsert took {:?}", start.elapsed());
            }
            ProtoCtrl::NextLockUpdate { token, tx } => {
                let start = Instant::now();
                let (tx_in, rx_in): (Sender<i128>, Receiver<i128>) = unbounded();
                rt.block_on(async {
                    let mut q = String::new();
                    write!(&mut q, "update {} set ", table).unwrap();
                    write!(&mut q, "heartbeat = PENDING_COMMIT_TIMESTAMP(), ").unwrap();
                    write!(&mut q, "token = @token, ").unwrap();
                    write!(&mut q, "writer = @writer ").unwrap();
                    write!(&mut q, "where name = @name").unwrap();
                    let mut stmt1 = Statement::new(q);
                    let odt = OffsetDateTime::from_unix_timestamp_nanos(token).unwrap();
                    stmt1.add_param("token", &odt);
                    stmt1.add_param("writer", &id);
                    stmt1.add_param("name", &name);
                    let rwt = client.begin_read_write_transaction().await;
                    if rwt.is_err() {
                        tx.send(-1).unwrap();
                        return;
                    }

                    let mut t = rwt.unwrap();
                    let res = t.update(stmt1).await;

                    // Best-effort cleanup.
                    let mut q = String::new();
                    write!(&mut q, "delete from {} ", table).unwrap();
                    write!(&mut q, "where starts_with(name, '{}_')", name).unwrap();
                    let stmt2 = Statement::new(q);
                    let _ = t.update(stmt2).await;

                    let res = t.end(res, None).await;
                    match res {
                        Ok(s) => {
                            let ts = s.0.unwrap();
                            let dt = OffsetDateTime::from_unix_timestamp(ts.seconds)
                                .unwrap()
                                .replace_nanosecond(ts.nanos as u32)
                                .unwrap();
                            tx_in.send(dt.unix_timestamp_nanos()).unwrap();
                        }
                        Err(e) => {
                            error!("NextLockUpdate DML failed: {e}");
                            tx_in.send(-1).unwrap();
                        }
                    };
                });

                if let Err(e) = tx.send(rx_in.recv().unwrap()) {
                    error!("NextLockUpdate reply failed: {e}")
                }

                debug!("NextLockUpdate took {:?}", start.elapsed());
            }
            ProtoCtrl::CheckLock(tx) => {
                let start = Instant::now();
                let (tx_in, rx_in): (Sender<DiffToken>, Receiver<DiffToken>) = unbounded();
                rt.block_on(async {
                    let mut q = String::new();
                    write!(&mut q, "select ",).unwrap();
                    write!(&mut q, "timestamp_diff(current_timestamp(), ",).unwrap();
                    write!(&mut q, "heartbeat, millisecond) as diff, ",).unwrap();
                    write!(&mut q, "token from {} ", table).unwrap();
                    write!(&mut q, "where name = @name").unwrap();
                    let mut stmt = Statement::new(q);
                    stmt.add_param("name", &name);
                    let mut tx = client.single().await.unwrap();
                    let mut iter = tx.query(stmt).await.unwrap();
                    let mut empty = true;
                    while let Some(row) = iter.next().await.unwrap() {
                        let d = row.column_by_name::<i64>("diff").unwrap();
                        let t = row.column_by_name::<CommitTimestamp>("token").unwrap();
                        tx_in
                            .send(DiffToken {
                                diff: d,
                                token: t.unix_timestamp_nanos(),
                            })
                            .unwrap();

                        empty = false;
                        break; // ensure single line
                    }

                    if empty {
                        tx_in.send(DiffToken { diff: 0, token: -1 }).unwrap();
                    }
                });

                if let Err(e) = tx.send(rx_in.recv().unwrap()) {
                    error!("CheckLock reply failed: {e}")
                }

                debug!("CheckLock took {:?}", start.elapsed());
            }
            ProtoCtrl::CurrentToken(tx) => {
                let start = Instant::now();
                let (tx_in, rx_in): (Sender<Record>, Receiver<Record>) = unbounded();
                rt.block_on(async {
                    let mut q = String::new();
                    write!(&mut q, "select token, writer from {} ", table).unwrap();
                    write!(&mut q, "where name = @name").unwrap();
                    let mut stmt = Statement::new(q);
                    stmt.add_param("name", &name);
                    let mut tx = client.single().await.unwrap();
                    let mut iter = tx.query(stmt).await.unwrap();
                    let mut empty = true;
                    while let Some(row) = iter.next().await.unwrap() {
                        let t = row.column_by_name::<CommitTimestamp>("token").unwrap();
                        let w = row.column_by_name::<String>("writer").unwrap();
                        tx_in
                            .send(Record {
                                name: String::from(""),
                                heartbeat: 0,
                                token: t.unix_timestamp_nanos(),
                                writer: w,
                            })
                            .unwrap();

                        empty = false;
                        break; // ensure single line
                    }

                    if empty {
                        tx_in
                            .send(Record {
                                name: String::from(""),
                                heartbeat: -1,
                                token: -1,
                                writer: String::from(""),
                            })
                            .unwrap();
                    }
                });

                if let Err(e) = tx.send(rx_in.recv().unwrap()) {
                    error!("CurrentToken reply failed: {e}")
                }

                debug!("CurrentToken took {:?}", start.elapsed());
            }
            ProtoCtrl::Heartbeat(tx) => {
                let start = Instant::now();
                let (tx_in, rx_in): (Sender<i128>, Receiver<i128>) = unbounded();
                rt.block_on(async {
                    let mut q = String::new();
                    write!(&mut q, "update {} ", table).unwrap();
                    write!(&mut q, "set heartbeat = PENDING_COMMIT_TIMESTAMP() ").unwrap();
                    write!(&mut q, "where name = @name").unwrap();
                    let mut stmt = Statement::new(q);
                    stmt.add_param("name", &name);
                    let rwt = client.begin_read_write_transaction().await;
                    if rwt.is_err() {
                        tx.send(-1).unwrap();
                        return;
                    }

                    let mut t = rwt.unwrap();
                    let res = t.update(stmt).await;
                    let res = t.end(res, None).await;
                    match res {
                        Ok(s) => {
                            let ts = s.0.unwrap();
                            let dt = OffsetDateTime::from_unix_timestamp(ts.seconds)
                                .unwrap()
                                .replace_nanosecond(ts.nanos as u32)
                                .unwrap();
                            debug!("Heartbeat commit timestamp: {dt}");
                            tx_in.send(dt.unix_timestamp_nanos()).unwrap();
                        }
                        Err(_) => tx_in.send(-1).unwrap(),
                    };
                });

                if let Err(e) = tx.send(rx_in.recv().unwrap()) {
                    error!("Heartbeat reply failed: {e}")
                }

                debug!("Heartbeat took {:?}", start.elapsed());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_run() {}
}
