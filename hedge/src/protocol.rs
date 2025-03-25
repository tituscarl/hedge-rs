use crate::LeaderChannel;
use log::*;
use std::collections::HashMap;
use std::fmt::Write as _;
use std::io::{BufReader, prelude::*};
use std::net::TcpStream;
use std::sync::{Arc, Mutex, mpsc};

pub const CMD_CLDR: &str = "#"; // for leader confirmation, reply="+<1|0>"
pub const CMD_PING: &str = "*"; // heartbeat to indicate availability, fmt="+[id]"
pub const CMD_SEND: &str = "$"; // member to leader, fmt="$<base64(payload)>"

// Replies starts with either '+' or '-'; '+' = success, '-' = error.
pub fn handle_protocol(
    id: usize,
    mut stream: TcpStream,
    leader: usize,
    members: Arc<Mutex<HashMap<String, usize>>>,
    toleader: Vec<mpsc::Sender<LeaderChannel>>,
) {
    let mut reader = BufReader::new(&stream);
    let mut data = String::new();
    reader.read_line(&mut data).unwrap();

    info!("[T{id}]: request: {data:?}");

    // Confirm if we are leader. Reply with +1 if so, otherwise, +0.
    if data.starts_with(CMD_CLDR) {
        let mut ack = String::new();
        if leader > 0 {
            write!(&mut ack, "+1\n").unwrap();
        } else {
            write!(&mut ack, "+0\n").unwrap();
        }

        if let Err(e) = stream.write_all(ack.as_bytes()) {
            error!("[T{id}]: write_all failed: {e}");
        }

        return;
    }

    // Heartbeat. If the payload is "*<name>\n", sender is non-leader, and leader will
    // reply with "+{comma-separated-list-of-members}". If the payload is "*\n", sender
    // is leader, for liveness check, and we reply +1.
    if data.starts_with(CMD_PING) {
        if data.len() == 2 {
            let mut ack = String::new();
            write!(&mut ack, "+1\n").unwrap();

            if let Err(e) = stream.write_all(ack.as_bytes()) {
                error!("[T{id}]: write_all failed: {e}");
            }

            return;
        }

        {
            if let Ok(mut v) = members.lock() {
                let name = &data[1..&data.len() - 1];
                v.insert(name.to_string(), 0);
            }
        }

        let mut all = String::new();
        let mut ack = String::new();

        {
            if let Ok(v) = members.lock() {
                for (k, _) in &*v {
                    write!(&mut all, "{},", k).unwrap();
                }
            }
        }

        all.pop(); // rm last ','
        write!(&mut ack, "+{}\n", all).unwrap();
        if let Err(e) = stream.write_all(ack.as_bytes()) {
            error!("[T{id}]: write_all failed: {e}");
        }

        return;
    }

    // TODO: docs
    if data.starts_with(CMD_SEND) {
        let (tx, rx): (mpsc::Sender<Vec<u8>>, mpsc::Receiver<Vec<u8>>) = mpsc::channel();
        if let Err(e) = toleader[0].send(LeaderChannel::ToLeader {
            msg: "zz".as_bytes().to_vec(),
            tx,
        }) {
            error!("send failed: {e}");
        } else {
            let rep = rx.recv().unwrap();
            info!(">>>>> reply from main: {:?}", String::from_utf8(rep));
        }

        let mut ack = String::new();
        if leader > 0 {
            write!(&mut ack, "+todo:actual-send\n").unwrap();
        } else {
            write!(&mut ack, "+sorry-not-leader\n").unwrap();
        }

        if let Err(e) = stream.write_all(ack.as_bytes()) {
            error!("[T{id}]: write_all failed: {e}");
        }

        return;
    }

    if let Err(e) = stream.write_all(b"-unknown\n") {
        error!("[T{id}]: write_all failed: {e}");
    }
}
