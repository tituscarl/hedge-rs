use crate::Comms;
use base64ct::{Base64, Encoding};
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
    toleader: Vec<mpsc::Sender<Comms>>,
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

        let _ = stream.write_all(ack.as_bytes());
        return;
    }

    // Heartbeat. If the payload is "*<name>\n", sender is non-leader, and leader will
    // reply with "+{comma-separated-list-of-members}". If the payload is "*\n", sender
    // is leader, for liveness check, and we reply +1.
    if data.starts_with(CMD_PING) {
        if data.len() == 2 {
            let mut ack = String::new();
            write!(&mut ack, "+1\n").unwrap();
            let _ = stream.write_all(ack.as_bytes());
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
        let _ = stream.write_all(ack.as_bytes());
        return;
    }

    // TODO: docs
    if data.starts_with(CMD_SEND) {
        if leader == 0 {
            let _ = stream.write_all("-not leader\n".as_bytes());
            return;
        }

        if toleader.len() == 0 {
            let _ = stream.write_all("-send disabled\n".as_bytes());
            return;
        }

        let decoded = match Base64::decode_vec(&data[1..&data.len() - 1]) {
            Ok(v) => v,
            Err(e) => {
                let mut err = String::new();
                write!(&mut err, "-{e}\n").unwrap();
                let _ = stream.write_all(err.as_bytes());
                return;
            }
        };

        let (tx, rx): (mpsc::Sender<Vec<u8>>, mpsc::Receiver<Vec<u8>>) = mpsc::channel();
        if let Err(e) = toleader[0].send(Comms::ToLeader { msg: decoded, tx }) {
            let mut err = String::new();
            write!(&mut err, "-{e}\n").unwrap();
            let _ = stream.write_all(err.as_bytes());
            return;
        }

        let mut rep = rx.recv().unwrap();
        let mut ack = vec![b'+'];
        ack.append(&mut rep);
        ack.push(b'\n');
        let _ = stream.write_all(&ack);
        return;
    }

    let _ = stream.write_all(b"-unknown\n");
}
