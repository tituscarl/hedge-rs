use log::*;
use std::collections::HashMap;
use std::fmt::Write as _;
use std::io::{BufReader, prelude::*};
use std::net::TcpStream;
use std::sync::{Arc, Mutex};

pub const LDR: &str = "LDR"; // for leader confirmation, reply="ACK"
pub const HEY: &str = "HEY"; // heartbeat to indicate availability, fmt="HEY [id]"
pub const ACK: &str = "ACK"; // generic reply, fmt="ACK"|"ACK base64(err)"|"ACK base64(JSON(members))"

pub fn handle_protocol(id: usize, mut conn: TcpStream, leader: usize, members: Arc<Mutex<HashMap<String, usize>>>) {
    let mut reader = BufReader::new(&conn);
    let mut data = String::new();
    reader.read_line(&mut data).unwrap();

    info!("[t{id}]: request: {data:?}");

    // Confirm if we are leader. Reply with ACK if so, otherwise, empty.
    if data.starts_with(LDR) {
        let mut ack = String::new();
        if leader > 0 {
            write!(&mut ack, "{}\n", ACK).unwrap();
        } else {
            write!(&mut ack, "\n").unwrap();
        }

        if let Err(e) = conn.write_all(ack.as_bytes()) {
            error!("[t{id}]: write_all failed: {e}");
        }

        return;
    }

    // We should be leader here. Member heartbeat.
    // Reply with list of members, including sender.
    if data.starts_with(HEY) {
        'onetime: loop {
            if leader < 1 {
                break 'onetime;
            }

            let ss: Vec<&str> = data.split(" ").collect();
            if ss.len() != 2 {
                break 'onetime;
            }

            {
                if let Ok(mut v) = members.lock() {
                    let s1 = &ss[1][..&ss[1].len() - 1];
                    v.insert(s1.to_string(), 0);
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
            write!(&mut ack, "{}\n", all).unwrap();
            if let Err(e) = conn.write_all(ack.as_bytes()) {
                error!("[t{id}]: write_all failed: {e}");
            }

            return;
        }
    }

    if let Err(e) = conn.write_all(b"\n") {
        error!("[t{id}]: write_all failed: {e}");
    }
}
