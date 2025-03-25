use log::*;
use std::collections::HashMap;
use std::fmt::Write as _;
use std::io::{BufReader, prelude::*};
use std::net::TcpStream;
use std::sync::{Arc, Mutex};

pub const LDR: &str = "LDR"; // for leader confirmation, reply="ACK"
pub const HEY: &str = "HEY"; // heartbeat to indicate availability, fmt="HEY [id]"
pub const ACK: &str = "ACK"; // generic reply, fmt="ACK"|"ACK base64(err)"

pub fn handle_protocol(id: usize, mut stream: TcpStream, leader: usize, members: Arc<Mutex<HashMap<String, usize>>>) {
    let mut reader = BufReader::new(&stream);
    let mut data = String::new();
    reader.read_line(&mut data).unwrap();

    info!("[T{id}]: request: {data:?}");

    // Confirm if we are leader. Reply with ACK if so, otherwise, empty.
    if data.starts_with(LDR) {
        let mut ack = String::new();
        if leader > 0 {
            write!(&mut ack, "{}\n", ACK).unwrap();
        } else {
            write!(&mut ack, "\n").unwrap();
        }

        if let Err(e) = stream.write_all(ack.as_bytes()) {
            error!("[T{id}]: write_all failed: {e}");
        }

        return;
    }

    // Heartbeat. If the payload is "HEY <name>\n", sender is non-leader.
    // If the payload is "HEY\n", sender is leader, for liveness check.
    if data.starts_with(HEY) {
        'onetime: loop {
            let ss: Vec<&str> = data.split(" ").collect();
            if ss.len() == 1 {
                let mut ack = String::new();
                write!(&mut ack, "{}\n", ACK).unwrap();

                if let Err(e) = stream.write_all(ack.as_bytes()) {
                    error!("[T{id}]: write_all failed: {e}");
                }

                return;
            }

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
            if let Err(e) = stream.write_all(ack.as_bytes()) {
                error!("[T{id}]: write_all failed: {e}");
            }

            return;
        }
    }

    if let Err(e) = stream.write_all(b"\n") {
        error!("[T{id}]: write_all failed: {e}");
    }
}
