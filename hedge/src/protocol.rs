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

    // Reply with ACK.
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

    // Reply with list of members, including sender.
    if data.starts_with(HEY) {
        'onetime: loop {
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

            let mut ack = String::new();

            {
                if let Ok(v) = members.lock() {
                    for (k, _) in &*v {
                        write!(&mut ack, "{},", k).unwrap();
                    }
                }
            }

            if let Err(e) = conn.write_all(ack[..ack.len() - 1].as_bytes()) {
                error!("[t{id}]: write_all failed: {e}");
            }

            break 'onetime;
        }
    }

    if let Err(e) = conn.write_all(b"\n") {
        error!("[t{id}]: write_all failed: {e}");
    }
}
