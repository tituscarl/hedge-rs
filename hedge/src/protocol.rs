use log::*;
use std::fmt::Write as _;
use std::io::{BufReader, prelude::*};
use std::net::TcpStream;

pub const LDR: &str = "LDR"; // for leader confirmation, reply="ACK"
pub const ACK: &str = "ACK"; // generic reply, fmt="ACK"|"ACK base64(err)"|"ACK base64(JSON(members))"

pub fn handle_protocol(id: usize, mut conn: TcpStream, leader: usize) {
    let mut reader = BufReader::new(&conn);
    let mut data = String::new();
    reader.read_line(&mut data).unwrap();

    info!("[t{id}]: request: {data:?}");

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

    if let Err(e) = conn.write_all(b"\n") {
        error!("[t{id}]: write_all failed: {e}");
    }
}
