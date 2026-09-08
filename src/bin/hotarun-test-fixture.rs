//! Small Rust-only child process used by deterministic stream tests.
//! It intentionally has no timing or shell dependencies.

use std::io::{self, Read, Write};

fn main() {
    let mode = std::env::args().nth(1).unwrap_or_default();
    match mode.as_str() {
        "raw" => write_all(b"TLV-raw-45328"),
        "raw-hold" => {
            write_all(b"TLV-raw-45328");
            std::thread::park();
        }
        "lower" => write_all(b"lower"),
        "ts" => write_all(&[0x47, 0x00, 0x01, b'T', b'L', b'V']),
        "ts-hold" => {
            write_ts_fixture();
            std::thread::park();
        }
        "bytes" => write_all(b"bytes"),
        "fanout" => write_all(b"hello-fanout"),
        "respawn" => write_all(b"respawn-data"),
        "decoder-upper" => {
            let mut input = [0u8; 4096];
            loop {
                let size = io::stdin().read(&mut input).expect("read stdin");
                if size == 0 {
                    break;
                }
                input[..size].make_ascii_uppercase();
                write_all(&input[..size]);
            }
        }
        "dispatch" => match std::env::args().nth(2).as_deref() {
            Some("tlv") => write_all(b"TLV-raw-45328"),
            _ => write_ts_fixture(),
        },
        "decoder-hold" | "hold" => std::thread::park(),
        "exit-success" => {}
        "fail" => std::process::exit(1),
        _ => std::process::exit(2),
    }
}

fn write_ts_fixture() {
    let mut pat = vec![0x00, 0xb0, 0x11, 0x00, 0x01, 0xc1, 0x00, 0x00];
    pat.extend_from_slice(&[
        0x00, 0x65, 0xe1, 0x00, // service 101 -> PMT 0x100
        0x00, 0xca, 0xe2, 0x00, // service 202 -> PMT 0x200
        0, 0, 0, 0,
    ]);
    let pmt = [
        0x02, 0xb0, 0x12, 0x00, 0x65, 0xc1, 0x00, 0x00, 0xe1, 0x01, 0xf0, 0x00,
        0x1b, 0xe1, 0x01, 0xf0, 0x00, 0x0f, 0xe1, 0x02, 0xf0, 0x00, 0, 0, 0, 0,
    ];
    write_packet(0, &pat);
    write_packet(0x100, &pmt);
    write_packet(0x101, b"selected-pid");
    write_packet(0x200, b"other-pid");
}

fn write_packet(pid: u16, payload: &[u8]) {
    let mut packet = [0xff; 188];
    packet[0] = 0x47;
    packet[1] = 0x40 | ((pid >> 8) as u8 & 0x1f);
    packet[2] = pid as u8;
    packet[3] = 0x10;
    packet[4] = 0;
    let len = payload.len().min(183);
    packet[5..5 + len].copy_from_slice(&payload[..len]);
    write_all(&packet);
}

fn write_all(bytes: &[u8]) {
    io::stdout().write_all(bytes).expect("write stdout");
    io::stdout().flush().expect("flush stdout");
}
