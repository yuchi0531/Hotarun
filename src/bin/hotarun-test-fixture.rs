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
        "valid-ts-empty" => write_empty_ts_packet(),
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
        "decoder-prefix" => {
            write_all(b"decoded:");
            let mut input = [0u8; 4096];
            let size = io::stdin().read(&mut input).expect("read stdin");
            write_all(&input[..size]);
        }
        "dispatch" => match std::env::args().nth(2).as_deref() {
            Some("tlv") => write_all(b"TLV-raw-45328"),
            Some("27") => write_ts_fixture(),
            Some(channel) => {
                let channel = channel.parse::<u16>().unwrap_or(13);
                let offset = channel.saturating_sub(13) as u32;
                write_ts_fixture_with_services(101 + offset * 2, 202 + offset * 2);
            }
            None => write_ts_fixture(),
        },
        "scan-dispatch" => {
            let channel = std::env::args().nth(2).and_then(|value| value.parse::<u16>().ok()).unwrap_or(13);
            write_ts_fixture_with_services(101 + u32::from(channel.saturating_sub(13)) * 2, 202 + u32::from(channel.saturating_sub(13)) * 2);
        }
        "decoder-hold" | "hold" => std::thread::park(),
        "repeat-hold" => loop {
            write_ts_fixture();
            std::thread::sleep(std::time::Duration::from_millis(20));
        },
        "multipart-hold" => loop {
            let channel = std::env::args().nth(2).and_then(|value| value.parse::<u16>().ok()).unwrap_or(13);
            let offset = u32::from(channel.saturating_sub(13));
            write_multipart_ts_with_services(101 + offset * 2, 202 + offset * 2);
            std::thread::sleep(std::time::Duration::from_millis(10));
        },
        "exit-success" => {}
        "fail" => std::process::exit(1),
        _ => std::process::exit(2),
    }
}

fn write_ts_fixture() {
    write_ts_fixture_with_services(101, 202);
}

fn write_empty_ts_packet() {
    let mut packet = [0xff; 188];
    packet[0] = 0x47;
    packet[1] = 0x40;
    packet[2] = 0x10;
    packet[3] = 0x10;
    packet[4] = 0;
    write_all(&packet);
}

fn write_ts_fixture_with_services(first_service: u32, second_service: u32) {
    let mut pat = vec![0x00, 0xb0, 0x11, 0x00, 0x01, 0xc1, 0x00, 0x00];
    pat.extend_from_slice(&[
        (first_service >> 8) as u8, first_service as u8, 0xe1, 0x00,
        (second_service >> 8) as u8, second_service as u8, 0xe2, 0x00,
        0, 0, 0, 0,
    ]);
    let pmt = [
        0x02, 0xb0, 0x12, 0x00, 0x65, 0xc1, 0x00, 0x00, 0xe1, 0x01, 0xf0, 0x00,
        0x1b, 0xe1, 0x01, 0xf0, 0x00, 0x0f, 0xe1, 0x02, 0xf0, 0x00, 0, 0, 0, 0,
    ];
    write_packet(0, &pat);
    write_packet(0x10, &nit());
    write_packet(0x11, &sdt(first_service, second_service));
    write_packet(0x100, &pmt);
    write_packet(0x101, b"selected-pid");
    write_packet(0x200, b"other-pid");
}

fn nit() -> Vec<u8> {
    vec![0x40, 0xb0, 0x09, 0, 0, 0xc1, 0, 0, 0, 0, 0, 0]
}

fn sdt(first_service: u32, second_service: u32) -> Vec<u8> {
    let mut section = vec![0x42, 0, 0, 0, 1, 0xc1, 0, 0, 0, 0, 0];
    for (service, name) in [(first_service, b"One!".as_slice()), (second_service, b"Two!".as_slice())] {
        section.extend_from_slice(&[(service >> 8) as u8, service as u8, 0xfc, 0xf0, (name.len() + 5) as u8,
            0x48, (name.len() + 3) as u8, 1, 0, name.len() as u8]);
        section.extend_from_slice(name);
    }
    let section_length = section.len() + 4 - 3;
    section[1] = 0xb0 | ((section_length >> 8) as u8 & 0x0f);
    section[2] = section_length as u8;
    section.extend_from_slice(&[0, 0, 0, 0]);
    section
}

fn write_multipart_ts_with_services(first_service: u32, second_service: u32) {
    let mut pat = vec![0x00, 0xb0, 0x11, 0x00, 0x01, 0xc1, 0x00, 0x00];
    pat.extend_from_slice(&[
        (first_service >> 8) as u8, first_service as u8, 0xe1, 0x00,
        (second_service >> 8) as u8, second_service as u8, 0xe2, 0x00,
        0, 0, 0, 0,
    ]);
    write_packet_in_parts(0, &pat);
    write_packet(0x10, &nit());
    write_packet(0x11, &sdt(first_service, second_service));
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

fn write_packet_in_parts(pid: u16, payload: &[u8]) {
    let mut packet = [0xff; 188];
    packet[0] = 0x47;
    packet[1] = 0x40 | ((pid >> 8) as u8 & 0x1f);
    packet[2] = pid as u8;
    packet[3] = 0x10;
    packet[4] = 0;
    let len = payload.len().min(183);
    packet[5..5 + len].copy_from_slice(&payload[..len]);
    write_all(&packet[..47]);
    std::thread::sleep(std::time::Duration::from_millis(10));
    write_all(&packet[47..]);
}

fn write_all(bytes: &[u8]) {
    io::stdout().write_all(bytes).expect("write stdout");
    io::stdout().flush().expect("flush stdout");
}
