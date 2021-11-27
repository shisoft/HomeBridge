use std::time::Duration;

use clap::{App, Arg, SubCommand};
use server::Server;

mod bridge;
mod server;
#[cfg(test)]
mod test;
mod utils;

const PORTS: &'static str = "ports";
const BRIDGE: &'static str = "bridge";
const THREADS: &'static str = "threads";
const BINDING: &'static str = "binding";

#[tokio::main]
async fn main() {
    env_logger::init();
    let matches = App::new("HomeBridge by Shisoft")
        .version("0.1")
        .author("Hao Shi <shisoftgenius@gmail.com>")
        .subcommand(
            SubCommand::with_name("server")
                .about("run at server mode at the machine you want to expose")
                .arg(Arg::with_name(PORTS)
                    .short("p")
                    .takes_value(true)
                    .help("provide ports mapping to expose, in format like 111:222,333:444, where right hand side is the port on bridge server"))
                .arg(Arg::with_name(BRIDGE)
                    .short("b")
                    .takes_value(true)
                    .help("the address to the bridge")),
        )
        .subcommand(SubCommand::with_name(BRIDGE)
                .about("run at bridge mode at the machine have fixed IP address and controllable firewall")
                .arg(Arg::with_name(BINDING)
                    .short("b")
                    .takes_value(true)
                    .help("address for server to talk to")))
        .get_matches();
    if let Some(m) = matches.subcommand_matches("server") {
        let ports = m.value_of(PORTS).expect("Must provide ports to expose");
        let bridge = m.value_of(BRIDGE).expect("Must provide bridge address");
        let threads = m
            .value_of(THREADS)
            .unwrap_or("1")
            .parse::<u32>()
            .expect("threads should be a number");
        start_server(ports, bridge, threads).await;
    } else if let Some(m) = matches.subcommand_matches(BRIDGE) {
        let bind = m.value_of(BINDING).unwrap_or("0.0.0.0:5678");
        start_bridge(bind).await;
    } else {
        panic!("Don't know how to run");
    }
}

async fn start_server<'a>(ports: &'a str, bridge: &'a str, threads: u32) {
    let ports = ports
        .split(",")
        .map(|s| {
            let arr = s.trim().split(":").collect::<Vec<_>>();
            let src_port = arr[0].parse::<u32>().expect("cannot parse source port");
            let dest_port = arr
                .get(1)
                .map(|dest| dest.parse::<u32>().expect("cannot parse destination port"))
                .unwrap_or(src_port);
            (src_port, dest_port)
        })
        .collect::<Vec<_>>();
    let server = Server::new();
    server.start(ports, bridge).await.unwrap();
    loop {
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

async fn start_bridge<'a>(bind: &'a str) {
    bridge::start(bind).await.unwrap()
}
