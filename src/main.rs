// use std::net::{TcpListener, TcpStream};

// fn handle_client(_stream: TcpStream) {
// }
mod protocol;

fn main() -> std::io::Result<()> {
    // let listener = TcpListener::bind("127.0.0.1:8080")?;

    // for stream in listener.incoming() {
    //     handle_client(stream?);
    // }

    // get launch parameter
    let args: Vec<String> = std::env::args().collect();

    if args.len() < 2 {
        println!("Usage: {} <mode>", args[0]);
        return Ok(());
    }

    let mode = &args[1];

    if mode == "server" {
        println!("server mode");
        protocol::server();
    }
    else {
        println!("client mode");
        protocol::client();
    }

    Ok(())
}