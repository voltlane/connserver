use std::io::{self, Read, Write};
use std::net::{Ipv4Addr, SocketAddrV4, TcpStream};
use std::str;

fn main() -> io::Result<()> {
    let addr = SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 42000);
    let mut stream = TcpStream::connect(addr)?;
    stream.set_nodelay(true)?;

    use std::io::IsTerminal;
use std::time::Instant;

    let mut size_buf = [0; 4];
    let mut response_buf = vec![0];
    let mut total_latency = std::time::Duration::new(0, 0);
    let start_time = Instant::now();

    let mut iterations = 0;
    while start_time.elapsed().as_secs() < 100 {
        let mut input_buf = [0; 1024];
        let bytes_read = io::stdin().read(&mut input_buf)?;
        if bytes_read == 0 {
            break;
        }

        let size = (bytes_read as u32).to_be_bytes();
        let iov = [
            std::io::IoSlice::new(&size),
            std::io::IoSlice::new(&input_buf[..bytes_read]),
        ];

        let start = Instant::now();
        stream.write_vectored(&iov)?;

        stream.read_exact(&mut size_buf)?;
        let size = u32::from_be_bytes(size_buf) as usize;
        response_buf.resize(size, 0);
        stream.read_exact(&mut response_buf)?;
        let duration = start.elapsed();

        total_latency += duration;
        iterations += 1;

        io::stdout().write_all(&response_buf)?;
    }

    if iterations > 0 {
        let average_latency = total_latency / iterations as u32;
        eprintln!("Average Latency: {:?}", average_latency);
    } else {
        eprintln!("No iterations were completed.");
    }

    Ok(())
}
