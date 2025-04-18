use std::io::{self, Read, Write};
use std::net::{Ipv4Addr, SocketAddrV4, TcpStream};

fn main() -> io::Result<()> {
    let addr = SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 42000);
    let mut stream = TcpStream::connect(addr)?;
    stream.set_nodelay(true)?;

    let mut size_buf = [0; 4];
    let mut response_buf = vec![0];

    loop {
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

        stream.write_vectored(&iov)?;

        stream.read_exact(&mut size_buf)?;
        let size = u32::from_be_bytes(size_buf) as usize;
        response_buf.resize(size, 0);
        stream.read_exact(&mut response_buf)?;

        io::stdout().write_all(&response_buf)?;
    }

    Ok(())
}
