use std::io;
use std::io::Read;
use std::process::ExitCode;

use apache_avro::types::Value;
use apache_avro::Reader;

use rs_avro_count::bind;
use rs_avro_count::lift;

fn iter2count<I, T>(i: I) -> Result<usize, io::Error>
where
    I: Iterator<Item = T>,
{
    Ok(i.count())
}

fn print_count(cnt: usize) -> impl FnMut() -> Result<(), io::Error> {
    move || {
        println!("{cnt}");
        Ok(())
    }
}

fn reader2values<R>(reader: R) -> Result<impl Iterator<Item = Result<Value, io::Error>>, io::Error>
where
    R: Read,
{
    let rdr: Reader<_> = Reader::new(reader).map_err(io::Error::other)?;
    Ok(rdr.map(|rslt| rslt.map_err(io::Error::other)))
}

fn stdin2values() -> Result<impl Iterator<Item = Result<Value, io::Error>>, io::Error> {
    let i = io::stdin();
    let il = i.lock();
    reader2values(il)
}

fn stdin2iter2count() -> Result<usize, io::Error> {
    bind!(stdin2values, lift!(iter2count))()
}

fn stdin2count2print() -> Result<(), io::Error> {
    bind!(stdin2iter2count, print_count)()
}

fn sub() -> Result<(), io::Error> {
    stdin2count2print()
}

fn main() -> ExitCode {
    sub().map(|_| ExitCode::SUCCESS).unwrap_or_else(|e| {
        eprintln!("{e}");
        ExitCode::FAILURE
    })
}
