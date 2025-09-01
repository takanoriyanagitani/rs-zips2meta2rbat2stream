use std::io;
use std::process::ExitCode;

use rs_zips2meta2rbat2stream::futures;

use rs_zips2meta2rbat2stream::arrow;

use futures::Stream;
use futures::StreamExt;
use futures::pin_mut;

use arrow::record_batch::RecordBatch;

fn env2dirname() -> Result<String, io::Error> {
    std::env::var("DIR_OF_ZIPS").map_err(io::Error::other)
}

async fn print_batch(b: &RecordBatch) -> Result<(), io::Error> {
    println!("{b:#?}");
    Ok(())
}

async fn print_batch_stream<S>(strm: S) -> Result<(), io::Error>
where
    S: Stream<Item = Result<RecordBatch, io::Error>>,
{
    pin_mut!(strm);
    while let Some(r) = strm.next().await {
        let rb: RecordBatch = r?;
        print_batch(&rb).await?;
    }
    Ok(())
}

async fn sub() -> Result<(), io::Error> {
    let dirname: String = env2dirname()?;
    let bstrm = rs_zips2meta2rbat2stream::dir2zips2stream(dirname.into())?;
    print_batch_stream(bstrm).await?;
    Ok(())
}

#[tokio::main]
async fn main() -> ExitCode {
    match sub().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("{e}");
            ExitCode::FAILURE
        }
    }
}
