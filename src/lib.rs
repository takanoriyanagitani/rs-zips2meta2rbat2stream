pub use arrow;
pub use futures;

use std::io;

use io::BufReader;

use std::path::PathBuf;

use std::fs::DirEntry;
use std::fs::File;

use futures::Stream;

use arrow::record_batch::RecordBatch;

pub trait ZipToBatch: Sync + Send + 'static {
    type ZipId;

    fn zip2batch(&self, id: Self::ZipId) -> Result<RecordBatch, io::Error>;
}

pub struct ZipConvFs {
    pub root: PathBuf,
}

impl ZipToBatch for ZipConvFs {
    type ZipId = String;

    fn zip2batch(&self, id: Self::ZipId) -> Result<RecordBatch, io::Error> {
        let fullpath = self.root.join(&id);
        let f: File = File::open(fullpath)?;
        let rdr = BufReader::new(f);
        let z = zip::ZipArchive::new(rdr)?;

        rs_zip2meta2rbat::sync::zip2record_batch(id, z)
    }
}

pub fn ids2stream<I, Z>(ids: I, zconv: Z) -> impl Stream<Item = Result<RecordBatch, io::Error>>
where
    Z: ZipToBatch,
    I: Iterator<Item = Result<Z::ZipId, io::Error>>,
{
    async_stream::try_stream! {
        for rid in ids {
            let id: Z::ZipId = rid?;
            let rb: RecordBatch = zconv.zip2batch(id)?;
            yield rb;
        }
    }
}

pub fn dirent2name(dirent: DirEntry) -> Result<String, io::Error> {
    let ostr = dirent.file_name();
    ostr.into_string()
        .map_err(|_| "invalid file name")
        .map_err(io::Error::other)
}

pub fn is_zip_name(name: &str) -> bool {
    name.ends_with(".zip")
}

pub fn rname2zname(rname: Result<String, io::Error>) -> Option<Result<String, io::Error>> {
    match rname {
        Err(e) => Some(Err(e)),
        Ok(name) => {
            let is_zip: bool = is_zip_name(&name);
            is_zip.then_some(Ok(name))
        }
    }
}

/// Gets the zip filenames from the dir and converts them to a stream.
pub fn dir2zips2stream(
    dirname: PathBuf,
) -> Result<impl Stream<Item = Result<RecordBatch, io::Error>>, io::Error> {
    let dirents = std::fs::read_dir(&dirname)?;
    let mapd = dirents.map(|rdir| rdir.and_then(dirent2name));
    let filtered = mapd.filter_map(rname2zname);
    let zconv = ZipConvFs { root: dirname };
    Ok(ids2stream(filtered, zconv))
}
