// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::io;
use std::io::prelude::*;
use std::path::PathBuf;
use std::time::Duration;

use aws_credential_types::Credentials;
use aws_sdk_s3::config::Region;
use aws_sdk_s3::Client;
use aws_sdk_s3::Config;
use bytes::BytesMut;
use csv::ByteRecord;
use csv::ReaderBuilder;
use openssl::symm::decrypt;
use openssl::symm::Cipher;
use serde::Deserialize;
use url::Url;

use mz_ore::collections::CollectionExt;
use mz_ore::retry::Retry;
use mz_repr::{Datum, RelationType, Row, RowArena};

static END_OF_COPY_MARKER: &[u8] = b"\\.";

pub fn encode_copy_row_binary(
    row: Row,
    typ: &RelationType,
    out: &mut Vec<u8>,
) -> Result<(), io::Error> {
    const NULL_BYTES: [u8; 4] = (-1i32).to_be_bytes();

    // 16-bit int of number of tuples.
    let count = i16::try_from(typ.column_types.len()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::Other,
            "column count does not fit into an i16",
        )
    })?;

    out.extend(count.to_be_bytes());
    let mut buf = BytesMut::new();
    for (field, typ) in row
        .iter()
        .zip(&typ.column_types)
        .map(|(datum, typ)| (mz_pgrepr::Value::from_datum(datum, &typ.scalar_type), typ))
    {
        match field {
            None => out.extend(NULL_BYTES),
            Some(field) => {
                buf.clear();
                field.encode_binary(&mz_pgrepr::Type::from(&typ.scalar_type), &mut buf)?;
                out.extend(
                    i32::try_from(buf.len())
                        .map_err(|_| {
                            io::Error::new(
                                io::ErrorKind::Other,
                                "field length does not fit into an i32",
                            )
                        })?
                        .to_be_bytes(),
                );
                out.extend(&buf);
            }
        }
    }
    Ok(())
}

pub fn encode_copy_row_text(
    row: Row,
    typ: &RelationType,
    out: &mut Vec<u8>,
) -> Result<(), io::Error> {
    let delim = b'\t';
    let null = b"\\N";
    let mut buf = BytesMut::new();
    for (idx, field) in mz_pgrepr::values_from_row(row, typ).into_iter().enumerate() {
        if idx > 0 {
            out.push(delim);
        }
        match field {
            None => out.extend(null),
            Some(field) => {
                buf.clear();
                field.encode_text(&mut buf);
                for b in &buf {
                    match b {
                        b'\\' => out.extend(b"\\\\"),
                        b'\n' => out.extend(b"\\n"),
                        b'\r' => out.extend(b"\\r"),
                        b'\t' => out.extend(b"\\t"),
                        _ => out.push(*b),
                    }
                }
            }
        }
    }
    out.push(b'\n');
    Ok(())
}

pub struct CopyTextFormatParser<'a> {
    data: &'a [u8],
    position: usize,
    column_delimiter: &'a str,
    null_string: &'a str,
    buffer: Vec<u8>,
}

impl<'a> CopyTextFormatParser<'a> {
    pub fn new(data: &'a [u8], column_delimiter: &'a str, null_string: &'a str) -> Self {
        Self {
            data,
            position: 0,
            column_delimiter,
            null_string,
            buffer: Vec::new(),
        }
    }

    fn peek(&self) -> Option<u8> {
        if self.position < self.data.len() {
            Some(self.data[self.position])
        } else {
            None
        }
    }

    fn consume_n(&mut self, n: usize) {
        self.position = std::cmp::min(self.position + n, self.data.len());
    }

    pub fn is_eof(&self) -> bool {
        self.peek().is_none() || self.is_end_of_copy_marker()
    }

    pub fn is_end_of_copy_marker(&self) -> bool {
        self.check_bytes(END_OF_COPY_MARKER)
    }

    fn is_end_of_line(&self) -> bool {
        match self.peek() {
            Some(b'\n') | None => true,
            _ => false,
        }
    }

    pub fn expect_end_of_line(&mut self) -> Result<(), io::Error> {
        if self.is_end_of_line() {
            self.consume_n(1);
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "extra data after last expected column",
            ))
        }
    }

    fn is_column_delimiter(&self) -> bool {
        self.check_bytes(self.column_delimiter.as_bytes())
    }

    pub fn expect_column_delimiter(&mut self) -> Result<(), io::Error> {
        if self.consume_bytes(self.column_delimiter.as_bytes()) {
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "missing data for column",
            ))
        }
    }

    fn check_bytes(&self, bytes: &[u8]) -> bool {
        let remaining_bytes = self.data.len() - self.position;
        remaining_bytes >= bytes.len()
            && self.data[self.position..]
                .iter()
                .zip(bytes.iter())
                .all(|(x, y)| x == y)
    }

    fn consume_bytes(&mut self, bytes: &[u8]) -> bool {
        if self.check_bytes(bytes) {
            self.consume_n(bytes.len());
            true
        } else {
            false
        }
    }

    fn consume_null_string(&mut self) -> bool {
        if self.null_string.is_empty() {
            // An empty NULL marker is supported. Look ahead to ensure that is followed by
            // a column delimiter, an end of line or it is at the end of the data.
            self.is_column_delimiter()
                || self.is_end_of_line()
                || self.is_end_of_copy_marker()
                || self.is_eof()
        } else {
            self.consume_bytes(self.null_string.as_bytes())
        }
    }

    pub fn consume_raw_value(&mut self) -> Result<Option<&[u8]>, io::Error> {
        if self.consume_null_string() {
            return Ok(None);
        }

        let mut start = self.position;

        // buffer where unescaped data is accumulated
        self.buffer.clear();

        while !self.is_eof() && !self.is_end_of_copy_marker() {
            if self.is_end_of_line() || self.is_column_delimiter() {
                break;
            }
            match self.peek() {
                Some(b'\\') => {
                    // Add non-escaped data parsed so far
                    self.buffer.extend(&self.data[start..self.position]);

                    self.consume_n(1);
                    match self.peek() {
                        Some(b'b') => {
                            self.consume_n(1);
                            self.buffer.push(8);
                        }
                        Some(b'f') => {
                            self.consume_n(1);
                            self.buffer.push(12);
                        }
                        Some(b'n') => {
                            self.consume_n(1);
                            self.buffer.push(b'\n');
                        }
                        Some(b'r') => {
                            self.consume_n(1);
                            self.buffer.push(b'\r');
                        }
                        Some(b't') => {
                            self.consume_n(1);
                            self.buffer.push(b'\t');
                        }
                        Some(b'v') => {
                            self.consume_n(1);
                            self.buffer.push(11);
                        }
                        Some(b'x') => {
                            self.consume_n(1);
                            match self.peek() {
                                Some(_c @ b'0'..=b'9')
                                | Some(_c @ b'A'..=b'F')
                                | Some(_c @ b'a'..=b'f') => {
                                    let mut value: u8 = 0;
                                    let decode_nibble = |b| match b {
                                        Some(c @ b'a'..=b'f') => Some(c - b'a' + 10),
                                        Some(c @ b'A'..=b'F') => Some(c - b'A' + 10),
                                        Some(c @ b'0'..=b'9') => Some(c - b'0'),
                                        _ => None,
                                    };
                                    for _ in 0..2 {
                                        match decode_nibble(self.peek()) {
                                            Some(c) => {
                                                self.consume_n(1);
                                                value = value << 4 | c;
                                            }
                                            _ => break,
                                        }
                                    }
                                    self.buffer.push(value);
                                }
                                _ => {
                                    self.buffer.push(b'x');
                                }
                            }
                        }
                        Some(_c @ b'0'..=b'7') => {
                            let mut value: u8 = 0;
                            for _ in 0..3 {
                                match self.peek() {
                                    Some(c @ b'0'..=b'7') => {
                                        self.consume_n(1);
                                        value = value << 3 | (c - b'0');
                                    }
                                    _ => break,
                                }
                            }
                            self.buffer.push(value);
                        }
                        Some(c) => {
                            self.consume_n(1);
                            self.buffer.push(c);
                        }
                        None => {
                            self.buffer.push(b'\\');
                        }
                    }

                    start = self.position;
                }
                Some(_) => {
                    self.consume_n(1);
                }
                None => {}
            }
        }

        // Return a slice of the original buffer if no escaped characters where processed
        if self.buffer.is_empty() {
            Ok(Some(&self.data[start..self.position]))
        } else {
            // ... otherwise, add the remaining non-escaped data to the decoding buffer
            // and return a pointer to it
            self.buffer.extend(&self.data[start..self.position]);
            Ok(Some(&self.buffer[..]))
        }
    }

    /// Error if more than `num_columns` values in `parser`.
    pub fn iter_raw(self, num_columns: usize) -> RawIterator<'a> {
        RawIterator {
            parser: self,
            current_column: 0,
            num_columns,
            truncate: false,
        }
    }

    /// Return no more than `num_columns` values from `parser`.
    pub fn iter_raw_truncating(self, num_columns: usize) -> RawIterator<'a> {
        RawIterator {
            parser: self,
            current_column: 0,
            num_columns,
            truncate: true,
        }
    }
}

pub struct RawIterator<'a> {
    parser: CopyTextFormatParser<'a>,
    current_column: usize,
    num_columns: usize,
    truncate: bool,
}

impl<'a> RawIterator<'a> {
    pub fn next(&mut self) -> Option<Result<Option<&[u8]>, io::Error>> {
        if self.current_column > self.num_columns {
            return None;
        }

        if self.current_column == self.num_columns {
            if !self.truncate {
                if let Some(err) = self.parser.expect_end_of_line().err() {
                    return Some(Err(err));
                }
            }

            return None;
        }

        if self.current_column > 0 {
            if let Some(err) = self.parser.expect_column_delimiter().err() {
                return Some(Err(err));
            }
        }

        self.current_column += 1;
        Some(self.parser.consume_raw_value())
    }
}

#[derive(Debug)]
pub enum CopyFormatParams<'a> {
    Text(CopyTextFormatParams<'a>),
    Csv(CopyCsvFormatParams<'a>),
}

#[derive(Debug)]
pub enum CopyFromTarget {
    Stdin,
    Url {
        url: Url,
        credentials: Option<String>,
        region: Option<String>,
        gzip: bool,
        encrypted: bool,
        manifest: bool,
    },
}

pub async fn copy_fetch_url(
    url: Url,
    credentials: Option<&str>,
    region: Option<&str>,
    gzip: bool,
    encrypted: bool,
    manifest: bool,
) -> Result<Vec<String>, anyhow::Error> {
    match url.scheme() {
        "s3" => {
            copy_fetch_s3(
                url,
                credentials.unwrap_or(""),
                region,
                gzip,
                encrypted,
                manifest,
            )
            .await
        }
        _ => anyhow::bail!("unsupported url scheme {}", url.scheme()),
    }
}

#[tokio::test]
async fn test_copy_fetch_s3() {
    const URL: &str = "s3://mjibson-fivetran-redshift/upload.manifest";
    const CREDENTIALS: &str = "aws_access_key_id=AKIAV2KIV5LPZML2LK5W;aws_secret_access_key=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;master_symmetric_key=NXF8SW4gLOZpEzvkpwDc9y6m5j7JiWSyPz7L+PBgLlw=";
    let files = copy_fetch_s3(
        url::Url::parse(URL).unwrap(),
        CREDENTIALS,
        Some("us-east-1"),
        true,
        true,
        true,
    )
    .await
    .unwrap();
    let file = files.into_element();
    println!("file: {file}");
}

#[test]
fn test_decrypt_s3() {
    let data = std::fs::read("/home/mjibson/materialize/s3/f0bd7c76-49e0-4499-9be8-abf40b0b75d7/1")
        .unwrap();
    let master_key = "NXF8SW4gLOZpEzvkpwDc9y6m5j7JiWSyPz7L+PBgLlw=";

    let f = S3File {
        raw_body: data,
        metadata: BTreeMap::from_iter([
            ("x-amz-iv".into(), "steLdZYfT/mtmfhGoGLhCQ==".into()),
            ("x-amz-unencrypted-content-length".into(), "116".into()),
            ("x-amz-matdesc".into(), "{}".into()),
            (
                "x-amz-key".into(),
                "eTOQVtxMT53JZnQMZnGQJhT0VVkJXnza7iFd6s8kSYTKcSuzURELPBZIW2iLVjKU".into(),
            ),
        ]),
    };

    let d = f.body(Some(master_key)).unwrap();
    dbg!(String::from_utf8(d));

    /*
    use aes_gcm::{
        aead::{Aead, KeyInit},
        Aes256Gcm, Nonce,
    };
    let cipher = Aes256Gcm::new_from_slice(&key).unwrap();
    let output = cipher
        .decrypt(Nonce::from_slice(b""), data.as_slice())
        .unwrap();
    */
}

async fn copy_fetch_s3(
    url: Url,
    credentials: &str,
    region: Option<&str>,
    gzip: bool,
    encrypted: bool,
    manifest: bool,
) -> Result<Vec<String>, anyhow::Error> {
    let mut creds = BTreeMap::new();
    for s in credentials.split(';') {
        let Some((key, val)) = s.split_once('=') else {
            anyhow::bail!("misformatted CREDENTIALS");
        };
        creds.insert(key, val.to_string());
    }
    let Some(aws_access_key_id) = creds.remove("aws_access_key_id") else {
        anyhow::bail!("CREDENTIALS missing aws_access_key_id");
    };
    let Some(aws_secret_access_key) = creds.remove("aws_secret_access_key") else {
        anyhow::bail!("CREDENTIALS missing aws_secret_access_key");
    };
    let session_token = creds.remove("token");
    let master_symmetric_key = if encrypted {
        let Some(master_symmetric_key) = creds.remove("master_symmetric_key") else {
            anyhow::bail!("CREDENTIALS missing master_symmetric_key");
        };
        Some(master_symmetric_key)
    } else {
        None
    };

    dbg!(
        &aws_access_key_id,
        &aws_secret_access_key,
        &session_token,
        &region,
        &credentials
    );

    let creds = Credentials::from_keys(aws_access_key_id, aws_secret_access_key, session_token);
    let config = Config::builder()
        .credentials_provider(creds)
        .region(region.map(|r| Region::new(r.to_owned())))
        .build();
    let client = Client::from_conf(config);

    let manifest: CopyManifest = if manifest {
        if let Ok(dur) = std::env::var("COPY_SLEEP") {
            let dur: u64 = dur.parse().unwrap();
            tokio::time::sleep(std::time::Duration::from_secs(dur)).await;
        }
        let manifest = fetch_s3_file(&client, &url).await?;
        serde_json::from_slice(&manifest.body(master_symmetric_key.as_deref())?)?
    } else {
        CopyManifest {
            entries: vec![CopyManifestEntry {
                url,
                mandatory: true,
            }],
        }
    };

    let mut files = Vec::new();
    for entry in manifest.entries {
        match fetch_s3_file(&client, &entry.url).await {
            Ok(file) => {
                let contents = file.body(master_symmetric_key.as_deref())?;
                dbg!(String::from_utf8(contents.clone()));
                let contents = if gzip {
                    let mut buf = String::new();
                    flate2::read::GzDecoder::new(&*contents).read_to_string(&mut buf)?;
                    buf
                } else {
                    String::from_utf8(contents)?
                };
                files.push(contents);
            }
            Err(err) => {
                if !entry.mandatory {
                    anyhow::bail!("non-mandatory COPY manifest files not supported");
                }
                // Detect errors for non-mandatory files that don't exist.
                /*
                if let aws_sdk_s3::error::SdkError::ServiceError(e) = &err {
                    if !entry.mandatory
                        && matches!(
                            e.err(),
                            aws_sdk_s3::operation::get_object::GetObjectError::NoSuchKey(_)
                        )
                    {
                        continue;
                    }
                }
                */
                return Err(err);
            }
        };
    }
    Ok(files)
}

#[derive(Debug)]
struct S3File {
    raw_body: Vec<u8>,
    metadata: BTreeMap<String, String>,
}

impl S3File {
    // Returns and possibly decrypts the file. master_symmetric_key can be a base64-encoded master
    // key.
    fn body(&self, master_symmetric_key: Option<&str>) -> Result<Vec<u8>, anyhow::Error> {
        const IV_LEN: usize = 12;
        if !self.metadata.contains_key("x-amz-iv") {
            return Ok(self.raw_body.clone());
        }
        if self.raw_body.len() < IV_LEN {
            anyhow::bail!("file too short to contain iv");
        }
        let Some(iv) = self.metadata.get("x-amz-iv") else {
            anyhow::bail!("expected s3 iv in metadata");
        };
        let Some(key) = self.metadata.get("x-amz-key") else {
            anyhow::bail!("expected s3 key in metadata");
        };
        let iv = aws_smithy_types::base64::decode(iv)?;
        let key = aws_smithy_types::base64::decode(key)?;
        let Some(master_symmetric_key) = master_symmetric_key else {
            anyhow::bail!("expected master symmetric key when decoding encrypted s3 file");
        };
        let master_symmetric_key = aws_smithy_types::base64::decode(master_symmetric_key).unwrap();
        let cipher = Cipher::aes_256_gcm();
        let unwrapped_key = decrypt(cipher, &master_symmetric_key, Some(&iv), &key).unwrap();
        dbg!(master_symmetric_key.len(), unwrapped_key.len());
        let iv = &self.raw_body[0..IV_LEN];
        Ok(decrypt(cipher, &unwrapped_key, Some(iv), &self.raw_body[IV_LEN..]).unwrap())
    }
}

// Fetches a file with retries.
async fn fetch_s3_file(client: &Client, url: &Url) -> Result<S3File, anyhow::Error> {
    if url.scheme() != "s3" {
        anyhow::bail!("expected s3 url scheme");
    }
    let Some(bucket) = url.host_str().map(|s| s.to_owned()) else {
        anyhow::bail!("url has no host");
    };
    let mut key = url.path().to_string();
    if key.starts_with('/') {
        key.remove(0);
    }

    // The retry numbers are made up here.
    Retry::default()
        .max_duration(Duration::from_secs(30))
        .retry_async_canceling(|_state| {
            if _state.i > 0 {
                println!("fetch s3 retry {}", _state.i);
            }
            let bucket = bucket.clone();
            let key = key.clone();
            async {
                let file = client
                    .get_object()
                    .bucket(bucket)
                    .key(key.clone())
                    .send()
                    .await?;
                let mut metadata = BTreeMap::new();
                if let Some(map) = file.metadata() {
                    metadata.extend(map.iter().map(|(k, v)| (k.clone(), v.clone())));
                }
                let body = file.body.collect().await.map(|data| data.to_vec())?;
                let mut path = PathBuf::new();
                path.set_file_name(key);
                let path = PathBuf::from("./s3").join(path);
                dbg!(&path);
                std::fs::create_dir_all(path.parent().unwrap()).unwrap();
                std::fs::write(path, &body).unwrap();
                Ok(S3File {
                    metadata,
                    raw_body: body,
                })
            }
        })
        .await
}

#[derive(Deserialize, Debug)]
struct CopyManifest {
    entries: Vec<CopyManifestEntry>,
}

#[derive(Deserialize, Debug)]
struct CopyManifestEntry {
    url: url::Url,
    #[serde(default)]
    mandatory: bool,
}

pub fn decode_copy_format<'a>(
    data: &[u8],
    column_types: &[mz_pgrepr::Type],
    params: CopyFormatParams<'a>,
) -> Result<Vec<Row>, io::Error> {
    match params {
        CopyFormatParams::Text(params) => decode_copy_format_text(data, column_types, params),
        CopyFormatParams::Csv(params) => decode_copy_format_csv(data, column_types, params),
    }
}

#[derive(Debug)]
pub struct CopyTextFormatParams<'a> {
    pub null: Cow<'a, str>,
    pub delimiter: Cow<'a, str>,
}

pub fn decode_copy_format_text(
    data: &[u8],
    column_types: &[mz_pgrepr::Type],
    CopyTextFormatParams { null, delimiter }: CopyTextFormatParams,
) -> Result<Vec<Row>, io::Error> {
    let mut rows = Vec::new();

    let mut parser = CopyTextFormatParser::new(data, &delimiter, &null);
    while !parser.is_eof() && !parser.is_end_of_copy_marker() {
        let mut row = Vec::new();
        let buf = RowArena::new();
        for (col, typ) in column_types.iter().enumerate() {
            if col > 0 {
                parser.expect_column_delimiter()?;
            }
            let raw_value = parser.consume_raw_value()?;
            if let Some(raw_value) = raw_value {
                match mz_pgrepr::Value::decode_text(typ, raw_value) {
                    Ok(value) => row.push(value.into_datum(&buf, typ)),
                    Err(err) => {
                        let msg = format!("unable to decode column: {}", err);
                        return Err(io::Error::new(io::ErrorKind::InvalidData, msg));
                    }
                }
            } else {
                row.push(Datum::Null);
            }
        }
        parser.expect_end_of_line()?;
        rows.push(Row::pack(row));
    }
    // Note that if there is any junk data after the end of copy marker, we drop
    // it on the floor as PG does.
    Ok(rows)
}

#[derive(Debug)]
pub struct CopyCsvFormatParams<'a> {
    pub delimiter: u8,
    pub quote: u8,
    pub escape: u8,
    pub header: bool,
    pub null: Cow<'a, str>,
    pub ignore_header: u32,
    pub truncate_columns: bool,
    pub accept_inv_chars: bool,
}

pub fn decode_copy_format_csv(
    data: &[u8],
    column_types: &[mz_pgrepr::Type],
    CopyCsvFormatParams {
        delimiter,
        quote,
        escape,
        null,
        header,
        ignore_header,
        truncate_columns,
        accept_inv_chars,
    }: CopyCsvFormatParams,
) -> Result<Vec<Row>, io::Error> {
    let mut rows = Vec::new();

    let (double_quote, escape) = if quote == escape {
        (true, None)
    } else {
        (false, Some(escape))
    };

    let mut rdr = ReaderBuilder::new()
        .delimiter(delimiter)
        .quote(quote)
        .has_headers(header)
        .double_quote(double_quote)
        .escape(escape)
        // Must be flexible to accept end of copy marker, which will always be 1
        // field.
        .flexible(true)
        .from_reader(data);

    let null_as_bytes = null.as_bytes();

    let mut record = ByteRecord::new();

    while rdr.read_byte_record(&mut record)? {
        if record.len() == 1 && record.iter().next() == Some(END_OF_COPY_MARKER) {
            break;
        }

        match record.len().cmp(&column_types.len()) {
            std::cmp::Ordering::Less => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "missing data for column",
            )),
            std::cmp::Ordering::Greater => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "extra data after last expected column",
            )),
            std::cmp::Ordering::Equal => Ok(()),
        }?;

        let mut row = Vec::new();
        let buf = RowArena::new();

        for (typ, raw_value) in column_types.iter().zip(record.iter()) {
            if raw_value == null_as_bytes {
                row.push(Datum::Null);
            } else {
                match mz_pgrepr::Value::decode_text(typ, raw_value) {
                    Ok(value) => row.push(value.into_datum(&buf, typ)),
                    Err(err) => {
                        let msg = format!("unable to decode column: {}", err);
                        return Err(io::Error::new(io::ErrorKind::InvalidData, msg));
                    }
                }
            }
        }
        rows.push(Row::pack(row));
    }

    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_copy_format_text_parser() {
        let text = "\t\\nt e\t\\N\t\n\\x60\\xA\\x7D\\x4a\n\\44\\044\\123".as_bytes();
        let mut parser = CopyTextFormatParser::new(text, "\t", "\\N");
        assert!(parser.is_column_delimiter());
        parser
            .expect_column_delimiter()
            .expect("expected column delimiter");
        assert_eq!(
            parser
                .consume_raw_value()
                .expect("unexpected error")
                .expect("unexpected empty result"),
            "\nt e".as_bytes()
        );
        parser
            .expect_column_delimiter()
            .expect("expected column delimiter");
        // null value
        assert!(parser
            .consume_raw_value()
            .expect("unexpected error")
            .is_none());
        parser
            .expect_column_delimiter()
            .expect("expected column delimiter");
        assert!(parser.is_end_of_line());
        parser.expect_end_of_line().expect("expected eol");
        // hex value
        assert_eq!(
            parser
                .consume_raw_value()
                .expect("unexpected error")
                .expect("unexpected empty result"),
            "`\n}J".as_bytes()
        );
        parser.expect_end_of_line().expect("expected eol");
        // octal value
        assert_eq!(
            parser
                .consume_raw_value()
                .expect("unexpected error")
                .expect("unexpected empty result"),
            "$$S".as_bytes()
        );
        assert!(parser.is_eof());
    }

    #[test]
    fn test_copy_format_text_empty_null_string() {
        let text = "\t\n10\t20\n30\t\n40\t".as_bytes();
        let expect = vec![
            vec![None, None],
            vec![Some("10"), Some("20")],
            vec![Some("30"), None],
            vec![Some("40"), None],
        ];
        let mut parser = CopyTextFormatParser::new(text, "\t", "");
        for line in expect {
            for (i, value) in line.iter().enumerate() {
                if i > 0 {
                    parser
                        .expect_column_delimiter()
                        .expect("expected column delimiter");
                }
                match value {
                    Some(s) => {
                        assert!(!parser.consume_null_string());
                        assert_eq!(
                            parser
                                .consume_raw_value()
                                .expect("unexpected error")
                                .expect("unexpected empty result"),
                            s.as_bytes()
                        );
                    }
                    None => {
                        assert!(parser.consume_null_string());
                    }
                }
            }
            parser.expect_end_of_line().expect("expected eol");
        }
    }

    #[test]
    fn test_copy_format_text_parser_escapes() {
        struct TestCase {
            input: &'static str,
            expect: &'static [u8],
        }
        let tests = vec![
            TestCase {
                input: "simple",
                expect: b"simple",
            },
            TestCase {
                input: r#"new\nline"#,
                expect: b"new\nline",
            },
            TestCase {
                input: r#"\b\f\n\r\t\v\\"#,
                expect: b"\x08\x0c\n\r\t\x0b\\",
            },
            TestCase {
                input: r#"\0\12\123"#,
                expect: &[0, 0o12, 0o123],
            },
            TestCase {
                input: r#"\x1\xaf"#,
                expect: &[0x01, 0xaf],
            },
            TestCase {
                input: r#"T\n\07\xEV\x0fA\xb2C\1"#,
                expect: b"T\n\x07\x0eV\x0fA\xb2C\x01",
            },
            TestCase {
                input: r#"\\\""#,
                expect: b"\\\"",
            },
            TestCase {
                input: r#"\x"#,
                expect: b"x",
            },
            TestCase {
                input: r#"\xg"#,
                expect: b"xg",
            },
            TestCase {
                input: r#"\"#,
                expect: b"\\",
            },
            TestCase {
                input: r#"\8"#,
                expect: b"8",
            },
            TestCase {
                input: r#"\a"#,
                expect: b"a",
            },
            TestCase {
                input: r#"\x\xg\8\xH\x32\s\"#,
                expect: b"xxg8xH2s\\",
            },
        ];

        for test in tests {
            let mut parser = CopyTextFormatParser::new(test.input.as_bytes(), "\t", "\\N");
            assert_eq!(
                parser
                    .consume_raw_value()
                    .expect("unexpected error")
                    .expect("unexpected empty result"),
                test.expect,
                "input: {}, expect: {:?}",
                test.input,
                std::str::from_utf8(test.expect),
            );
            assert!(parser.is_eof());
        }
    }
}
