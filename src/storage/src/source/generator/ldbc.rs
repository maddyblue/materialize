// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::io::Read;
use std::path::PathBuf;

use bytes::{Buf, Bytes};
use csv::{ReaderBuilder, StringRecord};
use flate2::read::GzDecoder;
use mz_ore::now::NowFn;
use mz_repr::{RelationDesc, Row, RowArena};
use mz_storage_client::types::sources::{Generator, GeneratorMessageType, LDBC_DESCS};

use tracing::debug;

#[derive(Clone, Debug)]
pub struct Ldbc {
    pub urls: Vec<String>,
}

impl Generator for Ldbc {
    fn by_seed(
        &self,
        _: NowFn,
        _seed: Option<u64>,
    ) -> Box<dyn Iterator<Item = (usize, GeneratorMessageType, Row, i64)>> {
        // TODO: Figure out how to re-download the file on each restart after the source is done.
        // Maybe load generators already do that once they return None?
        let bytes = match read(&self.urls) {
            Ok(bytes) => bytes,
            Err(err) => {
                debug!("could not download file: {err}");
                return Box::new(std::iter::empty());
            }
        };
        let mut archive = tar::Archive::new(bytes.as_slice());
        let entries = match archive.entries() {
            Ok(entries) => entries,
            Err(err) => {
                debug!("count not extract tar: {err}");
                return Box::new(std::iter::empty());
            }
        };
        let entries = entries
            .map(|entry| {
                let mut entry = entry.unwrap();
                let path = entry.path().unwrap().into_owned();
                let mut bytes = Vec::new();
                entry.read_to_end(&mut bytes).unwrap();
                (path, bytes)
            })
            .collect::<BTreeMap<_, _>>();
        let ctx = Context {
            entries: entries.into_iter(),
            rdr: None,
            record: StringRecord::new(),
            arena: RowArena::new(),
            finalized: false,
        };
        Box::new(ctx)
    }
}

struct CsvFile {
    rdr: csv::Reader<bytes::buf::Reader<bytes::Bytes>>,
    desc: &'static RelationDesc,
    output: usize,
}

struct Context {
    entries: std::collections::btree_map::IntoIter<PathBuf, Vec<u8>>,
    rdr: Option<CsvFile>,
    record: StringRecord,
    arena: RowArena,
    finalized: bool,
}

impl Context {
    /// Opens the next file if `self.rdr` is `None`. `self.rdr` is set to `None` if there are no
    /// more files to read.
    fn next_file(&mut self) {
        while self.rdr.is_none() {
            let Some((mut path, mut entry)) = self.entries.next() else {
                return;
            };
            // Gunzip gz files, removing the .gz extension.
            if matches!(path.extension().and_then(|s| s.to_str()), Some("gz")) {
                let mut buf = Vec::new();
                if let Err(err) = GzDecoder::new(entry.as_slice()).read_to_end(&mut buf) {
                    tracing::debug!("error gunzipping inner ldbc file {:?}: {}", path, err);
                    continue;
                }
                path = path.with_extension("");
                entry = buf;
            }
            // Skip non-CSV files.
            if !matches!(path.extension().and_then(|s| s.to_str()), Some("csv")) {
                continue;
            }
            let parts = path.parent().and_then(|path| {
                let mut parts = path.components().rev();
                let Some(entity) = parts.next() else {
                return None;
            };
                let Some(entity) = entity.as_os_str().to_str() else {
                return None;
            };
                let Some(load) = parts.next() else {
                return None;
            };
                let Some(load) = load.as_os_str().to_str() else {
                return None;
            };
                Some((load, entity))
            });
            let (output, desc) = match parts {
                Some(("static", entity)) => {
                    let Some((output, desc)) = LDBC_DESCS.get(entity.to_ascii_lowercase().as_str()) else {
                        tracing::debug!("could not find ldbc relation: {entity}");
                        continue;
                    };
                    (output, desc)
                }
                Some(_) => continue,
                None => continue,
            };
            let reader = Bytes::from(entry).reader();
            let rdr = ReaderBuilder::new().delimiter(b'|').from_reader(reader);
            self.rdr = Some(CsvFile {
                rdr,
                desc,
                output: *output,
            });
        }
    }

    /// Reads the next record out of `self.rdr`. Returns `None` if `self.rdr` is out of records.
    fn next_record(&mut self) -> Option<(usize, Row)> {
        self.rdr
            .as_mut()
            .and_then(|csv| match csv.rdr.read_record(&mut self.record) {
                Ok(true) => {
                    let row = Row::pack(self.record.iter().zip(csv.desc.iter_types()).map(
                        |(field, typ)| {
                            let pgtyp = mz_pgrepr::Type::from(&typ.scalar_type);
                            match  mz_pgrepr::Value::decode_text(&pgtyp, field.as_bytes()) {
                                Ok(value) => value.into_datum(&self.arena, &pgtyp),
                                Err(_) => {
                                    tracing::error!("could not decode typ {typ:?} for value '{field}' in ldbc output {}", csv.output);
                                    // TODO: The null here may conflict with the relation desc.
                                    Datum::Null
                                }
                            }
                        },
                    ));
                    Some((csv.output, row))
                }
                Ok(false) => None,
                Err(_) => None,
            })
    }
}

impl Iterator for Context {
    type Item = (usize, GeneratorMessageType, Row, i64);

    fn next(&mut self) -> Option<Self::Item> {
        let (output, row) = match self.next_record() {
            Some((output, row)) => (output, row),
            None => {
                // If we're out of records for this file, open the next and read its first record.
                self.rdr = None;
                self.next_file();
                match self.next_record() {
                    Some((output, row)) => (output, row),
                    None => {
                        if self.finalized {
                            return None;
                        } else {
                            self.finalized = true;
                            return None;
                        }
                    }
                }
            }
        };
        Some((output, GeneratorMessageType::InProgress, row, 1))
    }
}

fn read(urls: &[String]) -> Result<Vec<u8>, anyhow::Error> {
    let bytes = bytes_from_urls(urls);
    let decompressed = zstd::stream::decode_all(bytes.reader())?;
    Ok(decompressed)
}

fn bytes_from_urls(urls: &[String]) -> Bytes {
    assert_eq!(urls.len(), 1, "only single file LDBC supported");
    let url = &urls[0];
    // TODO: use ore retry.
    loop {
        match read_url(url) {
            Ok(bytes) => return bytes,
            Err(err) => {
                debug!("ldbc file fetch: {url}: {err}");
                continue;
            }
        }
    }
}

fn read_url(url: &str) -> Result<Bytes, reqwest::Error> {
    reqwest::blocking::get(url)?.error_for_status()?.bytes()
}
