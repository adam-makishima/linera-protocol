// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{collections::BTreeMap, ops::Deref, slice::Chunks, sync::Arc};

use async_lock::RwLock;
use clap::parser::Values;
use key_value_store::{statement, OptValue};
use linera_storage_service::common::{KeyTag, MAX_PAYLOAD_SIZE,MAX_KEY_SIZE};
use linera_views::{
    batch::Batch,
    common::{CommonStoreConfig, ReadableKeyValueStore, WritableKeyValueStore},
    memory::{create_memory_store_stream_queries, MemoryStore},
};
#[cfg(feature = "rocksdb")]
use linera_views::{
    common::AdminKeyValueStore,
    rocks_db::{RocksDbStore, RocksDbStoreConfig},
};
use serde::{de::value, Serialize};
use tonic::{transport::Server, Request, Response, Status, Streaming};

use std::pin::Pin;
use tokio::sync::mpsc;
use tokio_stream::{wrappers::ReceiverStream, Stream, StreamExt};

use deepsize::DeepSizeOf;

use crate::key_value_store::{
    statement::Operation,
    store_processor_server::{StoreProcessor, StoreProcessorServer},
    KeyValue, ReplyContainsKey, ReplyCreateNamespace, ReplyDeleteAll,
    ReplyDeleteNamespace, ReplyExistsNamespace, ReplyFindKeyValuesByPrefix, ReplyFindKeysByPrefix,
    ReplyListAll, ReplyReadMultiValues, ReplyReadValue,
    ReplyWriteBatch, RequestContainsKey, RequestCreateNamespace, RequestDeleteAll,
    RequestDeleteNamespace, RequestExistsNamespace, RequestFindKeyValuesByPrefix,
    RequestFindKeysByPrefix, RequestListAll, RequestReadMultiValues, RequestReadValue,
    RequestWriteBatch
};


#[derive(DeepSizeOf)]
struct DeepSizeWrapper(Option<Vec<u8>>,bool);

#[allow(clippy::derive_partial_eq_without_eq)]
// https://github.com/hyperium/tonic/issues/1056
pub mod key_value_store {
    tonic::include_proto!("key_value_store.v1");
}

enum ServiceStoreServer {
    Memory(MemoryStore),
    /// The RocksDb key value store
    #[cfg(feature = "rocksdb")]
    RocksDb(RocksDbStore),
}

//const MAX_KEY_SIZE: usize = 1000000;

impl ServiceStoreServer {
    pub async fn read_value_bytes(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Status> {
        match self {
            ServiceStoreServer::Memory(store) => store
                .read_value_bytes(key)
                .await
                .map_err(|_e| Status::not_found("read_value_bytes")),
            #[cfg(feature = "rocksdb")]
            ServiceStoreServer::RocksDb(store) => store
                .read_value_bytes(key)
                .await
                .map_err(|_e| Status::not_found("read_value_bytes")),
        }
    }

    pub async fn contains_key(&self, key: &[u8]) -> Result<bool, Status> {
        match self{
            ServiceStoreServer::Memory(store) => store
                .contains_key(key)
                .await
                .map_err(|_e| Status::not_found("contains_key")),
            #[cfg(feature = "rocksdb")]
            ServiceStoreServer::RocksDb(store) => store
                .contains_key(key)
                .await
                .map_err(|_e| Status::not_found("contains_key")),
        }
    }

    pub async fn read_multi_values_bytes(
        &self,
        keys: Vec<Vec<u8>>,
    ) -> Result<Vec<Option<Vec<u8>>>, Status> {
        match self{
            ServiceStoreServer::Memory(store) => store
                .read_multi_values_bytes(keys)
                .await
                .map_err(|_e| Status::not_found("read_multi_values_bytes")),
            #[cfg(feature = "rocksdb")]
            ServiceStoreServer::RocksDb(store) => store
                .read_multi_values_bytes(keys)
                .await
                .map_err(|_e| Status::not_found("read_multi_values_bytes")),
        }
    }

    pub async fn find_keys_by_prefix(&self, key_prefix: &[u8]) -> Result<Vec<Vec<u8>>, Status> {
        match self {
            ServiceStoreServer::Memory(store) => store
                .find_keys_by_prefix(key_prefix)
                .await
                .map_err(|_e| Status::not_found("find_keys_by_prefix")),
            #[cfg(feature = "rocksdb")]
            ServiceStoreServer::RocksDb(store) => store
                .find_keys_by_prefix(key_prefix)
                .await
                .map_err(|_e| Status::not_found("find_keys_by_prefix")),
        }
    }

    pub async fn find_key_values_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, Status> {
        match self {
            ServiceStoreServer::Memory(store) => store
                .find_key_values_by_prefix(key_prefix)
                .await
                .map_err(|_e| Status::not_found("find_key_values_by_prefix")),
            #[cfg(feature = "rocksdb")]
            ServiceStoreServer::RocksDb(store) => store
                .find_key_values_by_prefix(key_prefix)
                .await
                .map_err(|_e| Status::not_found("find_key_values_by_prefix")),
        }
    }

    pub async fn write_batch(&self, batch: Batch) -> Result<(), Status> {
        match self{
            ServiceStoreServer::Memory(store) => store
                .write_batch(batch, &[])
                .await
                .map_err(|_e| Status::not_found("write_batch")),
            #[cfg(feature = "rocksdb")]
            ServiceStoreServer::RocksDb(store) => store
                .write_batch(batch, &[])
                .await
                .map_err(|_e| Status::not_found("write_batch")),
        }
    }

    pub async fn list_all(&self) -> Result<Vec<Vec<u8>>, Status> {
        self.find_keys_by_prefix(&[KeyTag::Namespace as u8]).await
    }

    pub async fn delete_all(&self) -> Result<(), Status> {
        let mut batch = Batch::new();
        batch.delete_key_prefix(vec![KeyTag::Key as u8]);
        batch.delete_key_prefix(vec![KeyTag::Namespace as u8]);
        self.write_batch(batch).await
    }

    pub async fn exists_namespace(&self, namespace: &[u8]) -> Result<bool, Status> {
        let mut full_key = vec![KeyTag::Namespace as u8];
        full_key.extend(namespace);
        self.contains_key(&full_key).await
    }

    pub async fn create_namespace(&self, namespace: &[u8]) -> Result<(), Status> {
        let mut full_key = vec![KeyTag::Namespace as u8];
        full_key.extend(namespace);
        let mut batch = Batch::new();
        batch.put_key_value_bytes(full_key, vec![]);
        self.write_batch(batch).await
    }

    pub async fn delete_namespace(&self, namespace: &[u8]) -> Result<(), Status> {
        let mut batch = Batch::new();
        let mut full_key = vec![KeyTag::Namespace as u8];
        full_key.extend(namespace);
        batch.delete_key(full_key);
        let mut key_prefix = vec![KeyTag::Key as u8];
        key_prefix.extend(namespace);
        batch.delete_key_prefix(key_prefix);
        self.write_batch(batch).await
    }

}

#[derive(clap::Parser)]
#[command(
    name = "storage_service_server",
    version = linera_version::VersionInfo::default_clap_str(),
    about = "A server providing storage service",
)]
enum ServiceStoreServerOptions {
    #[command(name = "memory")]
    Memory {
        #[arg(long = "endpoint")]
        endpoint: String,
    },

    #[cfg(feature = "rocksdb")]
    #[command(name = "rocksdb")]
    RocksDb {
        #[arg(long = "endpoint")]
        path: String,
        #[arg(long = "endpoint")]
        endpoint: String,
    },
}

#[tonic::async_trait]
impl StoreProcessor for ServiceStoreServer {

    type ProcessReadValueStream = ReceiverStream<Result<ReplyReadValue, Status>>;

    async fn process_read_value(
        &self,
        request: Request<RequestReadValue>,
    ) -> Result<Response<Self::ProcessReadValueStream>, Status> {
        let (mut tx, rx) = mpsc::channel(4);
        let request = request.into_inner();
        let RequestReadValue { key } = request;
        let value = self.read_value_bytes(&key).await?;
        match value{
            Some(value)=>{
                if value.len()<=MAX_PAYLOAD_SIZE{
                    tokio::spawn(async move {
                        tx.send(Ok(ReplyReadValue {value:Some(value) })).await;
                    });
                    return Ok(Response::new(ReceiverStream::new(rx)))
                }
                let chunks = value
                    .chunks(MAX_PAYLOAD_SIZE)
                    .map(|x| x.to_vec())
                    .collect::<Vec<_>>();
                let num_of_chunks = chunks.len() ;
                tokio::spawn(async move {
                    for index in 0..num_of_chunks{
                        tx.send(Ok(ReplyReadValue { value:Some(chunks[index].clone())})).await;
                    }
                });
                Ok(Response::new(ReceiverStream::new(rx)))

            }
            None=>{
                tokio::spawn(async move {
                    tx.send(Ok(ReplyReadValue {value:None })).await;
                });
                return Ok(Response::new(ReceiverStream::new(rx)))
            }

        }

    }

    async fn process_contains_key(
        &self,
        request: Request<RequestContainsKey>,
    ) -> Result<Response<ReplyContainsKey>, Status> {
        let request = request.into_inner();
        let RequestContainsKey { key } = request;
        let test = self.contains_key(&key).await?;
        let response = ReplyContainsKey { test };
        Ok(Response::new(response))
    }

    type ProcessReadMultiValuesStream = ReceiverStream<Result<ReplyReadMultiValues, Status>>;

    async fn process_read_multi_values(
        &self,
        request: Request<Streaming<RequestReadMultiValues>>,
    ) -> Result<Response<Self::ProcessReadMultiValuesStream>, Status> {
        let (tx, rx) = mpsc::channel(4);

        let mut stream = request.into_inner();
        let mut values = Vec::new();
        while let Some(RequestReadMultiValues { keys }) = stream.message().await?{
            let mut inner_values = self.read_multi_values_bytes(keys).await?;
            values.append(&mut inner_values);
        }
        let values = values
            .into_iter()
            .map(|value| if value.is_some(){
                    let mut chunks = value.unwrap()
                    .chunks(MAX_PAYLOAD_SIZE)
                    .map(|x|x.to_vec())
                    .map(|x|OptValue{value:Some(x),last_chunk:false})
                    .collect::<Vec<_>>();
                    if let Some(mut last_chunk) = chunks.pop(){
                        last_chunk.last_chunk = true;
                        chunks.push(last_chunk);
                    }else{
                        chunks.push(OptValue{value:Some(vec![]),last_chunk:true});
                    }
                    chunks
                }else{
                    vec![OptValue { value:value, last_chunk:true }]
                }
            )
            .flatten()
            .collect::<Vec<_>>();

            //grouping values in sizes of 4M chunks
            let mut grouped_values = Vec::new();
            let mut chunk = Vec::new();
            let mut chunk_size = 0;
            for value in values{
                let value_size = DeepSizeWrapper(value.value.clone(),value.clone().last_chunk).deep_size_of();
                if chunk_size+value_size<=MAX_PAYLOAD_SIZE{
                    chunk.push(value);
                    grouped_values.pop();
                    grouped_values.push(chunk.clone());
                    chunk_size+=value_size;
                }
                else {
                    chunk.clear();
                    chunk_size=0;
                    chunk.push(value);
                    grouped_values.push(chunk.clone());
                    chunk_size+=value_size;
                }
            }

            tokio::spawn(async move {
                for value in grouped_values{
                tx.send(Ok(ReplyReadMultiValues { values:value})).await;
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    type ProcessFindKeysByPrefixStream = ReceiverStream<Result<ReplyFindKeysByPrefix, Status>>;


    async fn process_find_keys_by_prefix(
        &self,
        request: Request<RequestFindKeysByPrefix>,
    ) -> Result<Response<Self::ProcessFindKeysByPrefixStream>, Status> {
        let (tx, rx) = mpsc::channel(4);
        let request = request.into_inner();
        let RequestFindKeysByPrefix { key_prefix } = request;
        let keys = self.find_keys_by_prefix(&key_prefix).await?;

        if keys.len()==0{
            tokio::spawn(async move {
                tx.send(Ok(ReplyFindKeysByPrefix {keys})).await;
            });
            return Ok(Response::new(ReceiverStream::new(rx)));
        }
        let mut grouped_keys = Vec::new();
        let mut chunk = Vec::new();
        let mut chunk_size = 0;
        for key in keys{
            let key_size = key.len();
            if key_size+chunk_size<=MAX_PAYLOAD_SIZE{
                chunk.push(key);
                grouped_keys.pop();
                grouped_keys.push(chunk.clone());
                chunk_size+=key_size;
            }
            else{
                chunk.clear();
                chunk_size=0;
                chunk.push(key);
                grouped_keys.push(chunk.clone());
                chunk_size+=key_size;

            }
        }

        tokio::spawn(async move {
            for keys in grouped_keys{
                tx.send(Ok(ReplyFindKeysByPrefix {keys})).await;
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    type ProcessFindKeyValuesByPrefixStream = ReceiverStream<Result<ReplyFindKeyValuesByPrefix, Status>>;

    async fn process_find_key_values_by_prefix(
        &self,
        request: Request<RequestFindKeyValuesByPrefix>,
    ) -> Result<Response<Self::ProcessFindKeyValuesByPrefixStream>, Status> {
        let ( tx, rx) = mpsc::channel(4);
        let request = request.into_inner();
        let RequestFindKeyValuesByPrefix { key_prefix } = request;
        let key_values = self.find_key_values_by_prefix(&key_prefix).await?;
        let key_values = key_values
        .iter()
        .map(|key_value|{
                let key_size = key_value.0.len();
                let mut value = [0u8,0,0,1].to_vec();
                value.append(&mut key_value.1.clone());
                let value_size = value.len();
                let mut chunk = Vec::new();
                if key_size+value_size<=MAX_PAYLOAD_SIZE{
                    chunk.push(KeyValue{key:key_value.0.clone(),value:value.clone()});
                    return chunk
                }
                let mut chunks = value
                    .chunks(MAX_KEY_SIZE)
                    .map(|x|x.to_vec())
                    .collect::<Vec<_>>();
                let num_of_chunks = (chunks.len() as u32)
                    .to_be_bytes()
                    .to_vec();
                chunks[0].splice(0..4, num_of_chunks);
                chunk.push(KeyValue{key:key_value.0.clone(),value:chunks[0].clone()});
                for i in 1..chunks.len(){
                    chunk.push(KeyValue{key:vec![], value:chunks[i].clone()});
                }
                chunk
            }
        )
        .flatten()
        .collect::<Vec<_>>();

        if key_values.len()==0{
            tokio::spawn(async move {
                tx.send(Ok(ReplyFindKeyValuesByPrefix {key_values})).await;
            });
            return Ok(Response::new(ReceiverStream::new(rx)));
        }

        let mut grouped_key_values = Vec::new();
        let mut chunk = Vec::new();
        let mut chunk_size = 0;
        for key_value in key_values{
            let key_size = key_value.key.len();
            let value_size = key_value.value.len();
            if key_size+value_size+chunk_size<=MAX_PAYLOAD_SIZE{
                chunk.push(key_value);
                grouped_key_values.pop();
                grouped_key_values.push(chunk.clone());
                chunk_size+=key_size+value_size;
            }
            else{
                chunk.clear();
                chunk_size=0;
                chunk.push(key_value);
                grouped_key_values.push(chunk.clone());
                chunk_size+=key_size+value_size;

            }
        }
        tokio::spawn(async move {
            for key_values in grouped_key_values{
                tx.send(Ok(ReplyFindKeyValuesByPrefix { key_values})).await;
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn process_write_batch(
        &self,
        request: Request<Streaming<RequestWriteBatch>>,
    ) -> Result<Response<ReplyWriteBatch>, Status> {
        let mut stream = request.into_inner();
        let mut batch = Batch::default();
        let mut key_value_recombined = KeyValue { key: Vec::new(), value: Vec::new() };
        let mut num_of_chunks = 0u32;
        while let Some(Ok(RequestWriteBatch { statements })) = stream.next().await{

            for statement in statements {
                match statement.operation.unwrap() {
                    Operation::Delete(key) => {
                        batch.delete_key(key);
                    },
                    Operation::Put(key_value) => {
                        let mut value = key_value.value.clone();
                        if num_of_chunks == 0{
                            num_of_chunks  = u32::from_be_bytes(<[u8;4]>::try_from(value
                                .drain(0..4)
                                .as_slice())
                                .unwrap());
                            key_value_recombined = KeyValue{key :key_value.key , value : vec![]};
                        }
                        if num_of_chunks == 1{
                            key_value_recombined.value.extend(value);
                            num_of_chunks-=1;
                            batch.put_key_value_bytes(key_value_recombined.key.clone(),key_value_recombined.value.clone());
                        }else{
                            key_value_recombined.value.extend(value);
                            num_of_chunks-=1;
                        }

                    },
                    Operation::DeletePrefix(key_prefix) => {
                        batch.delete_key_prefix(key_prefix);
                    },
                }
            }
        }
        self.write_batch(batch).await?;
        let response = ReplyWriteBatch {};
        Ok(Response::new(response))
    }



    async fn process_create_namespace(
        &self,
        request: Request<RequestCreateNamespace>,
    ) -> Result<Response<ReplyCreateNamespace>, Status> {
        let request = request.into_inner();
        let RequestCreateNamespace { namespace } = request;
        self.create_namespace(&namespace).await?;
        let response = ReplyCreateNamespace {};
        Ok(Response::new(response))
    }

    async fn process_exists_namespace(
        &self,
        request: Request<RequestExistsNamespace>,
    ) -> Result<Response<ReplyExistsNamespace>, Status> {
        let request = request.into_inner();
        let RequestExistsNamespace { namespace } = request;
        let exists = self.exists_namespace(&namespace).await?;
        let response = ReplyExistsNamespace { exists };
        Ok(Response::new(response))
    }

    async fn process_delete_namespace(
        &self,
        request: Request<RequestDeleteNamespace>,
    ) -> Result<Response<ReplyDeleteNamespace>, Status> {
        let request = request.into_inner();
        let RequestDeleteNamespace { namespace } = request;
        self.delete_namespace(&namespace).await?;
        let response = ReplyDeleteNamespace {};
        Ok(Response::new(response))
    }

    async fn process_list_all(
        &self,
        _request: Request<RequestListAll>,
    ) -> Result<Response<ReplyListAll>, Status> {
        let namespaces = self.list_all().await?;
        let response = ReplyListAll { namespaces };
        Ok(Response::new(response))
    }

    async fn process_delete_all(
        &self,
        _request: Request<RequestDeleteAll>,
    ) -> Result<Response<ReplyDeleteAll>, Status> {
        self.delete_all().await?;
        let response = ReplyDeleteAll {};
        Ok(Response::new(response))
    }

}

#[tokio::main]
async fn main() {
    let options = <ServiceStoreServerOptions as clap::Parser>::parse();
    let common_config = CommonStoreConfig::default();
    let (store, endpoint) = match options {
        ServiceStoreServerOptions::Memory { endpoint } => {
            let store = create_memory_store_stream_queries(common_config.max_stream_queries);
            (ServiceStoreServer::Memory(store), endpoint)
        }
        #[cfg(feature = "rocksdb")]
        ServiceStoreServerOptions::RocksDb { path, endpoint } => {
            let path_buf = path.into();
            let config = RocksDbStoreConfig {
                path_buf,
                common_config,
            };
            let namespace = "linera";
            let store = RocksDbStore::maybe_create_and_connect(&config, namespace)
                .await
                .expect("store");
            (ServiceStoreServer::RocksDb(store), endpoint)
        }
    };
    let endpoint = endpoint.parse().unwrap();
    Server::builder()
        .add_service(StoreProcessorServer::new(store))
        .serve(endpoint)
        .await
        .expect("a successful running of the server");
}
