// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use async_lock::{RwLock, Semaphore, SemaphoreGuard};
use linera_base::ensure;
#[cfg(with_testing)]
use linera_views::test_utils::generate_test_namespace;
use linera_views::{
    batch::{Batch, WriteOperation},
    common::{
        AdminKeyValueStore, CommonStoreConfig, KeyValueStore, ReadableKeyValueStore,
        WritableKeyValueStore,
    },
};
use tonic::transport::{Channel, Endpoint, Error};

use crate::{
    common::{KeyTag, ServiceContextError, ServiceStoreConfig, MAX_PAYLOAD_SIZE,MAX_KEY_SIZE},
    key_value_store::{
        statement::{self, Operation}, store_processor_client::StoreProcessorClient, KeyValue,
        OptValue, ReplyContainsKey, ReplyExistsNamespace, ReplyFindKeyValuesByPrefix, ReplyFindKeysByPrefix,
        ReplyListAll, ReplyReadMultiValues, ReplyReadValue, RequestContainsKey, RequestCreateNamespace,
        RequestDeleteAll, RequestDeleteNamespace, RequestExistsNamespace, RequestFindKeyValuesByPrefix,
        RequestFindKeysByPrefix, RequestListAll, RequestReadMultiValues, RequestReadValue, RequestWriteBatch,
        Statement,
    },
};

// The maximum key size is set to 1M rather arbitrarily.
//const MAX_KEY_SIZE: usize = 1000000;

// The shared store client.
// * Interior mutability is required for client because
// accessing requires mutability while the KeyValueStore
// does not allow it.
// * The semaphore and max_stream_queries work as other
// stores.
//
// The encoding of namespaces is done by taking their
// serialization. This works because the set of serialization
// of strings is prefix free.
// The data is stored in the following way.
// * A `key` in a `namespace` is stored as
//   [KeyTag::Key] + [namespace] + [key]
// * An additional key with empty value is stored at
//   [KeyTag::Namespace] + [namespace]
// is stored to indicate the existence of a namespace.
#[derive(Clone)]
pub struct ServiceStoreClient {
    client: Arc<RwLock<StoreProcessorClient<Channel>>>,
    semaphore: Option<Arc<Semaphore>>,
    max_stream_queries: usize,
    namespace: Vec<u8>,
}

impl ReadableKeyValueStore<ServiceContextError> for ServiceStoreClient {
    const MAX_KEY_SIZE: usize = MAX_KEY_SIZE;
    type Keys = Vec<Vec<u8>>;
    type KeyValues = Vec<(Vec<u8>, Vec<u8>)>;

    fn max_stream_queries(&self) -> usize {
        self.max_stream_queries
    }

    async fn read_value_bytes(&self, key: &[u8]) -> Result<Option<Vec<u8>>, ServiceContextError> {
        ensure!(key.len() <= MAX_KEY_SIZE, ServiceContextError::KeyTooLong);
        let mut full_key = self.namespace.clone();
        full_key.extend(key);
        let query = RequestReadValue { key: full_key };
        let request = tonic::Request::new(query);
        let mut client = self.client.write().await;
        let _guard = self.acquire().await;
        let mut response_stream = client
            .process_read_value(request)
            .await?
            .into_inner();
        let mut recombined_value = Vec::new();
        if let Some(chunk) = response_stream.message().await?{
            let v = &chunk.value;
            if v.is_none(){
                return Ok(None);
            }
            recombined_value.append(&mut v.clone().unwrap());
        }
        while let Some(ReplyReadValue { value }) = response_stream.message().await?{
            recombined_value.append(&mut value.unwrap());
        }
        Ok(Some(recombined_value))
    }

    async fn contains_key(&self, key: &[u8]) -> Result<bool, ServiceContextError> {
        ensure!(key.len() <= MAX_KEY_SIZE, ServiceContextError::KeyTooLong);
        let mut full_key = self.namespace.clone();
        full_key.extend(key);
        let query = RequestContainsKey { key: full_key };
        let request = tonic::Request::new(query);
        let mut client = self.client.write().await;
        let _guard = self.acquire().await;
        let response = client.process_contains_key(request).await?;
        let response = response.into_inner();
        let ReplyContainsKey { test } = response;
        Ok(test)
    }

    async fn read_multi_values_bytes(
        &self,
        keys: Vec<Vec<u8>>,
    ) -> Result<Vec<Option<Vec<u8>>>, ServiceContextError> {
        let mut full_keys = Vec::new();
        for key in keys {
            ensure!(key.len() <= MAX_KEY_SIZE, ServiceContextError::KeyTooLong);
            let mut full_key = self.namespace.clone();
            full_key.extend(&key);
            full_keys.push(full_key);
        }
        let mut grouped_keys = Vec::new();
        let mut chunk = Vec::new();
        let mut chunk_size = 0;
        for key in full_keys{
            let key_size = key.len();
            if key_size+chunk_size<=MAX_PAYLOAD_SIZE{
                grouped_keys.pop().unwrap_or_default();
                chunk.push(key);
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
        let mut query = Vec::new();
        for keys in grouped_keys{
            query.push(RequestReadMultiValues { keys});
        }
        let request = tonic::Request::new(tokio_stream::iter(query));
        let mut client = self.client.write().await;
        let _guard = self.acquire().await;
        let mut response_stream = client
            .process_read_multi_values(request)
            .await?
            .into_inner();
        let mut response = Vec::new();
        let mut recombined_value = Vec::new() ;
        while let Some(ReplyReadMultiValues { values }) = response_stream.message().await?{
            for OptValue { value, last_chunk } in values{
                if value.is_none(){
                    response.push(value);
                    continue;
                }
                recombined_value.append(&mut value.unwrap());
                if last_chunk{
                    response.push(Some(recombined_value.clone()));
                    recombined_value.clear();
                }
            }
        }
        Ok(response)
    }

    async fn find_keys_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Vec<Vec<u8>>, ServiceContextError> {
        ensure!(
            key_prefix.len() <= MAX_KEY_SIZE,
            ServiceContextError::KeyTooLong
        );
        let mut full_key_prefix = self.namespace.clone();
        full_key_prefix.extend(key_prefix);
        let query = RequestFindKeysByPrefix {
            key_prefix: full_key_prefix,
        };
        let request = tonic::Request::new(query);
        let mut client = self.client.write().await;
        let _guard = self.acquire().await;
        let mut response_stream = client
            .process_find_keys_by_prefix(request)
            .await?.
            into_inner();
        let mut  response = Vec::new();
        while let Some(ReplyFindKeysByPrefix { keys }) = response_stream.message().await?{
            for key in keys{
                response.push(key);
            }
        }
        Ok(response)
    }

    async fn find_key_values_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, ServiceContextError> {
        ensure!(
            key_prefix.len() <= MAX_KEY_SIZE,
            ServiceContextError::KeyTooLong
        );
        let mut full_key_prefix = self.namespace.clone();
        full_key_prefix.extend(key_prefix);
        let query = RequestFindKeyValuesByPrefix {
            key_prefix: full_key_prefix,
        };
        let request = tonic::Request::new(query);
        let mut client = self.client.write().await;
        let _guard = self.acquire().await;
        let mut response_stream = client
            .process_find_key_values_by_prefix(request)
            .await?
            .into_inner();
        let mut response = Vec::new();
        let mut num_of_value_blocks = 0;
        while let Some(ReplyFindKeyValuesByPrefix { mut key_values }) = response_stream.message().await?  {
            for i in 0..key_values.len(){
                if num_of_value_blocks == 0{
                    let num_of_value_blocks_bytes : [u8;4] = <[u8;4]>::try_from(key_values[i]
                        .value
                        .drain(0..4)
                        .as_slice())
                        .unwrap();
                    num_of_value_blocks = u32::from_be_bytes(num_of_value_blocks_bytes);
                    response.push((key_values[i].key.clone(),key_values[i].value.clone()));
                }else{
                    let mut recombined_key_value = response.pop().unwrap();
                    recombined_key_value.1.append(&mut key_values[i].value);
                    response.push(recombined_key_value);
                }
                num_of_value_blocks-=1;
            }
        }
        Ok(response)
    }
}

impl WritableKeyValueStore<ServiceContextError> for ServiceStoreClient {
    const MAX_VALUE_SIZE: usize = usize::MAX;

    async fn write_batch(&self, batch: Batch, _base_key: &[u8]) -> Result<(), ServiceContextError> {
        let mut chunk_of_statements = Vec::new();
        let mut chunk_size = 0;
        let mut statments_query:Vec<RequestWriteBatch> = Vec::new();
        let mut operations = Vec::new();
        for operation in batch.operations{
            operations.push(self.get_statement(operation));
        }

        for mut statement in operations{
            let (key_len,value_len) = match &mut statement.operation{
                Some(Operation::Delete (key ))=>( key.len(),0),
                Some(Operation::DeletePrefix ( key_prefix ))=>( key_prefix.len(),0),
                Some(Operation::Put (KeyValue { key,  value } )) =>{
                    let mut v = [0u8,0,0,1].to_vec();
                    v.append(value);
                    *value = v;
                    ( key.len(),value.len())},
                None=>continue,
            };
            if key_len+value_len+chunk_size <= MAX_PAYLOAD_SIZE{
                statments_query.pop().unwrap_or_default();
                chunk_of_statements.push(statement);
                statments_query.push(RequestWriteBatch{statements:chunk_of_statements.clone()});
                chunk_size+=key_len+value_len;
            }else{
                if let Some(Operation::Put(KeyValue { key, value })) = statement.operation{
                    let mut values = value
                        .chunks(MAX_KEY_SIZE)
                        .map(|x|x.to_vec())
                        .collect::<Vec<_>>();
                    let num_of_chunks = (values.len() as u32)
                        .to_be_bytes()
                        .to_vec();
                    values[0].splice(0..4, num_of_chunks);
                    let mut keys = vec![vec![];values.len()];
                    keys[0] = key;

                    for i in 0..keys.len(){
                        if keys[i].len()+values[i].len()+chunk_size<=MAX_PAYLOAD_SIZE{
                            statments_query.pop();
                            chunk_of_statements.push(Statement { operation:Some(Operation::Put(KeyValue { key: keys[i].clone(), value: values[i].clone() }) )});
                            statments_query.push(RequestWriteBatch{statements:chunk_of_statements.clone()});
                            chunk_size+=keys[i].len()+values[i].len();
                        } else {
                            chunk_of_statements.clear();
                            chunk_size = 0;
                            chunk_of_statements.push(Statement { operation:Some(Operation::Put(KeyValue { key: keys[i].clone(), value: values[i].clone() }) )});
                            statments_query.push(RequestWriteBatch{statements:chunk_of_statements.clone()});
                            chunk_size+=keys[i].len()+values[i].len();
                        }

                    }
                    continue;
                }
                chunk_of_statements.clear();
                chunk_size = 0;
                chunk_of_statements.push(statement);
                statments_query.push(RequestWriteBatch{statements:chunk_of_statements.clone()});
                chunk_size+=key_len+value_len;
            }
        }
        let request = tokio_stream::iter(statments_query);
        let mut client = self.client.write().await;
        let _guard = self.acquire().await;
        let _response = client
            .process_write_batch(request)
            .await?
            .into_inner();

        Ok(())

    }

    async fn clear_journal(&self, _base_key: &[u8]) -> Result<(), ServiceContextError> {
        Ok(())
    }

}

impl KeyValueStore for ServiceStoreClient {
    type Error = ServiceContextError;
}

impl ServiceStoreClient {
    /// Obtains the semaphore lock on the database if needed.
    async fn acquire(&self) -> Option<SemaphoreGuard<'_>> {
        match &self.semaphore {
            None => None,
            Some(count) => Some(count.acquire().await),
        }
    }

    fn namespace_as_vec(namespace: &str) -> Result<Vec<u8>, ServiceContextError> {
        let mut key = vec![KeyTag::Key as u8];
        bcs::serialize_into(&mut key, namespace)?;
        Ok(key)
    }

    fn get_statement(&self, operation: WriteOperation) -> Statement {
        let operation = match operation {
            WriteOperation::Delete { key } => {
                let mut full_key = self.namespace.clone();
                full_key.extend(key);
                Operation::Delete(full_key)
            }
            WriteOperation::Put { key, value } => {
                let mut full_key = self.namespace.clone();
                full_key.extend(key);
                Operation::Put(KeyValue {
                    key: full_key,
                    value,
                })
            }
            WriteOperation::DeletePrefix { key_prefix } => {
                let mut full_key_prefix = self.namespace.clone();
                full_key_prefix.extend(key_prefix);
                Operation::DeletePrefix(full_key_prefix)
            }
        };
        Statement {
            operation: Some(operation),
        }
    }

}

impl AdminKeyValueStore for ServiceStoreClient {
    type Error = ServiceContextError;
    type Config = ServiceStoreConfig;

    async fn connect(config: &Self::Config, namespace: &str) -> Result<Self, ServiceContextError> {
        let endpoint = format!("http://{}", config.endpoint);
        let endpoint = Endpoint::from_shared(endpoint)?;
        let client = StoreProcessorClient::connect(endpoint).await?;
        let client = Arc::new(RwLock::new(client));
        let semaphore = config
            .common_config
            .max_concurrent_queries
            .map(|n| Arc::new(Semaphore::new(n)));
        let max_stream_queries = config.common_config.max_stream_queries;
        let namespace = Self::namespace_as_vec(namespace)?;
        Ok(ServiceStoreClient {
            client,
            semaphore,
            max_stream_queries,
            namespace,
        })
    }

    async fn list_all(config: &Self::Config) -> Result<Vec<String>, ServiceContextError> {
        let query = RequestListAll {};
        let request = tonic::Request::new(query);
        let endpoint = format!("http://{}", config.endpoint);
        let endpoint = Endpoint::from_shared(endpoint)?;
        let mut client = StoreProcessorClient::connect(endpoint).await?;
        let response = client.process_list_all(request).await?;
        let response = response.into_inner();
        let ReplyListAll { namespaces } = response;
        let namespaces = namespaces
            .into_iter()
            .map(|x| bcs::from_bytes(&x))
            .collect::<Result<_, _>>()?;
        Ok(namespaces)
    }

    async fn delete_all(config: &Self::Config) -> Result<(), ServiceContextError> {
        let query = RequestDeleteAll {};
        let request = tonic::Request::new(query);
        let endpoint = format!("http://{}", config.endpoint);
        let endpoint = Endpoint::from_shared(endpoint)?;
        let mut client = StoreProcessorClient::connect(endpoint).await?;
        let _response = client.process_delete_all(request).await?;
        Ok(())
    }

    async fn exists(config: &Self::Config, namespace: &str) -> Result<bool, ServiceContextError> {
        let namespace = bcs::to_bytes(namespace)?;
        let query = RequestExistsNamespace { namespace };
        let request = tonic::Request::new(query);
        let endpoint = format!("http://{}", config.endpoint);
        let endpoint = Endpoint::from_shared(endpoint)?;
        let mut client = StoreProcessorClient::connect(endpoint).await?;
        let response = client.process_exists_namespace(request).await?;
        let response = response.into_inner();
        let ReplyExistsNamespace { exists } = response;
        Ok(exists)
    }

    async fn create(config: &Self::Config, namespace: &str) -> Result<(), ServiceContextError> {
        let namespace = bcs::to_bytes(namespace)?;
        let query = RequestCreateNamespace { namespace };
        let request = tonic::Request::new(query);
        let endpoint = format!("http://{}", config.endpoint);
        let endpoint = Endpoint::from_shared(endpoint)?;
        let mut client = StoreProcessorClient::connect(endpoint).await?;
        let _response = client.process_create_namespace(request).await?;
        Ok(())
    }

    async fn delete(config: &Self::Config, namespace: &str) -> Result<(), ServiceContextError> {
        let namespace = bcs::to_bytes(namespace)?;
        let query = RequestDeleteNamespace { namespace };
        let request = tonic::Request::new(query);
        let endpoint = format!("http://{}", config.endpoint);
        let endpoint = Endpoint::from_shared(endpoint)?;
        let mut client = StoreProcessorClient::connect(endpoint).await?;
        let _response = client.process_delete_namespace(request).await?;
        Ok(())
    }
}

/// Creates the `CommonStoreConfig` for the `ServiceStoreClient`.
pub fn create_service_store_common_config() -> CommonStoreConfig {
    let max_stream_queries = 100;
    let cache_size = 10; // unused
    CommonStoreConfig {
        max_concurrent_queries: None,
        max_stream_queries,
        cache_size,
    }
}

/// Creates a `ServiceStoreConfig` from an endpoint.
pub fn service_config_from_endpoint(
    endpoint: &str,
) -> Result<ServiceStoreConfig, ServiceContextError> {
    let common_config = create_service_store_common_config();
    let endpoint = endpoint.to_string();
    Ok(ServiceStoreConfig {
        endpoint,
        common_config,
    })
}

/// Checks that endpoint is truly absent.
pub async fn storage_service_check_absence(endpoint: &str) -> Result<bool, ServiceContextError> {
    let endpoint = Endpoint::from_shared(endpoint.to_string())?;
    let result = StoreProcessorClient::connect(endpoint).await;
    Ok(result.is_err())
}

/// Checks whether an endpoint is valid or not.
pub async fn storage_service_check_validity(endpoint: &str) -> Result<(), ServiceContextError> {
    let config = service_config_from_endpoint(endpoint).unwrap();
    let namespace = "namespace";
    let store = ServiceStoreClient::connect(&config, namespace).await?;
    let _value = store.read_value_bytes(&[42]).await?;
    Ok(())
}

/// Creates a test store with an endpoint. The namespace is random.
#[cfg(with_testing)]
pub async fn create_service_test_store(
    endpoint: &str,
) -> Result<ServiceStoreClient, ServiceContextError> {
    let config = service_config_from_endpoint(endpoint).unwrap();
    let namespace = generate_test_namespace();
    ServiceStoreClient::connect(&config, &namespace).await
}


