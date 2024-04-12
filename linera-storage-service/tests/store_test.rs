// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_storage_service::{
    child::{get_free_port, StorageService},
    client::{create_service_test_store, service_config_from_endpoint, ServiceStoreClient},
};
use linera_views::{
    batch::Batch, common::{LocalReadableKeyValueStore, LocalWritableKeyValueStore}, test_utils::{self, admin_test, generate_random_batch, get_random_byte_vector, get_random_test_scenarios, realize_batch, run_reads, run_test_batch_from_blank, run_writes_from_blank, run_writes_from_state}
};
use proptest::prelude::Rng;

/// The endpoint used for the storage service tests.
#[cfg(test)]
fn get_storage_service_guard(endpoint: &str) -> StorageService {
    let binary = env!("CARGO_BIN_EXE_storage_service_server").to_string();
    StorageService::new(endpoint, binary)
}

#[tokio::test]
async fn test_reads_service_store() {
    let endpoint = get_free_port().await.unwrap();
    for scenario in get_random_test_scenarios() {
        let _guard = get_storage_service_guard(&endpoint).run().await;
        let key_value_store = create_service_test_store(&endpoint).await.unwrap();
        run_reads(key_value_store, scenario).await;
    }
}

#[tokio::test]
async fn test_service_store_writes_from_blank() {
    let endpoint = get_free_port().await.unwrap();
    let _guard = get_storage_service_guard(&endpoint).run().await;
    let key_value_store = create_service_test_store(&endpoint).await.unwrap();
    run_writes_from_blank(&key_value_store).await;
}

#[tokio::test]
async fn test_service_store_writes_from_state() {
    let endpoint = get_free_port().await.unwrap();
    let _guard = get_storage_service_guard(&endpoint).run().await;
    let key_value_store = create_service_test_store(&endpoint).await.unwrap();
    run_writes_from_state(&key_value_store).await;
}

#[tokio::test]
async fn test_service_admin() {
    let endpoint = get_free_port().await.unwrap();
    let _guard = get_storage_service_guard(&endpoint).run().await;
    let config = service_config_from_endpoint(&endpoint).expect("config");
    admin_test::<ServiceStoreClient>(&config).await;
}

#[tokio::test]
async fn test_service_big_raw_write() {
    let endpoint = get_free_port().await.unwrap();
    let _guard = get_storage_service_guard(&endpoint).run().await;
    let key_value_store = create_service_test_store(&endpoint).await.unwrap();

    let mut rng = test_utils::make_deterministic_rng();

    let batch: Batch = generate_random_batch(&mut rng, &[43], 50);
    let kv_state = realize_batch(&batch);
    let kvs = kv_state.into_iter().collect::<Vec<_>>();
    key_value_store.write_batch(batch.clone(), &[]).await;

    run_test_batch_from_blank(&key_value_store, vec![], batch).await;
    run_reads(key_value_store, kvs).await;

}
