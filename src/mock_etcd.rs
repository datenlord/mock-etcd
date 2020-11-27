//! The implementation for Mock Etcd

use super::etcd::{
    CompactionRequest, CompactionResponse, DeleteRangeRequest, DeleteRangeResponse, PutRequest,
    PutResponse, RangeRequest, RangeResponse, TxnRequest, TxnResponse,
};
use super::etcd_grpc::{create_kv, Kv};
use super::kv::KeyValue;
use async_lock::RwLock;
use futures::future::TryFutureExt;
use futures::prelude::*;
use grpcio::{Environment, RpcContext, RpcStatus, RpcStatusCode, Server, ServerBuilder, UnarySink};
use log::{debug, error};
use protobuf::RepeatedField;
use std::collections::HashMap;
use std::sync::Arc;
use utilities::Cast;

/// Help function to send success `gRPC` response
async fn success<R: Send>(response: R, sink: UnarySink<R>) {
    sink.success(response)
        .map_err(|e| error!("failed to send response, the error is: {:?}", e))
        .map(|_| ())
        .await
}

/// Send failure `gRPC` response
fn fail<R>(ctx: &RpcContext, sink: UnarySink<R>, rsc: RpcStatusCode, details: String) {
    debug_assert_ne!(
        rsc,
        RpcStatusCode::OK,
        "the input RpcStatusCode should not be OK"
    );
    let rs = RpcStatus::new(rsc, Some(details));
    let f = sink
        .fail(rs)
        .map_err(|e| error!("failed to send response, the error is: {:?}", e))
        .map(|_| ());
    ctx.spawn(f)
}

impl Default for MockEtcdServer {
    #[must_use]
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

/// Mock Etcd Server
#[derive(Debug)]
pub struct MockEtcdServer {
    /// grpc server
    server: Server,
}

impl MockEtcdServer {
    /// Create `MockEtcdServer`
    #[must_use]
    #[inline]
    pub fn new() -> Self {
        let etcd_service = create_kv(MockEtcd::new());
        Self {
            server: ServerBuilder::new(Arc::new(Environment::new(1)))
                .register_service(etcd_service)
                .bind("127.0.0.1", 2379)
                .build()
                .unwrap_or_else(|e| panic!("failed to build etcd server, the error is: {:?}", e)),
        }
    }

    /// Start Mock Etcd Server
    #[inline]
    pub fn start(&mut self) {
        self.server.start();
    }
}

/// Mock Etcd
#[derive(Debug, Clone)]
struct MockEtcd {
    /// map to store key value
    map: Arc<RwLock<HashMap<Vec<u8>, KeyValue>>>,
}

/// Range end to get all keys
const ALL_KEYS: &[u8] = &[0_u8];
/// Range end to get one key
const ONE_KEY: &[u8] = &[];

impl MockEtcd {
    /// Create `MockEtcd`
    fn new() -> Self {
        Self {
            map: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Get values of keys from a `RangeRequest` to map
    #[allow(clippy::pattern_type_mismatch)]
    async fn map_get(
        map_arc: Arc<RwLock<HashMap<Vec<u8>, KeyValue>>>,
        req: RangeRequest,
    ) -> Vec<KeyValue> {
        let key = req.get_key().to_vec();
        let range_end = req.get_range_end().to_vec();
        let mut kvs = vec![];
        let map = map_arc.read().await;
        match range_end.as_slice() {
            ONE_KEY => {
                if let Some(kv) = map.get(&key) {
                    kvs.push(kv.clone());
                }
            }
            ALL_KEYS => {
                if key == vec![0_u8] {
                    map.values().for_each(|v| kvs.push(v.clone()));
                }
            }
            _ => {
                map.iter().for_each(|(k, v)| {
                    if k >= &key && k < &range_end {
                        kvs.push(v.clone())
                    }
                });
            }
        }
        kvs
    }

    /// Insert a key value from a `PutRequest` to map
    async fn map_insert(
        map_arc: Arc<RwLock<HashMap<Vec<u8>, KeyValue>>>,
        req: PutRequest,
    ) -> Option<KeyValue> {
        let mut kv = KeyValue::new();
        kv.set_key(req.get_key().to_vec());
        kv.set_value(req.get_value().to_vec());
        let mut map = map_arc.write().await;
        map.insert(req.get_key().to_vec(), kv)
    }

    /// Delete keys from `DeleteRangeRequest` from map
    #[allow(clippy::pattern_type_mismatch)]
    async fn map_delete(
        map_arc: Arc<RwLock<HashMap<Vec<u8>, KeyValue>>>,
        req: DeleteRangeRequest,
    ) -> Vec<KeyValue> {
        let key = req.get_key().to_vec();
        let range_end = req.get_range_end().to_vec();
        let mut prev_kvs = vec![];
        let mut map = map_arc.write().await;
        match range_end.as_slice() {
            ONE_KEY => {
                if let Some(kv) = map.remove(&key) {
                    prev_kvs.push(kv);
                }
            }
            ALL_KEYS => {
                if key == vec![0_u8] {
                    map.values().for_each(|v| prev_kvs.push(v.clone()));
                    map.clear()
                }
            }
            _ => {
                map.retain(|k, v| {
                    if k >= &key && k < &range_end {
                        prev_kvs.push(v.clone());
                        false
                    } else {
                        true
                    }
                });
            }
        }
        prev_kvs
    }
}

impl Kv for MockEtcd {
    fn range(&mut self, ctx: RpcContext, req: RangeRequest, sink: UnarySink<RangeResponse>) {
        debug!(
            "Receive range request key={:?}, range_end={:?}",
            req.get_key(),
            req.get_range_end()
        );
        let map_arc = Arc::clone(&self.map);
        let task = async move {
            let kvs = Self::map_get(map_arc, req).await;
            let mut response = RangeResponse::new();
            response.set_count(kvs.len().cast());
            response.set_kvs(RepeatedField::from_vec(kvs));
            success(response, sink).await;
        };

        ctx.spawn(task)
    }

    fn put(&mut self, ctx: RpcContext, req: PutRequest, sink: UnarySink<PutResponse>) {
        debug!(
            "Receive put request key={:?}, value={:?}",
            req.get_key(),
            req.get_value()
        );
        let map_arc = Arc::clone(&self.map);
        let task = async move {
            let mut response = PutResponse::new();

            let prev = Self::map_insert(map_arc, req).await;
            if let Some(kv) = prev {
                response.set_prev_kv(kv);
            }
            success(response, sink).await;
        };
        ctx.spawn(task)
    }

    fn delete_range(
        &mut self,
        ctx: RpcContext,
        req: DeleteRangeRequest,
        sink: UnarySink<DeleteRangeResponse>,
    ) {
        debug!(
            "Receive delete range request key={:?}, range_end={:?}",
            req.get_key(),
            req.get_range_end()
        );

        let map_arc = Arc::clone(&self.map);
        let task = async move {
            let mut response = DeleteRangeResponse::new();
            let get_prev = req.get_prev_kv();
            let prev_kvs = Self::map_delete(map_arc, req).await;
            response.set_deleted(prev_kvs.len().cast());
            if get_prev {
                response.set_prev_kvs(RepeatedField::from_vec(prev_kvs));
            }
            success(response, sink).await;
        };
        ctx.spawn(task)
    }

    fn txn(&mut self, ctx: RpcContext, _req: TxnRequest, sink: UnarySink<TxnResponse>) {
        fail(
            &ctx,
            sink,
            RpcStatusCode::UNIMPLEMENTED,
            "Not Implemented".to_string(),
        )
    }

    fn compact(
        &mut self,
        ctx: RpcContext,
        _req: CompactionRequest,
        sink: UnarySink<CompactionResponse>,
    ) {
        fail(
            &ctx,
            sink,
            RpcStatusCode::UNIMPLEMENTED,
            "Not Implemented".to_string(),
        )
    }
}

#[cfg(test)]
#[allow(clippy::all, clippy::restriction)]
#[allow(clippy::too_many_lines)]
mod test {
    use crate::mock_etcd::{MockEtcd, MockEtcdServer};
    use async_compat::Compat;
    use etcd_rs::{Client, ClientConfig, DeleteRequest, KeyRange, PutRequest, RangeRequest};
    use std::sync::Arc;
    #[test]
    fn test_all() {
        unit_test();
        e2e_test();
    }
    fn unit_test() {
        smol::future::block_on(async {
            let mock_etcd = MockEtcd::new();
            // Test insert
            let mut put000 = crate::etcd::PutRequest::new();
            let mut put001 = crate::etcd::PutRequest::new();
            let mut put010 = crate::etcd::PutRequest::new();
            let mut put011 = crate::etcd::PutRequest::new();
            let mut put100 = crate::etcd::PutRequest::new();
            let mut put101 = crate::etcd::PutRequest::new();
            let mut put110 = crate::etcd::PutRequest::new();
            let mut put111 = crate::etcd::PutRequest::new();
            put000.set_key(vec![0_u8, 0_u8, 0_u8]);
            put001.set_key(vec![0_u8, 0_u8, 1_u8]);
            put010.set_key(vec![0_u8, 1_u8, 0_u8]);
            put011.set_key(vec![0_u8, 1_u8, 1_u8]);
            put100.set_key(vec![1_u8, 0_u8, 0_u8]);
            put101.set_key(vec![1_u8, 0_u8, 1_u8]);
            put110.set_key(vec![1_u8, 1_u8, 0_u8]);
            put111.set_key(vec![1_u8, 1_u8, 1_u8]);
            put000.set_value(vec![0_u8, 0_u8, 0_u8]);
            put001.set_value(vec![0_u8, 0_u8, 1_u8]);
            put010.set_value(vec![0_u8, 1_u8, 0_u8]);
            put011.set_value(vec![0_u8, 1_u8, 1_u8]);
            put100.set_value(vec![1_u8, 0_u8, 0_u8]);
            put101.set_value(vec![1_u8, 0_u8, 1_u8]);
            put110.set_value(vec![1_u8, 1_u8, 0_u8]);
            put111.set_value(vec![1_u8, 1_u8, 1_u8]);
            assert_eq!(
                MockEtcd::map_insert(Arc::clone(&mock_etcd.map), put000.clone()).await,
                None
            );
            assert_eq!(
                MockEtcd::map_insert(Arc::clone(&mock_etcd.map), put001.clone()).await,
                None
            );
            assert_eq!(
                MockEtcd::map_insert(Arc::clone(&mock_etcd.map), put010.clone()).await,
                None
            );
            assert_eq!(
                MockEtcd::map_insert(Arc::clone(&mock_etcd.map), put011.clone()).await,
                None
            );
            assert_eq!(
                MockEtcd::map_insert(Arc::clone(&mock_etcd.map), put100.clone()).await,
                None
            );
            assert_eq!(
                MockEtcd::map_insert(Arc::clone(&mock_etcd.map), put101.clone()).await,
                None
            );
            assert_eq!(
                MockEtcd::map_insert(Arc::clone(&mock_etcd.map), put110.clone()).await,
                None
            );
            assert_eq!(
                MockEtcd::map_insert(Arc::clone(&mock_etcd.map), put111.clone()).await,
                None
            );
            assert_eq!(
                {
                    let kv = MockEtcd::map_insert(Arc::clone(&mock_etcd.map), put000.clone()).await;
                    kv.unwrap().get_value().to_owned()
                },
                vec![0_u8, 0_u8, 0_u8]
            );
            // Test get
            // get one key
            let mut one_key_1 = crate::etcd::RangeRequest::new();
            one_key_1.set_key(vec![0_u8]);
            one_key_1.set_range_end(vec![]);
            let mut one_key_2 = crate::etcd::RangeRequest::new();
            one_key_2.set_key(vec![0_u8, 0_u8, 0_u8]);
            one_key_2.set_range_end(vec![]);
            // get all keys
            let mut all_keys = crate::etcd::RangeRequest::new();
            all_keys.set_key(vec![0_u8]);
            all_keys.set_range_end(vec![0_u8]);
            // get range
            let mut range1 = crate::etcd::RangeRequest::new();
            range1.set_key(vec![0_u8, 0_u8, 0_u8]);
            range1.set_range_end(vec![0_u8, 1_u8, 0_u8]);
            let mut range2 = crate::etcd::RangeRequest::new();
            range2.set_key(vec![0_u8, 0_u8, 0_u8]);
            range2.set_range_end(vec![1_u8, 1_u8, 1_u8]);
            let mut range2 = crate::etcd::RangeRequest::new();
            range2.set_key(vec![0_u8, 1_u8, 1_u8]);
            range2.set_range_end(vec![1_u8, 1_u8, 1_u8]);

            assert_eq!(
                MockEtcd::map_get(Arc::clone(&mock_etcd.map), one_key_1.clone()).await,
                vec![]
            );
            assert_eq!(
                {
                    let kv = MockEtcd::map_get(Arc::clone(&mock_etcd.map), one_key_2.clone()).await;
                    kv.get(0).unwrap().get_value().to_owned()
                },
                vec![0_u8, 0_u8, 0_u8]
            );
            assert_eq!(
                MockEtcd::map_get(Arc::clone(&mock_etcd.map), all_keys.clone())
                    .await
                    .len(),
                8
            );
            assert_eq!(
                MockEtcd::map_get(Arc::clone(&mock_etcd.map), range1.clone())
                    .await
                    .len(),
                2
            );
            assert_eq!(
                MockEtcd::map_get(Arc::clone(&mock_etcd.map), range2.clone())
                    .await
                    .len(),
                4
            );

            // Test delete
            let mut delete_no_exist = crate::etcd::DeleteRangeRequest::new();
            delete_no_exist.set_key(vec![0_u8]);
            delete_no_exist.set_range_end(vec![]);

            let mut delete_one_key = crate::etcd::DeleteRangeRequest::new();
            delete_one_key.set_key(vec![1_u8, 1_u8, 1_u8]);
            delete_one_key.set_range_end(vec![]);
            // delete range
            let mut delete_range = crate::etcd::DeleteRangeRequest::new();
            delete_range.set_key(vec![0_u8, 0_u8, 0_u8]);
            delete_range.set_range_end(vec![0_u8, 1_u8, 0_u8]);
            // delete all
            let mut delete_all = crate::etcd::DeleteRangeRequest::new();
            delete_all.set_key(vec![0_u8]);
            delete_all.set_range_end(vec![0_u8]);

            assert_eq!(
                MockEtcd::map_delete(Arc::clone(&mock_etcd.map), delete_no_exist.clone())
                    .await
                    .len(),
                0
            );
            assert_eq!(
                {
                    let kv =
                        MockEtcd::map_delete(Arc::clone(&mock_etcd.map), delete_one_key.clone())
                            .await;
                    kv.get(0).unwrap().get_value().to_owned()
                },
                vec![1_u8, 1_u8, 1_u8]
            );
            assert_eq!(mock_etcd.map.read().await.len(), 7);
            assert_eq!(
                MockEtcd::map_delete(Arc::clone(&mock_etcd.map), delete_range.clone())
                    .await
                    .len(),
                2
            );
            assert_eq!(mock_etcd.map.read().await.len(), 5);
            assert_eq!(
                MockEtcd::map_delete(Arc::clone(&mock_etcd.map), delete_all.clone())
                    .await
                    .len(),
                5
            );
            assert_eq!(mock_etcd.map.read().await.len(), 0);
        });
    }

    fn e2e_test() {
        let mut etcd_server = MockEtcdServer::new();
        etcd_server.start();

        smol::future::block_on(Compat::new(async {
            let endpoints = vec!["http://127.0.0.1:2379".to_owned()];
            let client = Client::connect(ClientConfig {
                endpoints,
                auth: None,
                tls: None,
            })
            .await
            .unwrap_or_else(|err| {
                panic!("failed to connect to etcd server, the error is: {}", err)
            });

            let key000 = vec![0_u8, 0_u8, 0_u8];
            let key001 = vec![0_u8, 0_u8, 1_u8];
            let key010 = vec![0_u8, 1_u8, 0_u8];
            let key011 = vec![0_u8, 1_u8, 1_u8];
            let key100 = vec![1_u8, 0_u8, 0_u8];
            let key101 = vec![1_u8, 0_u8, 1_u8];
            let key110 = vec![1_u8, 1_u8, 0_u8];
            let key111 = vec![1_u8, 1_u8, 1_u8];

            client
                .kv()
                .put(PutRequest::new(key000.clone(), key000.clone()))
                .await
                .unwrap_or_else(|err| {
                    panic!("failed to put key-value key000, the error is {}", err)
                });
            client
                .kv()
                .put(PutRequest::new(key001.clone(), key001.clone()))
                .await
                .unwrap_or_else(|err| {
                    panic!("failed to put key-value key001, the error is {}", err)
                });
            client
                .kv()
                .put(PutRequest::new(key010.clone(), key010.clone()))
                .await
                .unwrap_or_else(|err| {
                    panic!("failed to put key-value key010, the error is {}", err)
                });
            client
                .kv()
                .put(PutRequest::new(key011.clone(), key011.clone()))
                .await
                .unwrap_or_else(|err| {
                    panic!("failed to put key-value key011, the error is {}", err)
                });
            client
                .kv()
                .put(PutRequest::new(key100.clone(), key100.clone()))
                .await
                .unwrap_or_else(|err| {
                    panic!("failed to put key-value key100, the error is {}", err)
                });
            client
                .kv()
                .put(PutRequest::new(key101.clone(), key101.clone()))
                .await
                .unwrap_or_else(|err| {
                    panic!("failed to put key-value key101, the error is {}", err)
                });
            client
                .kv()
                .put(PutRequest::new(key110.clone(), key110.clone()))
                .await
                .unwrap_or_else(|err| {
                    panic!("failed to put key-value key110, the error is {}", err)
                });
            client
                .kv()
                .put(PutRequest::new(key111.clone(), key111.clone()))
                .await
                .unwrap_or_else(|err| {
                    panic!("failed to put key-value key111, the error is {}", err)
                });

            let resp = client
                .kv()
                .range(RangeRequest::new(KeyRange::key(vec![0_u8])))
                .await
                .unwrap_or_else(|err| panic!("failed to get key 0, the error is {}", err));
            assert_eq!(resp.count(), 0);
            let mut resp = client
                .kv()
                .range(RangeRequest::new(KeyRange::key(key000.clone())))
                .await
                .unwrap_or_else(|err| panic!("failed to get key 000, the error is {}", err));
            assert_eq!(resp.count(), 1);
            assert_eq!(resp.take_kvs().get(0).unwrap().value(), key000);

            let mut resp = client
                .kv()
                .range(RangeRequest::new(KeyRange::key(key111.clone())))
                .await
                .unwrap_or_else(|err| panic!("failed to get key 111, the error is {}", err));
            assert_eq!(resp.count(), 1);
            assert_eq!(resp.take_kvs().get(0).unwrap().value(), key111);

            let resp = client
                .kv()
                .range(RangeRequest::new(KeyRange::range(key000.clone(), key100)))
                .await
                .unwrap_or_else(|err| panic!("failed to get range 000-100, the error is {}", err));
            assert_eq!(resp.count(), 4);
            let resp = client
                .kv()
                .range(RangeRequest::new(KeyRange::all()))
                .await
                .unwrap_or_else(|err| panic!("failed to get range all, the error is {}", err));
            assert_eq!(resp.count(), 8);
            let resp = client
                .kv()
                .range(RangeRequest::new(KeyRange::prefix(vec![1_u8, 1_u8])))
                .await
                .unwrap_or_else(|err| panic!("failed to get prefix 11, the error is {}", err));
            assert_eq!(resp.count(), 2);

            let resp = client
                .kv()
                .delete(DeleteRequest::new(KeyRange::key(vec![0_u8])))
                .await
                .unwrap_or_else(|err| panic!("failed to delete key 0, the error is {}", err));
            assert_eq!(resp.count_deleted(), 0);

            let mut delete_req = DeleteRequest::new(KeyRange::key(key000.clone()));
            delete_req.set_prev_kv(true);
            let mut resp = client
                .kv()
                .delete(delete_req)
                .await
                .unwrap_or_else(|err| panic!("failed to delete key 000, the error is {}", err));
            assert_eq!(resp.take_prev_kvs().get(0).unwrap().value(), key000);

            let resp = client
                .kv()
                .range(RangeRequest::new(KeyRange::key(key000)))
                .await
                .unwrap_or_else(|err| panic!("failed to get key 000, the error is {}", err));
            assert_eq!(resp.count(), 0);

            let resp = client
                .kv()
                .delete(DeleteRequest::new(KeyRange::prefix(vec![1_u8, 1_u8])))
                .await
                .unwrap_or_else(|err| panic!("failed to delete prefix 11, the error is {}", err));
            assert_eq!(resp.count_deleted(), 2);

            let resp = client
                .kv()
                .range(RangeRequest::new(KeyRange::key(key111.clone())))
                .await
                .unwrap_or_else(|err| panic!("failed to get key 111, the error is {}", err));
            assert_eq!(resp.count(), 0);
        }));
    }
}
