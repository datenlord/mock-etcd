use anyhow::anyhow;
use futures::lock::Mutex;
use std::sync::Arc;
//use lockfree_cuckoohash::LockFreeCuckooHash;
use super::kv::KeyValue;
use super::rpc::{
    CompactionRequest, CompactionResponse, DeleteRangeRequest, DeleteRangeResponse, PutRequest,
    PutResponse, RangeRequest, RangeResponse, TxnRequest, TxnResponse,
};
use super::rpc_grpc::Kv;
use futures::prelude::*;
use grpcio::{RpcContext, RpcStatus, RpcStatusCode, UnarySink};
use log::error;
use protobuf::RepeatedField;
use std::collections::HashMap;

/// Format `anyhow::Error`
// TODO: refactor this
fn format_anyhow_error(error: &anyhow::Error) -> String {
    let err_msg_vec = error
        .chain()
        .map(std::string::ToString::to_string)
        .collect::<Vec<_>>();
    let mut err_msg = err_msg_vec.as_slice().join(", caused by: ");
    err_msg.push_str(&format!(", root cause: {}", error.root_cause()));
    err_msg
}

/// Send success `gRPC` response
fn success<R>(ctx: &RpcContext, sink: UnarySink<R>, r: R) {
    let f = sink
        .success(r)
        .map_err(move |e| error!("failed to send response, the error is: {:?}", e))
        .map(|_| ());
    ctx.spawn(f)
}

/// Send failure `gRPC` response
fn fail<R>(ctx: &RpcContext, sink: UnarySink<R>, rsc: RpcStatusCode, anyhow_err: &anyhow::Error) {
    debug_assert_ne!(
        rsc,
        RpcStatusCode::OK,
        "the input RpcStatusCode should not be OK"
    );
    let details = format_anyhow_error(anyhow_err);
    let rs = RpcStatus::new(rsc, Some(details));
    let f = sink
        .fail(rs)
        .map_err(move |e| error!("failed to send response, the error is: {:?}", e))
        .map(|_| ());
    ctx.spawn(f)
}

/// Mock Etcd Service
#[derive(Debug)]
pub struct MockEtcdService {
    /// hashmap to store key value
    //hashmap: LockFreeCuckooHash<Vec<u8>, KeyValue>,
    hashmap: Arc<Mutex<HashMap<Vec<u8>, KeyValue>>>,
}

impl Default for MockEtcdService {
    #[must_use]
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}
impl MockEtcdService {
    /// Create `MockEtcdService`
    #[must_use]
    #[inline]
    pub fn new() -> Self {
        Self {
            hashmap: Arc::new(Mutex::new(HashMap::<Vec<u8>, KeyValue>::new())),
        }
    }

    /// Get values of keys from a `RangeRequest`
    async fn get(&self, req: &RangeRequest) -> Vec<KeyValue> {
        let key = req.get_key().to_vec();
        let range_end = req.get_range_end().to_vec();
        let mut kvs = vec![];
        let hashmap = self.hashmap.lock().await;
        match range_end.as_slice() {
            [] => {
                if let Some(kv) = hashmap.get(&key) {
                    kvs.push(kv.clone());
                }
            }
            [0_u8] => {
                if key == vec![0_u8] {
                    hashmap.values().for_each(|v| kvs.push(v.clone()));
                }
            }
            _ => {
                hashmap.iter().for_each(|(k, v)| {
                    if k >= &key && k < &range_end {
                        kvs.push(v.clone())
                    }
                });
            }
        }
        kvs
    }

    /// Insert a key value from a `PutRequest`
    async fn insert(&mut self, req: &PutRequest) -> Option<KeyValue> {
        let mut kv = KeyValue::new();
        kv.set_key(req.get_key().to_vec());
        kv.set_value(req.get_value().to_vec());
        let mut hashmap = self.hashmap.lock().await;
        hashmap.insert(req.get_key().to_vec(), kv)
    }

    /// Delete keys from `DeleteRangeRequest`
    async fn delete(&mut self, req: &DeleteRangeRequest) -> Vec<KeyValue> {
        let key = req.get_key().to_vec();
        let range_end = req.get_range_end().to_vec();
        let mut prev_kvs = vec![];
        let mut hashmap = self.hashmap.lock().await;
        match range_end.as_slice() {
            [] => {
                if let Some(kv) = hashmap.remove(&key) {
                    prev_kvs.push(kv);
                }
            }
            [0_u8] => {
                if key == vec![0_u8] {
                    hashmap.values().for_each(|v| prev_kvs.push(v.clone()));
                    hashmap.clear()
                }
            }
            _ => {
                hashmap.retain(|k, v| {
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

impl Kv for MockEtcdService {
    fn range(&mut self, ctx: RpcContext, req: RangeRequest, sink: UnarySink<RangeResponse>) {
        let kvs = smol::run(async { self.get(&req).await });
        let mut response = RangeResponse::new();

        response.set_kvs(RepeatedField::from_vec(kvs));
        success(&ctx, sink, response)
    }

    fn put(&mut self, ctx: RpcContext, req: PutRequest, sink: UnarySink<PutResponse>) {
        let mut response = PutResponse::new();
        let prev = smol::run(async { self.insert(&req).await });
        if let Some(kv) = prev {
            response.set_prev_kv(kv);
        }
        success(&ctx, sink, response)
    }
    fn delete_range(
        &mut self,
        ctx: RpcContext,
        req: DeleteRangeRequest,
        sink: UnarySink<DeleteRangeResponse>,
    ) {
        let mut response = DeleteRangeResponse::new();
        let prev_kvs = smol::run(async { self.delete(&req).await });
        if req.get_prev_kv() {
            response.set_prev_kvs(RepeatedField::from_vec(prev_kvs));
        }
        success(&ctx, sink, response)
    }

    fn txn(&mut self, ctx: RpcContext, _req: TxnRequest, sink: UnarySink<TxnResponse>) {
        fail(
            &ctx,
            sink,
            RpcStatusCode::UNIMPLEMENTED,
            &anyhow!("Not Implemented"),
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
            &anyhow!("Not Implemented"),
        )
    }
}

#[cfg(test)]
#[allow(clippy::all, clippy::restriction)]
mod test {
    use super::*;
    #[test]
    fn test_all() {
        let mut mock_etcd_service = MockEtcdService::new();
        // Test insert
        let mut put000 = PutRequest::new();
        let mut put001 = PutRequest::new();
        let mut put010 = PutRequest::new();
        let mut put011 = PutRequest::new();
        let mut put100 = PutRequest::new();
        let mut put101 = PutRequest::new();
        let mut put110 = PutRequest::new();
        let mut put111 = PutRequest::new();
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
            smol::run(async { mock_etcd_service.insert(&put000).await }),
            None
        );
        assert_eq!(
            smol::run(async { mock_etcd_service.insert(&put001).await }),
            None
        );
        assert_eq!(
            smol::run(async { mock_etcd_service.insert(&put010).await }),
            None
        );
        assert_eq!(
            smol::run(async { mock_etcd_service.insert(&put011).await }),
            None
        );
        assert_eq!(
            smol::run(async { mock_etcd_service.insert(&put100).await }),
            None
        );
        assert_eq!(
            smol::run(async { mock_etcd_service.insert(&put101).await }),
            None
        );
        assert_eq!(
            smol::run(async { mock_etcd_service.insert(&put110).await }),
            None
        );
        assert_eq!(
            smol::run(async { mock_etcd_service.insert(&put111).await }),
            None
        );
        assert_eq!(
            smol::run(async {
                let kv = mock_etcd_service.insert(&put000).await;
                kv.unwrap().get_value().to_owned()
            }),
            vec![0_u8, 0_u8, 0_u8]
        );
        // Test get
        // get one key
        let mut one_key_1 = RangeRequest::new();
        one_key_1.set_key(vec![0_u8]);
        one_key_1.set_range_end(vec![]);
        let mut one_key_2 = RangeRequest::new();
        one_key_2.set_key(vec![0_u8, 0_u8, 0_u8]);
        one_key_2.set_range_end(vec![]);
        // get all keys
        let mut all_keys = RangeRequest::new();
        all_keys.set_key(vec![0_u8]);
        all_keys.set_range_end(vec![0_u8]);
        // get range
        let mut range1 = RangeRequest::new();
        range1.set_key(vec![0_u8, 0_u8, 0_u8]);
        range1.set_range_end(vec![0_u8, 1_u8, 0_u8]);
        let mut range2 = RangeRequest::new();
        range2.set_key(vec![0_u8, 0_u8, 0_u8]);
        range2.set_range_end(vec![1_u8, 1_u8, 1_u8]);
        let mut range2 = RangeRequest::new();
        range2.set_key(vec![0_u8, 1_u8, 1_u8]);
        range2.set_range_end(vec![1_u8, 1_u8, 1_u8]);

        assert_eq!(
            smol::run(async { mock_etcd_service.get(&one_key_1).await }),
            vec![]
        );
        assert_eq!(
            smol::run(async {
                let kv = mock_etcd_service.get(&one_key_2).await;
                kv.get(0).unwrap().get_value().to_owned()
            }),
            vec![0_u8, 0_u8, 0_u8]
        );
        assert_eq!(
            smol::run(async { mock_etcd_service.get(&all_keys).await.len() }),
            8
        );
        assert_eq!(
            smol::run(async { mock_etcd_service.get(&range1).await.len() }),
            2
        );
        assert_eq!(
            smol::run(async { mock_etcd_service.get(&range2).await.len() }),
            4
        );

        // Test delete
        let mut delete_no_exist = DeleteRangeRequest::new();
        delete_no_exist.set_key(vec![0_u8]);
        delete_no_exist.set_range_end(vec![]);

        let mut delete_one_key = DeleteRangeRequest::new();
        delete_one_key.set_key(vec![1_u8, 1_u8, 1_u8]);
        delete_one_key.set_range_end(vec![]);
        // delete range
        let mut delete_range = DeleteRangeRequest::new();
        delete_range.set_key(vec![0_u8, 0_u8, 0_u8]);
        delete_range.set_range_end(vec![0_u8, 1_u8, 0_u8]);
        // delete all
        let mut delete_all = DeleteRangeRequest::new();
        delete_all.set_key(vec![0_u8]);
        delete_all.set_range_end(vec![0_u8]);

        assert_eq!(
            smol::run(async { mock_etcd_service.delete(&delete_no_exist).await.len() }),
            0
        );
        assert_eq!(
            smol::run(async {
                let kv = mock_etcd_service.delete(&delete_one_key).await;
                kv.get(0).unwrap().get_value().to_owned()
            }),
            vec![1_u8, 1_u8, 1_u8]
        );
        assert_eq!(
            smol::run(async { mock_etcd_service.hashmap.lock().await.len() }),
            7
        );
        assert_eq!(
            smol::run(async { mock_etcd_service.delete(&delete_range).await.len() }),
            2
        );
        assert_eq!(
            smol::run(async { mock_etcd_service.hashmap.lock().await.len() }),
            5
        );
        assert_eq!(
            smol::run(async { mock_etcd_service.delete(&delete_all).await.len() }),
            5
        );
        assert_eq!(
            smol::run(async { mock_etcd_service.hashmap.lock().await.len() }),
            0
        );
    }
}
