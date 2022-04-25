//! ChiselStore RPC module.
use crate::rpc::proto::rpc_server::Rpc;
use crate::{StoreCommand, StoreServer, StoreTransport};
use async_mutex::Mutex;
use async_trait::async_trait;
use crossbeam::queue::ArrayQueue;
use derivative::Derivative;

// ---------------------------------------------------------------
// Importing OmniPaxos messages
// ---------------------------------------------------------------
use omnipaxos_core::messages::{
    AcceptDecide, AcceptStopSign, AcceptSync, Accepted, 
    AcceptedStopSign, Decide, DecideStopSign, FirstAccept, 
    Message, Prepare, Promise, Compaction, PaxosMsg
};
use omnipaxos_core::ballot_leader_election::messages::{
    BLEMessage, HeartbeatMsg, 
    HeartbeatRequest, HeartbeatReply
};
use omnipaxos_core::ballot_leader_election::Ballot;
use omnipaxos_core::util::SyncItem;
use omnipaxos_core::storage::{SnapshotType, StopSign};

use std::collections::HashMap;
use std::sync::Arc;
use std::marker::PhantomData;
use tonic::{Request, Response, Status};

// ---------------------------------------------------------------
// Proto stuff
// ---------------------------------------------------------------

#[allow(missing_docs)]
pub mod proto {
    tonic::include_proto!("proto");
}

use proto::rpc_client::RpcClient;

// Importing all from proto
use crate::rpc::proto::*; 
type NodeAddrFn = dyn Fn(usize) -> String + Send + Sync;

// ---------------------------------------------------------------
// Chisel store connection stuff 
// ---------------------------------------------------------------
#[derive(Debug)]
struct ConnectionPool {
    connections: ArrayQueue<RpcClient<tonic::transport::Channel>>,
}

struct Connection {
    conn: RpcClient<tonic::transport::Channel>,
    pool: Arc<ConnectionPool>,
}

impl Drop for Connection {
    fn drop(&mut self) {
        self.pool.replenish(self.conn.clone())
    }
}

impl ConnectionPool {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            connections: ArrayQueue::new(16),
        })
    }

    async fn connection<S: ToString>(&self, addr: S) -> RpcClient<tonic::transport::Channel> {
        let addr = addr.to_string();
        match self.connections.pop() {
            Some(x) => x,
            None => RpcClient::connect(addr).await.unwrap(),
        }
    }

    fn replenish(&self, conn: RpcClient<tonic::transport::Channel>) {
        let _ = self.connections.push(conn);
    }
}

#[derive(Debug, Clone)]
struct Connections(Arc<Mutex<HashMap<String, Arc<ConnectionPool>>>>);

impl Connections {
    fn new() -> Self {
        Self(Arc::new(Mutex::new(HashMap::new())))
    }

    async fn connection<S: ToString>(&self, addr: S) -> Connection {
        let mut conns = self.0.lock().await;
        let addr = addr.to_string();
        let pool = conns
            .entry(addr.clone())
            .or_insert_with(ConnectionPool::new);
        Connection {
            conn: pool.connection(addr).await,
            pool: pool.clone(),
        }
    }
}

/// RPC transport.
#[derive(Derivative)]
#[derivative(Debug)]
pub struct RpcTransport {
    /// Node address mapping function.
    #[derivative(Debug = "ignore")]
    node_addr: Box<NodeAddrFn>,
    connections: Connections,
}

impl RpcTransport {
    /// Creates a new RPC transport.
    pub fn new(node_addr: Box<NodeAddrFn>) -> Self {
        RpcTransport {
            node_addr,
            connections: Connections::new(),
        }
    }
}

// ---------------------------------------------------------------
// Handling an OmniPaxos message OR BleNessage sent from the server
// ---------------------------------------------------------------

#[async_trait]
impl StoreTransport for RpcTransport {
    fn send_sp_to_rpc(&self, to_id: u64, msg: Message<StoreCommand, ()>) {

        // Save converted message 
        let message = convert_sp_to_rpc(msg.clone());
        let peer = (self.node_addr)(to_id as usize);
        let pool = self.connections.clone();

        // Establish connection with peers
        // Send the message as a request to peers,
        // Wait for the peers to respond
        // Then send responds to server layer (spmessage)
        tokio::task::spawn(async move {
            let mut client = pool.connection(peer).await;
            let request = tonic::Request::new(message);
            client.conn.sp_message(request).await.unwrap();
        });
    }

    fn send_ble_to_rpc(&self, to_id: u64, msg: BLEMessage) {
        let message = convert_ble_to_rpc(msg.clone());
        let peer = (self.node_addr)(to_id as usize);
        let pool = self.connections.clone();

        tokio::task::spawn(async move {
            let mut client = pool.connection(peer).await;
            let request = tonic::Request::new(message);
            client.conn.ble_message(request).await.unwrap();
        });
    }
    
}

// ---------------------------------------------------------------
// Matching OmniPaxos messages with Rpc messages
// E.g. OmniPaxos Ballot (as defined in omnipaxos_core/src/messages.rs) -> 
// Rpc Ballot (as defined in proto.proto)
// Those functions will support the pattern matching done in convert_sp_to_rpc
// ---------------------------------------------------------------

fn ballot_to_rpc(ballot: Ballot) -> RpcBallot {
    RpcBallot {
        n: ballot.n,
        priority: ballot.priority,
        pid: ballot.pid,
    }
}

fn prepare_to_rpc(prepare: Prepare) -> RpcPrepare {
    RpcPrepare {
        n: Some(ballot_to_rpc(prepare.n)),
        ld: prepare.ld,
        n_accepted: Some(ballot_to_rpc(prepare.n_accepted)),
        la: prepare.la,
    }
}

fn store_command_to_rpc(cmd: StoreCommand) -> RpcStoreCommand {
    RpcStoreCommand {
        id: cmd.id as u64,
        sql: cmd.sql.clone()
    }
}

fn sync_item_to_rpc(sync_item: SyncItem<StoreCommand, ()>) -> RpcSyncItem {
    RpcSyncItem {
        item: Some(match sync_item {
            SyncItem::Entries(vec) => rpc_sync_item::Item::Entries(rpc_sync_item::Entries { vec: vec.into_iter().map(|e| store_command_to_rpc(e)).collect() }),
            SyncItem::Snapshot(ss) => match ss {
                SnapshotType::Complete(_) => rpc_sync_item::Item::Snapshot(rpc_sync_item::Snapshot::Complete as i32),
                SnapshotType::Delta(_) => rpc_sync_item::Item::Snapshot(rpc_sync_item::Snapshot::Delta as i32),
                SnapshotType::_Phantom(_) => rpc_sync_item::Item::Snapshot(rpc_sync_item::Snapshot::Phantom as i32),
            },
            SyncItem::None => rpc_sync_item::Item::None(rpc_sync_item::None {}),
        }),
    }
}

fn stop_sign_to_rpc(stop_sign: StopSign) -> RpcStopSign {
    RpcStopSign {
        config_id: stop_sign.config_id,
        nodes: stop_sign.nodes,
        metadata: match stop_sign.metadata {
            Some(vec) => Some(rpc_stop_sign::Metadata { vec: vec.into_iter().map(|m| m as u32).collect() }),
            None => None,
        }
    }
}

fn promise_to_rpc(promise: Promise<StoreCommand, ()>) -> RpcPromise {
    RpcPromise {
        n: Some(ballot_to_rpc(promise.n)),
        n_accepted: Some(ballot_to_rpc(promise.n_accepted)),
        sync_item: match promise.sync_item {
            Some(s) => Some(sync_item_to_rpc(s)),
            None => None,
        },
        ld: promise.ld,
        la: promise.la,
        stopsign: match promise.stopsign {
            Some(ss) => Some(stop_sign_to_rpc(ss)),
            None => None,
        },
    }
}

fn accept_sync_to_rpc(accept_sync: AcceptSync<StoreCommand, ()>) -> RpcAcceptSync {
    RpcAcceptSync {
        n: Some(ballot_to_rpc(accept_sync.n)),
        sync_item: Some(sync_item_to_rpc(accept_sync.sync_item)),
        sync_idx: accept_sync.sync_idx,
        decide_idx: accept_sync.decide_idx,
        stopsign: match accept_sync.stopsign {
            Some(ss) => Some(stop_sign_to_rpc(ss)),
            None => None,
        },
    }
}

fn first_accept_to_rpc(first_accept: FirstAccept<StoreCommand>) -> RpcFirstAccept {
    RpcFirstAccept {
        n: Some(ballot_to_rpc(first_accept.n)),
        entries: first_accept.entries.into_iter().map(|e| store_command_to_rpc(e)).collect(),
    }
}

fn accept_decide_to_rpc(accept_decide: AcceptDecide<StoreCommand>) -> RpcAcceptDecide {
    RpcAcceptDecide {
        n: Some(ballot_to_rpc(accept_decide.n)),
        ld: accept_decide.ld,
        entries: accept_decide.entries.into_iter().map(|e| store_command_to_rpc(e)).collect(),
    }
}

fn accepted_to_rpc(accepted: Accepted) -> RpcAccepted {
    RpcAccepted {
        n: Some(ballot_to_rpc(accepted.n)),
        la: accepted.la,
    }
}

fn decide_to_rpc(decide: Decide) -> RpcDecide {
    RpcDecide {
        n: Some(ballot_to_rpc(decide.n)),
        ld: decide.ld,
    }
}

fn proposal_forward_to_rpc(proposals: Vec<StoreCommand>) -> RpcProposalForward {
    RpcProposalForward {
        entries: proposals.into_iter().map(|e| store_command_to_rpc(e)).collect(),
    }
}

fn compaction_to_rpc(compaction: Compaction) -> RpcCompaction {
    RpcCompaction {
        compaction: Some(match compaction {
            Compaction::Trim(trim) => rpc_compaction::Compaction::Trim(rpc_compaction::Trim { trim }),
            Compaction::Snapshot(ss) => rpc_compaction::Compaction::Snapshot(ss),
        }),
    }
}

fn accept_stop_sign_to_rpc(accept_stop_sign: AcceptStopSign) -> RpcAcceptStopSign {
    RpcAcceptStopSign {
        n: Some(ballot_to_rpc(accept_stop_sign.n)),
        ss: Some(stop_sign_to_rpc(accept_stop_sign.ss)),
    }
}

fn accepted_stop_sign_to_rpc(accepted_stop_sign: AcceptedStopSign) -> RpcAcceptedStopSign {
    RpcAcceptedStopSign {
        n: Some(ballot_to_rpc(accepted_stop_sign.n)),
    }
}

fn decide_stop_sign_to_rpc(decide_stop_sign: DecideStopSign) -> RpcDecideStopSign {
    RpcDecideStopSign {
        n: Some(ballot_to_rpc(decide_stop_sign.n)),
    }
}

fn heart_beat_request_to_rpc(heartbeat_request: HeartbeatRequest) -> RpcHeartbeatRequest {
    RpcHeartbeatRequest {
        round: heartbeat_request.round,
    }
}

fn heart_beat_reply_to_rpc(heartbeat_reply: HeartbeatReply) -> RpcHeartbeatReply {
    RpcHeartbeatReply {
        round: heartbeat_reply.round,
        ballot: Some(ballot_to_rpc(heartbeat_reply.ballot)),
        majority_connected: heartbeat_reply.majority_connected,
    }
}

// ---------------------------------------------------------------
// Matching OmniPaxos messages with Rpc messages
// Converts the message sent from server to Rpc-format
// This is for store transport
// ---------------------------------------------------------------

fn convert_sp_to_rpc(message: Message<StoreCommand, ()>) -> RpcPMessage {
    RpcPMessage {
        from: message.from,
        to: message.to,
        msg: Some(match message.msg {
            // Prepare req kanske spökar
            PaxosMsg::PrepareReq => rpc_p_message::Msg::RpcPrepareReq(RpcPrepareReq {}),
            PaxosMsg::Prepare(prepare) => rpc_p_message::Msg::RpcPrepare(prepare_to_rpc(prepare)),
            PaxosMsg::Promise(promise) => rpc_p_message::Msg::RpcPromise(promise_to_rpc(promise)),
            PaxosMsg::AcceptSync(accept_sync) => rpc_p_message::Msg::RpcAcceptSync(accept_sync_to_rpc(accept_sync)),
            PaxosMsg::FirstAccept(first_accept) => rpc_p_message::Msg::RpcFirstAccept(first_accept_to_rpc(first_accept)),
            PaxosMsg::AcceptDecide(accept_decide) => rpc_p_message::Msg::RpcAcceptDecide(accept_decide_to_rpc(accept_decide)),
            PaxosMsg::Accepted(accepted) => rpc_p_message::Msg::RpcAccepted(accepted_to_rpc(accepted)),
            PaxosMsg::Decide(decide) => rpc_p_message::Msg::RpcDecide(decide_to_rpc(decide)),
            PaxosMsg::ProposalForward(proposals) => rpc_p_message::Msg::RpcProposalForward(proposal_forward_to_rpc(proposals)),
            PaxosMsg::Compaction(compaction) => rpc_p_message::Msg::RpcCompaction(compaction_to_rpc(compaction)),
            PaxosMsg::ForwardCompaction(compaction) => rpc_p_message::Msg::RpcForwardCompaction(compaction_to_rpc(compaction)),
            PaxosMsg::AcceptStopSign(accept_stop_sign) => rpc_p_message::Msg::RpcAcceptStopSign(accept_stop_sign_to_rpc(accept_stop_sign)),
            PaxosMsg::AcceptedStopSign(accepted_stop_sign) => rpc_p_message::Msg::RpcAcceptedStopSign(accepted_stop_sign_to_rpc(accepted_stop_sign)),
            PaxosMsg::DecideStopSign(decide_stop_sign) => rpc_p_message::Msg::RpcDecideStopSign(decide_stop_sign_to_rpc(decide_stop_sign)),
        })
    }
}

// ---------------------------------------------------------------
// Matching Rpc messages with OmniPaxos messages
// Those functions will support the pattern matching done in convert_rpc_to_sp
// ---------------------------------------------------------------

fn convert_ble_to_rpc(message: BLEMessage) -> RpcBleMessage {
    RpcBleMessage {
        from: message.from,
        to: message.to,
        msg: Some(match message.msg {
            HeartbeatMsg::Request(request) => rpc_ble_message::Msg::HeartbeatReq(heart_beat_request_to_rpc(request)),
            HeartbeatMsg::Reply(reply) => rpc_ble_message::Msg::HeartbeatRep(heart_beat_reply_to_rpc(reply)),
        })
    }
}

fn ballot_to_sp(obj: RpcBallot) -> Ballot {
    Ballot {
        n: obj.n,
        priority: obj.priority,
        pid: obj.pid,
    }
}

fn prepare_to_sp(obj: RpcPrepare) -> Prepare {
    Prepare {
        n: ballot_to_sp(obj.n.unwrap()),
        ld: obj.ld,
        n_accepted: ballot_to_sp(obj.n_accepted.unwrap()),
        la: obj.la,
    }
}

fn store_command_to_sp(obj: RpcStoreCommand) -> StoreCommand {
    StoreCommand {
        id: obj.id as usize,
        sql: obj.sql.clone()
    }
}

fn sync_item_to_sp(obj: RpcSyncItem) -> SyncItem<StoreCommand, ()> {
    match obj.item.unwrap() {
        rpc_sync_item::Item::Entries(entries) => SyncItem::Entries(entries.vec.into_iter().map(|e| store_command_to_sp(e)).collect()),
        rpc_sync_item::Item::Snapshot(ss) => match rpc_sync_item::Snapshot::from_i32(ss) {
            Some(rpc_sync_item::Snapshot::Complete) => SyncItem::Snapshot(SnapshotType::Complete(())),
            Some(rpc_sync_item::Snapshot::Delta) => SyncItem::Snapshot(SnapshotType::Delta(())),
            Some(rpc_sync_item::Snapshot::Phantom) => SyncItem::Snapshot(SnapshotType::_Phantom(PhantomData)),
            _ => unimplemented!() 
        },
        rpc_sync_item::Item::None(_) => SyncItem::None,

    }
}

fn stop_sign_to_sp(obj: RpcStopSign) -> StopSign {
    StopSign {
        config_id: obj.config_id,
        nodes: obj.nodes,
        metadata: match obj.metadata {
            Some(md) => Some(md.vec.into_iter().map(|m| m as u8).collect()),
            None => None,
        },
    }
}

fn promise_to_sp(obj: RpcPromise) -> Promise<StoreCommand, ()> {
    Promise {
        n: ballot_to_sp(obj.n.unwrap()),
        n_accepted: ballot_to_sp(obj.n_accepted.unwrap()),
        sync_item: match obj.sync_item {
            Some(s) => Some(sync_item_to_sp(s)),
            None => None,
        },
        ld: obj.ld,
        la: obj.la,
        stopsign: match obj.stopsign {
            Some(ss) => Some(stop_sign_to_sp(ss)),
            None => None,
        },
    }
}

fn accept_sync_to_sp(obj: RpcAcceptSync) -> AcceptSync<StoreCommand, ()> {
    AcceptSync {
        n: ballot_to_sp(obj.n.unwrap()),
        sync_item: sync_item_to_sp(obj.sync_item.unwrap()),
        sync_idx: obj.sync_idx,
        decide_idx: obj.decide_idx,
        stopsign: match obj.stopsign {
            Some(ss) => Some(stop_sign_to_sp(ss)),
            None => None,
        },
    }
}

fn first_accept_to_sp(obj: RpcFirstAccept) -> FirstAccept<StoreCommand> {
    FirstAccept {
        n: ballot_to_sp(obj.n.unwrap()),
        entries: obj.entries.into_iter().map(|e| store_command_to_sp(e)).collect(),
    }
}

fn accept_decide_to_sp(obj: RpcAcceptDecide) -> AcceptDecide<StoreCommand> {
    AcceptDecide {
        n: ballot_to_sp(obj.n.unwrap()),
        ld: obj.ld,
        entries: obj.entries.into_iter().map(|e| store_command_to_sp(e)).collect(),
    }
}

fn accepted_to_sp(obj: RpcAccepted) -> Accepted {
    Accepted {
        n: ballot_to_sp(obj.n.unwrap()),
        la: obj.la,
    }
}

fn decide_to_sp(obj: RpcDecide) -> Decide {
    Decide {
        n: ballot_to_sp(obj.n.unwrap()),
        ld: obj.ld,
    }
}

fn proposal_forward_to_sp(obj: RpcProposalForward) -> Vec<StoreCommand> {
    obj.entries.into_iter().map(|e| store_command_to_sp(e)).collect()
}

fn compaction_to_sp(obj: RpcCompaction) -> Compaction {
    match obj.compaction.unwrap() {
        rpc_compaction::Compaction::Trim(trim) => Compaction::Trim(trim.trim),
        rpc_compaction::Compaction::Snapshot(ss) => Compaction::Snapshot(ss),
    }
}

fn accept_stop_sign_to_sp(obj: RpcAcceptStopSign) -> AcceptStopSign {
    AcceptStopSign {
        n: ballot_to_sp(obj.n.unwrap()),
        ss: stop_sign_to_sp(obj.ss.unwrap()),
    }
}

fn accepted_stop_sign_to_sp(obj: RpcAcceptedStopSign) -> AcceptedStopSign {
    AcceptedStopSign {
        n: ballot_to_sp(obj.n.unwrap()),
    }
}

fn decide_stop_sign_to_sp(obj: RpcDecideStopSign) -> DecideStopSign {
    DecideStopSign {
        n: ballot_to_sp(obj.n.unwrap()),
    }
}

fn heartbeat_request_to_sp(obj: RpcHeartbeatRequest) -> HeartbeatRequest {
    HeartbeatRequest {
        round: obj.round,
    }
}

fn heartbeat_reply_to_sp(obj: RpcHeartbeatReply) -> HeartbeatReply {
    HeartbeatReply {
        round: obj.round,
        ballot: ballot_to_sp(obj.ballot.unwrap()),
        majority_connected: obj.majority_connected,
    }
}

// ---------------------------------------------------------------
// Rpc -> sp conversion
// Done before sending a rpc reply back to the server.
// ---------------------------------------------------------------

fn convert_rpc_to_sp(obj: RpcPMessage) -> Message<StoreCommand, ()> {
    Message {
        from: obj.from,
        to: obj.to,
        msg: match obj.msg.unwrap() {
            rpc_p_message::Msg::RpcPrepareReq(_) => PaxosMsg::PrepareReq,
            rpc_p_message::Msg::RpcPrepare(prepare) => PaxosMsg::Prepare(prepare_to_sp(prepare)),
            rpc_p_message::Msg::RpcPromise(promise) => PaxosMsg::Promise(promise_to_sp(promise)),
            rpc_p_message::Msg::RpcAcceptSync(accept_sync) => PaxosMsg::AcceptSync(accept_sync_to_sp(accept_sync)),
            rpc_p_message::Msg::RpcFirstAccept(first_accept) => PaxosMsg::FirstAccept(first_accept_to_sp(first_accept)),
            rpc_p_message::Msg::RpcAcceptDecide(accept_decide) => PaxosMsg::AcceptDecide(accept_decide_to_sp(accept_decide)),
            rpc_p_message::Msg::RpcAccepted(accepted) => PaxosMsg::Accepted(accepted_to_sp(accepted)),
            rpc_p_message::Msg::RpcDecide(decide) => PaxosMsg::Decide(decide_to_sp(decide)),
            rpc_p_message::Msg::RpcProposalForward(proposals) => PaxosMsg::ProposalForward(proposal_forward_to_sp(proposals)),
            rpc_p_message::Msg::RpcCompaction(compaction) => PaxosMsg::Compaction(compaction_to_sp(compaction)),
            rpc_p_message::Msg::RpcForwardCompaction(compaction) => PaxosMsg::ForwardCompaction(compaction_to_sp(compaction)),
            rpc_p_message::Msg::RpcAcceptStopSign(accept_stop_sign) => PaxosMsg::AcceptStopSign(accept_stop_sign_to_sp(accept_stop_sign)),
            rpc_p_message::Msg::RpcAcceptedStopSign(accepted_stop_sign) => PaxosMsg::AcceptedStopSign(accepted_stop_sign_to_sp(accepted_stop_sign)),
            rpc_p_message::Msg::RpcDecideStopSign(decide_stop_sign) => PaxosMsg::DecideStopSign(decide_stop_sign_to_sp(decide_stop_sign)),
        }
    }
}

fn convert_rpc_to_ble(obj: RpcBleMessage) -> BLEMessage {
    BLEMessage {
        from: obj.from,
        to: obj.to,
        msg: match obj.msg.unwrap() {
            rpc_ble_message::Msg::HeartbeatReq(request) => HeartbeatMsg::Request(heartbeat_request_to_sp(request)),
            rpc_ble_message::Msg::HeartbeatRep(reply) => HeartbeatMsg::Reply(heartbeat_reply_to_sp(reply)),
        }
    }
}


/// RPC service.
#[derive(Debug)]
pub struct RpcService {
    /// The ChiselStore server access via this RPC service.
    pub server: Arc<StoreServer<RpcTransport>>,
}

impl RpcService {
    /// Creates a new RPC service.
    pub fn new(server: Arc<StoreServer<RpcTransport>>) -> Self {
        Self { server }
    }
}

// Implementation of the RPC service
#[tonic::async_trait]
impl Rpc for RpcService {
    async fn execute(
        &self,
        request: Request<Query>,
    ) -> Result<Response<QueryResults>, tonic::Status> {
        let query = request.into_inner();
        let server = self.server.clone();
        let results = match server.query(query.sql).await {
            Ok(results) => results,
            Err(e) => return Err(Status::internal(format!("{}", e))),
        };
        let mut rows = vec![];
        for row in results.rows {
            rows.push(QueryRow {
                values: row.values.clone(),
            })
        }
        Ok(Response::new(QueryResults { rows }))
    }

    // Pass sequence pacos msg (OmniPaxos style) to server
    async fn sp_message(&self, request: Request<RpcPMessage>) -> Result<Response<Void>, tonic::Status> {
        let message = convert_rpc_to_sp(request.into_inner());
        let server = self.server.clone();
        server.handle_sp_msg(message);
        Ok(Response::new(Void {}))
    }

    // Pass ble msg (OmniPaxos style) to server
    async fn ble_message(&self, request: Request<RpcBleMessage>) -> Result<Response<Void>, tonic::Status> {
        let message = convert_rpc_to_ble(request.into_inner());
        let server = self.server.clone();
        server.handle_ble_msg(message);
        Ok(Response::new(Void {}))
    }

}

// ---------------------------------------------------------------
// FIRST ATTEMPT: CRAZY PATTERN MATCHING
// TODO: REMOVE BELOW BEFORE FINAL COMMIT
// ---------------------------------------------------------------


/*
//! ChiselStore RPC module.

use crate::rpc::proto::rpc_server::Rpc;
use crate::{ Consistency, StoreCommand, StoreServer, StoreTransport}; //Consistency //KVSnapshot
use async_mutex::Mutex;
use async_trait::async_trait;
use crossbeam::queue::ArrayQueue;
use derivative::Derivative;

//fixa den här
//use little_raft::message::Message;

use omnipaxos_core::messages::{Message, PaxosMsg, AcceptDecide, AcceptStopSign, AcceptSync, Accepted, AcceptedStopSign, Compaction, Decide, DecideStopSign, ProposalForward, FirstAccept, Prepare, Promise};
use omnipaxos_core::ballot_leader_election::messages::{ BLEMessage, HeartbeatMsg, HeartbeatRequest, HeartbeatReply};
use omnipaxos_core::ballot_leader_election::Ballot;
use omnipaxos_core::util::SyncItem;
use omnipaxos_core::storage::SnapshotType;

use std::collections::HashMap;
use std::sync::Arc;
use tonic::{Request, Response, Status};

#[allow(missing_docs)]
pub mod proto {
    tonic::include_proto!("proto");
}

use proto::rpc_client::RpcClient;
use proto::{
    Query, QueryResults, QueryRow, Void, Entry, Entries, StopSign, 
    PPaxosMsg, PBallot, PPromise, PPrepare, PAcceptedStopSign, PAcceptDecide, PAcceptSync, PDecide, PAccepted, PSyncItem, PFirstAccept, PAcceptStopSign, PDecideStopSign,
    PCompaction, PProposalForward, PHeartbeatReply, PHeartbeatRequest, PBleMessage, PForwardCompaction, PSyncItemType, PSnapshotType,
    PSnapshotOneof,
};

//  RpcSnapshotType, RpcSyncItemType, RpcCompaction, RpcSnapshotTypeEnum, RpcPrepareReq
//crate::rpc::proto::p_paxos_msg::Message::*;
//use crate::rpc::proto::*;
use proto::p_paxos_msg::Message::{Prep, Prom, Accsync, Firstacc, Accdec, Acc, Dec, Proposalfwd, Comp, Fwdcomp, Acceptss, Acceptedss, Decidess};
use proto::p_sync_item::S::{ Syncentries, Syncsnapshot, Syncitemtype};
use proto::p_ble_message::Message::{ Hbrequest, Hbreply };


//use proto::rpc_sync_item::V:: {Storecommands, Snapshottype, Syncitemtype};
//use proto::rpc_ble_message::Message::{Heartbeatreply, Heartbeatrequest};


    /* Raft msgs from proto
        AppendEntriesRequest, AppendEntriesResponse, LogEntry, Query, QueryResults, QueryRow, Void,
    VoteRequest, VoteResponse,
    */
type NodeAddrFn = dyn Fn(usize) -> String + Send + Sync;

#[derive(Debug)]
struct ConnectionPool {
    connections: ArrayQueue<RpcClient<tonic::transport::Channel>>,
}

struct Connection {
    conn: RpcClient<tonic::transport::Channel>,
    pool: Arc<ConnectionPool>,
}

impl Drop for Connection {
    fn drop(&mut self) {
        self.pool.replenish(self.conn.clone())
    }
}

impl ConnectionPool {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            connections: ArrayQueue::new(16),
        })
    }

    async fn connection<S: ToString>(&self, addr: S) -> RpcClient<tonic::transport::Channel> {
        let addr = addr.to_string();
        match self.connections.pop() {
            Some(x) => x,
            None => RpcClient::connect(addr).await.unwrap(),
        }
    }

    fn replenish(&self, conn: RpcClient<tonic::transport::Channel>) {
        let _ = self.connections.push(conn);
    }
}

#[derive(Debug, Clone)]
struct Connections(Arc<Mutex<HashMap<String, Arc<ConnectionPool>>>>);

impl Connections {
    fn new() -> Self {
        Self(Arc::new(Mutex::new(HashMap::new())))
    }

    async fn connection<S: ToString>(&self, addr: S) -> Connection {
        let mut conns = self.0.lock().await;
        let addr = addr.to_string();
        let pool = conns
            .entry(addr.clone())
            .or_insert_with(ConnectionPool::new);
        Connection {
            conn: pool.connection(addr).await,
            pool: pool.clone(),
        }
    }
}

/// RPC transport.
#[derive(Derivative)]
#[derivative(Debug)]
pub struct RpcTransport {
    /// Node address mapping function.
    #[derivative(Debug = "ignore")]
    node_addr: Box<NodeAddrFn>,
    connections: Connections,
}

impl RpcTransport {
    /// Creates a new RPC transport.
    pub fn new(node_addr: Box<NodeAddrFn>) -> Self {
        RpcTransport {
            node_addr,
            connections: Connections::new(),
        }
    }
}

#[async_trait]
impl StoreTransport for RpcTransport{
    // to id?
    fn send_sp(&self, to_id: usize, msg: Message<StoreCommand, ()>) { // Message <StoreCommand>
        // msg.msg?
        // will be a paxosmessage..
        let from = msg.from;
        let to = msg.to;
        match msg.msg {
            PaxosMsg::Prepare(p) => {
                let ballot_n = Some(PBallot{n: p.n.n, priority: p.n.priority, pid: p.n.pid});
                let ballot_n_accepted = Some(PBallot{n: p.n_accepted.n, priority: p.n_accepted.priority, pid: p.n_accepted.pid});

                let prep_msg = Prep(PPrepare{
                    n: ballot_n,
                    ld: p.ld,
                    n_accepted: ballot_n_accepted,
                    la: p.la,
                });
                let request = PPaxosMsg{to, from, message: Some(prep_msg)};
                let peer = (self.node_addr)(to_id);
                let pool = self.connections.clone();
                tokio::task::spawn(async move {
                    let mut client = pool.connection(peer).await;
                    let request = tonic::Request::new(request.clone());
                    
                    // Change append entries ? YES
                    client.conn.prepare(request).await.unwrap();
                });
            }
            PaxosMsg::Promise(p) => {

               let ballot_n = Some(PBallot{n: p.n.n, priority: p.n.priority, pid: p.n.pid});
               let ballot_n_accepted = Some(PBallot{n: p.n_accepted.n, priority: p.n_accepted.priority, pid: p.n_accepted.pid});
               
               //Using an empty map for snapshotting for now
               let mut empt_map: HashMap<u64, _> = HashMap::new();
               let mut sync_item = None;
               let mut stop_sign = None;

               match p.sync_item {
                   Some(SyncItem::Entries(e)) => {
                       let commands: Vec<Entry> = e
                                   .iter()
                                   .map(|entry| {
                                       let id = entry.id;
                                       let sql = entry.sql.clone();
                                       Entry {id: id as u64, sql}
                                   }).collect();
                       sync_item = Some(PSyncItem{s: Some(Syncentries(Entries {e: commands}))});
                   }
                   Some(SyncItem::Snapshot(e)) => {
                        match e {
                           SnapshotType::_Phantom(_) => {
                               sync_item = Some(PSyncItem{s: Some(Syncitemtype(PSyncItemType::Phantom  as i32))});
                           } 
                           SnapshotType::Complete(snap) => {
                               sync_item = Some(PSyncItem{s: Some(Syncsnapshot(PSnapshotType {st: PSnapshotOneof::Complete as i32 , s_map: empt_map}))});
                           }
                           SnapshotType::Delta(snap) => { 
                               sync_item =Some(PSyncItem{s: Some(Syncsnapshot(PSnapshotType {st: PSnapshotOneof::Delta as i32 , s_map: empt_map}))});  
                           }
                       }
                   }
                   Some(SyncItem::None) => {
                       sync_item = Some(PSyncItem{s: Some(Syncitemtype(PSyncItemType::None  as i32))});
                   }
                   None => { 
                       sync_item = Some(PSyncItem{s: Some(Syncitemtype(PSyncItemType::Empty  as i32))});
                   }
               }
               match p.stopsign {
                   Some(s) => {
                       stop_sign = Some(StopSign {config_id: s.config_id, nodes: s.nodes, metadata: s.metadata});
                   }
                   None => {
                       //no SS
                   }
               }
               let prom_msg = Prom(PPromise{n:ballot_n, n_accepted: ballot_n_accepted, sync_item: sync_item, ld: p.ld, la: p.la, ss: stop_sign});
               let request = PPaxosMsg {to, from, message: Some(prom_msg)};
               
               let peer = (self.node_addr)(to_id);
               let pool = self.connections.clone();
               tokio::task::spawn(async move {
                   let mut client = pool.connection(peer).await;
                   let request = tonic::Request::new(request.clone());
                   
                   
                   // here we try to send promise to the leader
                   client.conn.promise(request).await.unwrap();
                   
               });
           }
           PaxosMsg::AcceptSync(p) => {
                let ballot_n = Some(PBallot{n: p.n.n, priority: p.n.priority, pid: p.n.pid});
                let mut sync_item = None;
                let mut stop_sign = None;

                // Again, snapshotting is not impemented yet
                let mut empt_map: HashMap<u64, _> = HashMap::new();

                match p.sync_item {
                    SyncItem::Entries(e) => {
                        let commands: Vec<Entry> = e
                                    .iter()
                                    .map(|entry| {
                                        let id = entry.id;
                                        let sql = entry.sql.clone();
                                        Entry {id: id as u64, sql}
                                    }).collect();
                        sync_item = Some(PSyncItem{s: Some(Syncentries(Entries {e: commands}))});
                    }
                    SyncItem::Snapshot(e) => {
                         match e {
                            SnapshotType::_Phantom(_) => {
                                sync_item = Some(PSyncItem{s: Some(Syncitemtype(PSyncItemType::Phantom  as i32))});
                            } 
                            SnapshotType::Complete(snap) => {
                                sync_item = Some(PSyncItem{s: Some(Syncsnapshot(PSnapshotType {st: PSnapshotOneof::Complete as i32 , s_map: empt_map}))});
                            }
                            SnapshotType::Delta(snap) => { 
                                sync_item =Some(PSyncItem{s: Some(Syncsnapshot(PSnapshotType {st: PSnapshotOneof::Delta as i32 , s_map: empt_map}))});  
                            }
                        }
                    }
                    SyncItem::None => {
                        sync_item = Some(PSyncItem{s: Some(Syncitemtype(PSyncItemType::None  as i32))});
                    }
                    /*None => { 
                        sync_item = Some(PSyncItem{s: Some(Syncitemtype(PSyncItemType::Empty  as i32))});
                    } */
                }
                match p.stopsign {
                    Some(s) => {
                        stop_sign = Some(StopSign {config_id: s.config_id, nodes: s.nodes, metadata: s.metadata});
                    }
                    None => {
                        //no SS
                    }
                }
                let acc_sync_msg = Accsync(PAcceptSync{n: ballot_n, sync_item: sync_item, sync_idx: p.sync_idx, decide_idx: p.decide_idx, ss: stop_sign});
                let request = PPaxosMsg {to, from, message: Some(acc_sync_msg)};
                
                let peer = (self.node_addr)(to_id);
                let pool = self.connections.clone();
                tokio::task::spawn(async move {
                    let mut client = pool.connection(peer).await;
                    let request = tonic::Request::new(request.clone());
                    
                    
                    // here we try to send promise to the leader
                    client.conn.accept_sync(request).await.unwrap();
                    
                });
            }

            PaxosMsg::FirstAccept(p) => {
                let ballot_n = Some(PBallot{n: p.n.n, priority: p.n.priority, pid: p.n.pid});

                /// "entries" ÄR MATCHNING AV MEDDELANDE FRÅN OMNIPAXOS. Kan vara så att d ej funkar på senare
                let entries = p.entries.iter().map(|entry| {StoreCommand { id: entry.id , sql: entry.sql.clone()}}).collect();

                let first_acc_msg = Firstacc(PFirstAccept{n: ballot_n, e: entries});
                let request = PPaxosMsg {to, from, message: Some(first_acc_msg)};
                
                let peer = (self.node_addr)(to_id);
                let pool = self.connections.clone();
                tokio::task::spawn(async move {
                    let mut client = pool.connection(peer).await;
                    let request = tonic::Request::new(request.clone());
                    client.conn.first_accept(request).await.unwrap();
                });
            }
            PaxosMsg::AcceptDecide(p) => {
                let ballot_n = Some(PBallot{n: p.n.n, priority: p.n.priority, pid: p.n.pid});
                let entries = p.entries.iter().map(|entry| {Entry { id: entry.id as u64, sql: entry.sql.clone()}}).collect();
                let acc_dec_msg = Accdec(PAcceptDecide{n: ballot_n, ld: p.ld,  e: entries});
                let request = PPaxosMsg {to, from, message: Some(acc_dec_msg)};
                let peer = (self.node_addr)(to_id);
                let pool = self.connections.clone();
                tokio::task::spawn(async move {
                    let mut client = pool.connection(peer).await;
                    let request = tonic::Request::new(request.clone());
                    client.conn.accept_decide(request).await.unwrap();
                });
            }
            PaxosMsg::Accepted(p) => {
                let ballot_n = Some(PBallot{n: p.n.n, priority: p.n.priority, pid: p.n.pid});
                let acc_msg = Acc(PAccepted{n: ballot_n, la: p.la});
                let request = PPaxosMsg {to, from, message: Some(acc_msg)};
                
                let peer = (self.node_addr)(to_id);
                let pool = self.connections.clone();
                tokio::task::spawn(async move {
                    let mut client = pool.connection(peer).await;
                    let request = tonic::Request::new(request.clone());
                    client.conn.accepted(request).await.unwrap();
                });
            }
            PaxosMsg::Decide(p) => {

                let ballot_n = Some(PBallot{n: p.n.n, priority: p.n.priority, pid: p.n.pid});
                let dec_msg = Dec(PDecide{n:ballot_n, ld: p.ld});
                let request = PPaxosMsg {to, from, message: Some(dec_msg)};

                let peer = (self.node_addr)(to_id);
                let pool = self.connections.clone();
                tokio::task::spawn(async move {
                    let mut client = pool.connection(peer).await;
                    let request = tonic::Request::new(request.clone());
                    client.conn.decide(request).await.unwrap();
                });
            }
            PaxosMsg::ProposalForward(p) => {
                let entries = p.iter().map(|entry| { Entry {id: entry.id as u64, sql: entry.sql.clone()}}).collect();
                let proposal_fwd_msg = Proposalfwd (PProposalForward {e: entries});
                let request = PPaxosMsg {to, from, message: Some(proposal_fwd_msg)};

                let peer = (self.node_addr)(to_id);
                let pool = self.connections.clone();
                tokio::task::spawn(async move {
                    let mut client = pool.connection(peer).await;
                    let request = tonic::Request::new(request.clone());
                    client.conn.proposal_forward(request).await.unwrap();
                });
            }
            PaxosMsg::Compaction(p)  => {
                
                let mut comp_msg = None;
                match p {
                    Compaction::Trim(t) => {
                        comp_msg = Some(Comp(PCompaction {s: "trim".to_string(), v: to_id, itisforward: false}));
                    }
                    Compaction::Snapshot(t) => {
                        comp_msg = Some(Comp(PCompaction {s: "snapshot".to_string(), v: Some(t), itisforward: false}));
                    }
                }
                let request = PPaxosMsg{to, from, message: comp_msg};
                
                let peer = (self.node_addr)(to_id);
                let pool = self.connections.clone();
                tokio::task::spawn(async move {
                    let mut client = pool.connection(peer).await;
                    let request = tonic::Request::new(request.clone());
                    client.conn.compaction(request).await.unwrap();
                });
            }

            PaxosMsg::ForwardCompaction(p)  => {
                let mut fwd_comp_msg = None;
                match p {
                    Compaction::Trim(t) => {
                        fwd_comp_msg = Some(Comp(PCompaction {s: "trim".to_string(), v: t, itisforward: false}));
                    }
                    Compaction::Snapshot(t) => {
                        fwd_comp_msg = Some(Comp(PCompaction {s: "snapshot".to_string(), v: Some(t), itisforward: false}));
                    }
                }
                let request = PPaxosMsg {to, from, message: fwd_comp_msg};
                
                let peer = (self.node_addr)(to_id);
                let pool = self.connections.clone();
                tokio::task::spawn(async move {
                    let mut client = pool.connection(peer).await;
                    let request = tonic::Request::new(request.clone());
                    client.conn.compaction(request).await.unwrap();
                });
            }
        
            PaxosMsg::AcceptStopSign(p) => {
                let ballot_n = Some(PBallot{n: p.n.n, priority: p.n.priority, pid: p.n.pid});
                let stop_sign = Some(StopSign {config_id: p.ss.config_id, nodes: p.ss.nodes, metadata: Some(p.ss.metadata.unwrap())});
                let acc_ss_msg = Acceptss(PAcceptStopSign{n:ballot_n, ss:stop_sign});
                let request = PPaxosMsg {to, from, message: Some(acc_ss_msg)};

                let peer = (self.node_addr)(to_id);
                let pool = self.connections.clone();
                tokio::task::spawn(async move {
                    let mut client = pool.connection(peer).await;
                    let request = tonic::Request::new(request.clone());
                    client.conn.accept_stop_sign(request).await.unwrap();
                });

            }
            PaxosMsg::AcceptedStopSign(p) => {
                let ballot_n = Some(PBallot{n: p.n.n, priority: p.n.priority, pid: p.n.pid});
                let accd_ss_msg = Acceptedss(PAcceptedStopSign{n:ballot_n});
                let request = PPaxosMsg {to, from, message: Some(accd_ss_msg)};

                let peer = (self.node_addr)(to_id);
                let pool = self.connections.clone();
                tokio::task::spawn(async move {
                    let mut client = pool.connection(peer).await;
                    let request = tonic::Request::new(request.clone());
                    client.conn.accepted_stop_sign(request).await.unwrap();
                });
            }
            PaxosMsg::DecideStopSign(p) => {
                let ballot_n = Some(PBallot{n: p.n.n, priority: p.n.priority, pid: p.n.pid});
                let accd_ss_msg = Decidess(PDecideStopSign{n:ballot_n});
                let request = PPaxosMsg {to, from, message: Some(accd_ss_msg)};

                let peer = (self.node_addr)(to_id);
                let pool = self.connections.clone();
                tokio::task::spawn(async move {
                    let mut client = pool.connection(peer).await;
                    let request = tonic::Request::new(request.clone());
                    client.conn.decide_stop_sign(request).await.unwrap();
                });
            }

            // TODO ?
            PaxosMsg::PrepareReq => {

            }
        }
    }
    fn send_ble_to_rpc(&self, to_id: usize, msg: BLEMessage) { // Message <StoreCommand>
    
        let from = msg.from;
        let to = msg.to;

        match msg.msg {
            HeartbeatMsg::Request(r) => {
                //log::info!("[BLE_SEND] FROM {} TO {}, BLE REQUEST: {}",from, to, r.round);
                let hb_req_msg =  Hbrequest(PHeartbeatRequest{round: r.round});
                let request = PBleMessage {to, from, message: Some(hb_req_msg)};

                let peer = (self.node_addr)(to_id);
                let pool = self.connections.clone();
                tokio::task::spawn(async move {
                    let mut client = pool.connection(peer).await;
                    let request = tonic::Request::new(request.clone());
                    client.conn.ble_message(request).await.unwrap();
                });

            }
            HeartbeatMsg::Reply(r) => {
               // log::info!("[BLE_SEND] FROM {} TO {}, BLE REPLY: Ballot: n: {} priority: {} pid: {}",from, to,  r.ballot.n, r.ballot.priority, r.ballot.pid);
                let ballot_n = Some(PBallot{n: r.ballot.n, priority: r.ballot.priority, pid: r.ballot.pid});
                let hb_rep_msg = Hbreply(PHeartbeatReply{round: r.round, n: ballot_n, majority_connected: r.majority_connected});
                let request = PBleMessage {to, from, message: Some(hb_rep_msg)};

                let peer = (self.node_addr)(to_id);
                let pool = self.connections.clone();
                tokio::task::spawn(async move {
                    let mut client = pool.connection(peer).await;
                    let request = tonic::Request::new(request.clone());
                    client.conn.ble_message(request).await.unwrap();
                });
            }
        }
    }
    async fn delegate(
        &self,
        to_id: usize,
        sql: String,
        consistency: Consistency,
    ) -> Result<crate::server::QueryResults, crate::StoreError> {
        let addr = (self.node_addr)(to_id);
        let mut client = self.connections.connection(addr.clone()).await;
        let query = tonic::Request::new(Query {
            sql,
            consistency: consistency as i32,
        });
        let response = client.conn.execute(query).await.unwrap();
        let response = response.into_inner();
        let mut rows = vec![];
        for row in response.rows {
            rows.push(crate::server::QueryRow { values: row.values });
        }
        Ok(crate::server::QueryResults { rows })
    }
}

/// RPC service.
#[derive(Debug)]
pub struct RpcService {
    /// The ChiselStore server access via this RPC service.
    pub server: Arc<StoreServer<RpcTransport>>,
}

impl RpcService {
    /// Creates a new RPC service.
    pub fn new(server: Arc<StoreServer<RpcTransport>>) -> Self {
        Self { server }
    }
}

#[tonic::async_trait]
impl Rpc for RpcService {
    async fn execute(
        &self,
        request: Request<Query>,
    ) -> Result<Response<QueryResults>, tonic::Status> {
        let query = request.into_inner();
        let consistency =
            proto::Consistency::from_i32(query.consistency).unwrap_or(proto::Consistency::Strong);
        let consistency = match consistency {
            proto::Consistency::Strong => Consistency::Strong,
            proto::Consistency::RelaxedReads => Consistency::RelaxedReads,
        };
        let server = self.server.clone();
        let results = match server.query(query.sql, consistency).await {
            Ok(results) => results,
            Err(e) => return Err(Status::internal(format!("{}", e))),
        };
        let mut rows = vec![];
        for row in results.rows {
            rows.push(QueryRow {
                values: row.values.clone(),
            })
        }
        Ok(Response::new(QueryResults { rows }))
    }

// ----------- Construct PAXOS msgs -----------

    // Not sure if it should be Request<PaxosMsg> or Request<Prepare>!!!
    async fn prepare(&self, request: Request<PPaxosMsg>) -> Result<Response<Void>, tonic::Status> {
        
        //fetch paxos message that is sent from send()
        let msg = request.into_inner();
        //fetch from and to
        let from = msg.from;
        let to = msg.to;

        // create two new variables that will hold the ballot round, 
        // and latest round in which an entry was accepted
        let mut ballot_round = None;
        let mut ballot_round_acc = None;

        // need to unpack explicity (pattern match) since we have Option(t)
        // in order to get the values (in this case two types of ballot values).

        match msg.message{
            Some(Prep(prep)) => {
                match prep.n {
                    Some(b) => {
                        ballot_round = Some(Ballot{n: b.n, priority: b.priority, pid: b.pid});
                            }   
                    None => {
                                //no ballot round;
                    }
                }
                match prep.n_accepted{
                    // some ballot
                    Some(b) => {
                        ballot_round_acc= Some(Ballot{n: b.n, priority: b.priority, pid: b.pid});
                    }
                    None => {
                                // no accepted:
                    }
                }
                            // create a new Prepare msg with the unwrapped values (ballots)
                let prepare_msg = PaxosMsg::Prepare(Prepare{n: ballot_round.unwrap(), ld: prep.ld, n_accepted: ballot_round_acc.unwrap(), la: prep.la});
                // Create a new paxos message containing the promise msg
                // This will be handled by omni_paxos at the server layer
                let msg = Message{to: to.try_into().unwrap(), from, msg: prepare_msg};
                // Create server instance
                let server = self.server.clone();
                // Invoke msg handler in the server. 
                server.handle_sp_msg(msg);
            }
            // Panics if there is another type of msg being sent
            _ => { unreachable!() }
            None => {
                // rip
            }
        }
        Ok(Response::new(Void {}))
    }
    

    async fn promise(&self, request: Request<PPaxosMsg>)-> Result<Response<Void>, tonic::Status> {
        let msg = request.into_inner();
        let from = msg.from;
        let to = msg.to;
        let mut ballot_n = None;
        let mut ballot_n_accepted = None;
        let mut sync_item = None;
        let mut stop_sign = None;
       // let mut empt_map: HashMap<u64, _> = HashMap::new();
    
        match msg.message{
            Some(Prom(p)) => {
                match p.n {
                    Some(b) => {
                        ballot_n = Some(Ballot{n: b.n, priority: b.priority, pid: b.pid});
                    }
                    None => {
                    }
                }
                match p.n_accepted {
                    Some(b) => {
                        ballot_n_accepted = Some(Ballot{n: b.n, priority: b.priority, pid: b.pid});
                    }
                    None => {
                    }
                }
                match p.sync_item {
                    Some(item) => {
                        match item.s {
                            Some(Syncentries (Entries {e: entries})) => {
                                let entries = entries.iter().map(|entry| {StoreCommand { id: entry.id as usize, sql: entry.sql.clone()}}).collect();
                                sync_item = Some(SyncItem::<StoreCommand, ()>::Entries(entries));
                            }
                            Some(Syncitemtype(t)) => {
                                match PSyncItemType::from_i32(t) {
                                    Some(PSyncItemType::Phantom) =>{
                                        sync_item = Some(SyncItem::<StoreCommand, ()>::Snapshot(SnapshotType::<StoreCommand, ()>::_Phantom(core::marker::PhantomData::<StoreCommand>)));
                                    } ,
                                    Some(PSyncItemType::None) =>{
                                        sync_item = Some(SyncItem::<StoreCommand, ()>::None);
                                    }
                                    Some(PSyncItemType::Empty) =>{
                                    }
                                    None => {
                                        //
                                    }
                                }
                            }
                            Some(Syncsnapshot(PSnapshotType{st, s_map})) => {
                                match PSnapshotOneof::from_i32(st) {
                                    Some(PSnapshotOneof::Complete) =>{
                                        sync_item = Some(SyncItem::<StoreCommand, ()>::Snapshot(SnapshotType::Delta(())));
                                    } ,
                                    Some(PSnapshotOneof::Delta) =>{
                                        sync_item = Some(SyncItem::<StoreCommand, ()>::Snapshot(SnapshotType::Complete(())));
                                    }
                                    None => {
                                    }
                                }
                            }
                            None => {
                                //?
                            }
                        }
                    }
                    None => {
                        //?
                    }
                }
                match p.ss {
                    Some(s) => {
                        stop_sign = Some(omnipaxos_core::storage::StopSign{config_id: s.config_id, nodes: s.nodes, metadata: s.metadata});
                    }
                    None => {
                    }
                }
                let themessage = PaxosMsg::Promise(Promise{n: ballot_n.unwrap(), n_accepted: ballot_n_accepted.unwrap(), sync_item: sync_item, ld: p.ld, la: p.la, stopsign: stop_sign});
                let msg = Message{
                    to,
                    from,
                    msg: themessage,
                };
                let server = self.server.clone();
                server.handle_sp_msg(msg);
            }
            // Panics if there is another type of msg being sent
            _ => { unreachable!() }
            None => {
                //
            }
        }
        Ok(Response::new(Void {}))
    }
     
    async fn accept_sync(&self, request: Request<PPaxosMsg>) -> Result<Response<Void>, tonic::Status>{
        let msg = request.into_inner();
        let from = msg.from;
        let to = msg.to;
        let mut sync_item = None;
        let mut ballot_n = None;
        let mut stop_sign = None;
        
        match msg.message{
            Some(Accsync(p)) => {
                match p.n {
                    Some(b) => {
                        ballot_n = Some(Ballot{n: b.n, priority: b.priority, pid: b.pid});
                    }
                    None => {
                          
                    }
                } 
                match p.sync_item {
                    Some(item) => {
                        match item.s {
                            Some(Syncentries (Entries {e: entries})) => {
                                let entries = entries.iter().map(|entry| {StoreCommand { id: entry.id as usize, sql: entry.sql.clone()}}).collect();
                                sync_item = Some(SyncItem::<StoreCommand, ()>::Entries(entries));
                            }
                            Some(Syncitemtype(t)) => {
                                match PSyncItemType::from_i32(t) {
                                    Some(PSyncItemType::Phantom) =>{
                                        sync_item = Some(SyncItem::<StoreCommand, ()>::Snapshot(SnapshotType::<StoreCommand, ()>::_Phantom(core::marker::PhantomData::<StoreCommand>)));
                                    } ,
                                    Some(PSyncItemType::None) =>{
                                        sync_item = Some(SyncItem::<StoreCommand, ()>::None);
                                    }
                                    Some(PSyncItemType::Empty) =>{
                                    }
                                    None => {
                                        //
                                    }
                                }
                            }
                            Some(Syncsnapshot(PSnapshotType{st, s_map})) => {
                                match PSnapshotOneof::from_i32(st) {
                                    Some(PSnapshotOneof::Complete) =>{
                                        sync_item = Some(SyncItem::<StoreCommand, ()>::Snapshot(SnapshotType::Delta(())));
                                    } ,
                                    Some(PSnapshotOneof::Delta) =>{
                                        sync_item = Some(SyncItem::<StoreCommand, ()>::Snapshot(SnapshotType::Complete(())));
                                    }
                                    None => {
                                    }
                                }
                            }
                            None => {
                                //?
                            }
                        }
                    }
                    None => {
                        //?
                    }
                }   
                match p.ss {
                    Some(s) => {
                        stop_sign = Some(omnipaxos_core::storage::StopSign{config_id: s.config_id, nodes: s.nodes, metadata: s.metadata});
                    }
                    None => {
                    }
                }             
                let acc_sync_msg = PaxosMsg::AcceptSync(AcceptSync{n: ballot_n.unwrap(), sync_item: sync_item.unwrap(), sync_idx: p.sync_idx, decide_idx: p.decide_idx, stopsign: stop_sign});
                let msg = Message{to, from, msg: acc_sync_msg};
                let server = self.server.clone();
                server.handle_sp_msg(msg);     
                 
            }
            
            // Panics if there is another type of msg being sent
            _ => { unreachable!() }
            None => {
                //
            
            }
        }
        Ok(Response::new(Void {}))
    } 
        
        

    async fn first_accept(&self, request: Request<PPaxosMsg>) -> Result<Response<Void>, tonic::Status>{
        let msg = request.into_inner();
        let from = msg.from;
        let to = msg.to;
        let ballot_n = None;
        match msg.message{
            Some(Firstacc(p)) => {
                match p.n {
                    Some(b) => {
                        ballot_n = Some(Ballot{n: b.n, priority: b.priority, pid: b.pid});
                    }
                    None => {
                        //BBB
                    }
                }    
                // fetch entries by iterating over them
                // use closure expression.
                // here we map each entry to the store command (SQL) operation
                // finally we unpack the value with unwrap()
                let entries = p.e.iter().map(|entry| {StoreCommand { id: entry.id, sql: entry.sql.clone()}}).collect();
                let  first_accept_msg = PaxosMsg::FirstAccept(FirstAccept{n: ballot_n.unwrap(), entries: entries});
                let msg = Message{to, from, msg: first_accept_msg};
                let server = self.server.clone();
                server.handle_sp_msg(msg);
            }
            None => {
                // rip
            }
            _ => { unreachable!() }      
        }
        Ok(Response::new(Void {}))
    }    
    

    async fn accept_decide(&self, request: Request<PPaxosMsg>) -> Result<Response<Void>, tonic::Status>{
        let msg = request.into_inner();
        let from = msg.from;
        let to = msg.to;
        let ballot_n = None;    
        match msg.message{
            Some(Accdec(p)) => {
                match p.n{
                    Some(b) => {
                        ballot_n = Some(Ballot{n: b.n, priority: b.priority, pid: b.pid});
                    }
                    None => {
                        // No ballot
                    }
                }
                let entries = p.e.iter().map(|entry| {StoreCommand{id: entry.id, sql:entry.sql.clone()}}).collect();
                let accept_decide = PaxosMsg::FirstAccept(FirstAccept{n: ballot_n.unwrap(), entries: entries});
                let msg = Message{to, from, msg: accept_decide};
                let server = self.server.clone();
                server.handle_sp_msg(msg);
            }
            _ => { unreachable!() }
            None => {
            }
        }
        Ok(Response::new(Void {}))
    }
 
    async fn accepted(&self, request: Request<PPaxosMsg>) -> Result<Response<Void>, tonic::Status>{
        let msg = request.into_inner();
        let from = msg.from;
        let to = msg.to;
        let ballot_n = None;    
        match msg.message{
            Some(Acc(p))=>{
                match p.n{
                    Some(b) => {
                        ballot_n = Some(Ballot{n: b.n, priority: b.priority, pid: b.pid});
                    }
                    None => {
                        // No ballot
                    }
                }
                let accepted_msg = PaxosMsg::Accepted(Accepted{n: ballot_n.unwrap(), la: p.la});
                let msg = Message{to, from, msg: accepted_msg};
                let server = self.server.clone();
                server.handle_sp_msg(msg);
            }
            _ => { unreachable!() }
            None => {
                //eh
            }
        }
        Ok(Response::new(Void {}))
    }
    

    async fn decide(&self, request: Request<PPaxosMsg>) -> Result<Response<Void>, tonic::Status>{
        let msg = request.into_inner();
        let from = msg.from;
        let to = msg.to;
        let ballot_n = None;    
        
        match msg.message{
            Some(Dec(p))=>{
                match p.n{
                    Some(b) => {
                        ballot_n = Some(Ballot{n: b.n, priority: b.priority, pid: b.pid});
        
                    }
                    None => {
                        // No ballot
                    }
                }
                let decided_msg = PaxosMsg::Decide(Decide{n: ballot_n.unwrap(), ld: p.ld});
                let msg = Message{to, from, msg: decided_msg};
                let server = self.server.clone();
                server.handle_sp_msg(msg);
            }
            _ => { unreachable!() }
            None => {
                //
            }
        }
        Ok(Response::new(Void {}))
    }
    async fn proposal_forward(&self, request: Request<PPaxosMsg>) -> Result<Response<Void>, tonic::Status> {
        let msg = request.into_inner();
        let from = msg.from;
        let to = msg.to;
        let entries = None;

        match msg.message{
            Some(Proposalfwd(p))=> {
                entries = p.e.iter().map(|entry| {StoreCommand { id: entry.id, sql: entry.sql.clone()}}).collect();
            }
            _ => { unreachable!() }
            None => {
            }
        }
        let proposal_fwd_msg = PaxosMsg::ProposalForward(ProposalForward{es: entries});
        let msg = Message{to, from, msg: proposal_fwd_msg};
        let server = self.server.clone();
        server.handle_sp_msg(msg);

        Ok(Response::new(Void {}))
    }

    // Compaction
    async fn compaction(&self, request: Request<PPaxosMsg>) -> Result<Response<Void>, tonic::Status> {
        let msg = request.into_inner();
        let from = msg.from;
        let to = msg.to;
        let mut comp_msg = None;

        match msg.message{
            Some(Comp(p)) => {
                match p.itisforward {
                    true => {
                        if p.s == "trim" {
                             comp_msg = Some(PaxosMsg::ForwardCompaction(Compaction::Trim(p.v))); 
                        }
                        else if p.s == "snapshot" {
                             comp_msg = Some(PaxosMsg::ForwardCompaction(Compaction::Snapshot(p.v.unwrap())));
                        }
                    }
                    false => {
                        if p.s == "trim" {
                            comp_msg = Some(PaxosMsg::Compaction(Compaction::Trim(p.v))); 
                       }
                       else if p.s == "snapshot" {
                            comp_msg = Some(PaxosMsg::Compaction(Compaction::Snapshot(p.v.unwrap())));
                       }
                    }
                }
                let pmsg = Message{
                    to,
                    from,
                    msg: comp_msg.unwrap(),
                };
                let server = self.server.clone();
                server.handle_sp_msg(pmsg);
            }
            _ => { unreachable!() }
            None => {
            }
        }
        Ok(Response::new(Void {}))
    }

    // ForwardCompaction
    async fn accept_stop_sign(&self, request: Request<PPaxosMsg>) -> Result<Response<Void>, tonic::Status> {
        let msg = request.into_inner();
        let from = msg.from;
        let to = msg.to;
        let ballot_n = None;    
        let ss = None;
    
        match msg.message{
            Some(Acceptss(p))=>{
                match p.n{
                    Some(b) => {
                        ballot_n = Some(Ballot{n: b.n, priority: b.priority, pid: b.pid});
                    }
                    None => {
                        // No ballot
                    }
                }
                match p.ss{
                    Some(s) => {
                        ss = Some(omnipaxos_core::storage::StopSign{config_id: s.config_id, nodes: s.nodes, metadata: s.metadata});
                    }
                    None => {
                        // No ballot
                    }
                }
                let accept_ss_msg = PaxosMsg::AcceptStopSign(AcceptStopSign{n: ballot_n.unwrap(), ss: ss.unwrap()});
                let msg = Message{to, from, msg: accept_ss_msg};
                let server = self.server.clone();
                server.handle_sp_msg(msg);
            }
            _ => { unreachable!() }
            None => {
                //
            }
        }
        Ok(Response::new(Void {}))

    }
    async fn accepted_stop_sign(&self, request: Request<PPaxosMsg>) -> Result<Response<Void>, tonic::Status> {
        let msg = request.into_inner();
        let from = msg.from;
        let to = msg.to;
        let ballot_n = None;    
        
        match msg.message{
            Some(Dec(p))=>{
                match p.n{
                    Some(b) => {
                        ballot_n = Some(Ballot{n: b.n, priority: b.priority, pid: b.pid});
        
                    }
                    None => {
                        // No ballot
                    }
                }
                let accepted_ss_msg = PaxosMsg::AcceptedStopSign(AcceptedStopSign{n: ballot_n.unwrap()});
                let msg = Message{to, from, msg: accepted_ss_msg};
                let server = self.server.clone();
                server.handle_sp_msg(msg);
            }
            _ => { unreachable!() }
            None => {
                //
            }
        }
        Ok(Response::new(Void {}))
    }

    async fn decide_stop_sign(&self, request: Request<PPaxosMsg>) -> Result<Response<Void>, tonic::Status> {
        let msg = request.into_inner();
        let from = msg.from;
        let to = msg.to;
        let ballot_n = None;    
        match msg.message{
            Some(Dec(p))=>{
                match p.n{
                    Some(b) => {
                        ballot_n = Some(Ballot{n: b.n, priority: b.priority, pid: b.pid});
        
                    }
                    None => {
                        // No ballot
                    }
                }
                let decide_ss_msg = PaxosMsg::DecideStopSign(DecideStopSign{n: ballot_n.unwrap()});
                let msg = Message{to, from, msg: decide_ss_msg};
                let server = self.server.clone();
                server.handle_sp_msg(msg);
            }
            _ => { unreachable!() }
            None => {
                //
            }
        }
        Ok(Response::new(Void {}))

    }



    async fn ble_message(&self, request: Request<PBleMessage>) -> Result<Response<Void>, tonic::Status> {
       
        let msg = request.into_inner();
        let from = msg.from;
        let to = msg.to;
        let ballot_n = None;


        // case of HB request being sent to us
        match msg.message{
            Some(Hbrequest(props)) => {

                // store the ble_message, i.e. the HB request. It contains a round (see )
                // how we pattern match msg here from proto:
                // BleMessage picks --> (enum) HeartbeatMsg --> HeartbeatRequest(round)
                let ble_message = HeartbeatMsg::Request(HeartbeatRequest{round: props.round});
                // the new paxos messege sent to server side
                let msg = omnipaxos_core::ballot_leader_election::messages::BLEMessage{to, from, msg: ble_message};
                let server = self.server.clone();
                // pass message to server layer 
                server.handle_ble_msg(msg);
            }
            None => {
                // Too bad no msg
            }   

            // case of HB reply being sent from us
            // a reply uses Ballot as well.
            Some(Hbreply(props)) => {                
                match props.n {
                    // "if we have some ballot"
                    Some(b) => {
                        ballot_n = Some(Ballot{n: b.n, priority: b.priority, pid: b.pid});
                    }
                    None => {
                        //err
                    }
                }
                // BleMessage picks --> (enum) HeartbeatMsg --> HeartbeatReply(round, ballot, majority connected)
                let ble_message = HeartbeatMsg::Reply(HeartbeatReply{round: props.round, ballot: ballot_n.unwrap(), majority_connected: props.majority_connected});
                let msg = omnipaxos_core::ballot_leader_election::messages::BLEMessage{to,from, msg:ble_message};
                let server = self.server.clone();
                // pass message to server layer                
                server.handle_ble_msg(msg);
            }
        }
        Ok(Response::new(Void {}))
    }
}








      //  vote:  This function is starting a new leader election. 
       // Simply sends a voterequest to all nodes including reference to self
   /*  
    async fn vote(&self, request: Request<VoteRequest>) -> Result<Response<Void>, tonic::Status> {

        // request.to_inner() unwraps my message 
        let msg = request.into_inner();

        // unwrap all values from the message 
        let from_id = msg.from_id as usize;
        let term = msg.term as usize;
        let last_log_index = msg.last_log_index as usize;
        let last_log_term = msg.last_log_term as usize;

        // create a new raft message containing the values
        let msg = little_raft::message::Message::VoteRequest {
            from_id,
            term,
            last_log_index,
            last_log_term,
        };

        // clone server (?)
        let server = self.server.clone();

        // pass message to server layer 
        server.recv_msg(msg);

        // Ok message (???)
        Ok(Response::new(Void {}))
    }

    async fn respond_to_vote(
        &self,
        request: Request<VoteResponse>,
    ) -> Result<Response<Void>, tonic::Status> {
        let msg = request.into_inner();
        let from_id = msg.from_id as usize;
        let term = msg.term as usize;
        let vote_granted = msg.vote_granted;
        let msg = little_raft::message::Message::VoteResponse {
            from_id,
            term,
            vote_granted,
        };
        let server = self.server.clone();
        server.recv_msg(msg);
        Ok(Response::new(Void {}))
    }

    async fn append_entries(
        &self,
        request: Request<AppendEntriesRequest>,
    ) -> Result<Response<Void>, tonic::Status> {
        let msg = request.into_inner();
        let from_id = msg.from_id as usize;
        let term = msg.term as usize;
        let prev_log_index = msg.prev_log_index as usize;
        let prev_log_term = msg.prev_log_term as usize;
        let entries: Vec<little_raft::message::LogEntry<StoreCommand>> = msg
            .entries
            .iter()
            .map(|entry| {
                let id = entry.id as usize;
                let sql = entry.sql.to_string();
                let transition = StoreCommand { id, sql };
                let index = entry.index as usize;
                let term = entry.term as usize;
                little_raft::message::LogEntry {
                    transition,
                    index,
                    term,
                }
            })
            .collect();
        let commit_index = msg.commit_index as usize;
        let msg = little_raft::message::Message::AppendEntryRequest {
            from_id,
            term,
            prev_log_index,
            prev_log_term,
            entries,
            commit_index,
        };
        let server = self.server.clone();
        server.recv_msg(msg);
        Ok(Response::new(Void {}))
    }

    async fn respond_to_append_entries(
        &self,
        request: tonic::Request<AppendEntriesResponse>,
    ) -> Result<tonic::Response<Void>, tonic::Status> {
        let msg = request.into_inner();
        let from_id = msg.from_id as usize;
        let term = msg.term as usize;
        let success = msg.success;
        let last_index = msg.last_index as usize;
        let mismatch_index = msg.mismatch_index.map(|idx| idx as usize);
        let msg = little_raft::message::Message::AppendEntryResponse {
            from_id,
            term,
            success,
            last_index,
            mismatch_index,
        };
        let server = self.server.clone();
        server.recv_msg(msg);
        Ok(Response::new(Void {}))
    }
} */



/*
Raft shit

            Message::AppendEntryRequest {
                
                
                
                // Raft

                from_id,
                term,
                prev_log_index,
                prev_log_term,
                entries,
                commit_index,
            } => {
                let from_id = from_id as u64;
                let term = term as u64;
                let prev_log_index = prev_log_index as u64;
                let prev_log_term = prev_log_term as u64;
                let entries = entries
                    .iter()
                    .map(|entry| {
                        let id = entry.transition.id as u64;
                        let index = entry.index as u64;
                        let sql = entry.transition.sql.clone();
                        let term = entry.term as u64;
                        LogEntry {
                            id,
                            sql,
                            index,
                            term,
                        }
                    })
                    .collect();
                let commit_index = commit_index as u64;
                let request = AppendEntriesRequest {
                    from_id,
                    term,
                    prev_log_index,
                    prev_log_term,
                    entries,
                    commit_index,
                };
                let peer = (self.node_addr)(to_id);
                let pool = self.connections.clone();
                tokio::task::spawn(async move {
                    let mut client = pool.connection(peer).await;
                    let request = tonic::Request::new(request.clone());
                    client.conn.append_entries(request).await.unwrap();
                });
            }
            Message::AppendEntryResponse {
                from_id,
                term,
                success,
                last_index,
                mismatch_index,
            } => {
                let from_id = from_id as u64;
                let term = term as u64;
                let last_index = last_index as u64;
                let mismatch_index = mismatch_index.map(|idx| idx as u64);
                let request = AppendEntriesResponse {
                    from_id,
                    term,
                    success,
                    last_index,
                    mismatch_index,
                };
                let peer = (self.node_addr)(to_id);
                let pool = self.connections.clone();
                tokio::task::spawn(async move {
                    let mut client = pool.connection(peer).await;
                    let request = tonic::Request::new(request.clone());
                    client
                        .conn
                        .respond_to_append_entries(request)
                        .await
                        .unwrap();
                });
            }
            Message::VoteRequest {
                from_id,
                term,
                last_log_index,
                last_log_term,
            } => {
                let from_id = from_id as u64;
                let term = term as u64;
                let last_log_index = last_log_index as u64;
                let last_log_term = last_log_term as u64;
                let request = VoteRequest {
                    from_id,
                    term,
                    last_log_index,
                    last_log_term,
                };
                let peer = (self.node_addr)(to_id);
                let pool = self.connections.clone();
                tokio::task::spawn(async move {
                    let mut client = pool.connection(peer).await;
                    let vote = tonic::Request::new(request.clone());
                    client.conn.vote(vote).await.unwrap();
                });
            }
            Message::VoteResponse {
                from_id,
                term,
                vote_granted,
            } => {
                let peer = (self.node_addr)(to_id);
                tokio::task::spawn(async move {
                    let from_id = from_id as u64;
                    let term = term as u64;
                    let response = VoteResponse {
                        from_id,
                        term,
                        vote_granted,
                    };
                    if let Ok(mut client) = RpcClient::connect(peer.to_string()).await {
                        let response = tonic::Request::new(response.clone());
                        client.respond_to_vote(response).await.unwrap();
                    }
                });
            }


*/
*/