//! ChiselStore server module
use crate::errors::StoreError;
use async_notify::Notify;
use async_trait::async_trait;
use crossbeam_channel as channel;
use crossbeam_channel::{Receiver, Sender};
use derivative::Derivative;
use sqlite::{Connection, OpenFlags};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::sleep;
use std::time::Duration;

// Stuff we need from omnipaxos core.
use omnipaxos_core::messages::Message;
use omnipaxos_core::ballot_leader_election::{Ballot, BallotLeaderElection, BLEConfig};
use omnipaxos_core::ballot_leader_election::messages::BLEMessage;
use omnipaxos_core::sequence_paxos::{SequencePaxos, SequencePaxosConfig};
use omnipaxos_core::storage::{Snapshot, StopSignEntry, Storage};

/// ChiselStore transport layer.
/// Comment from ChiselStore creator: 
/// Your application should implement this trait to provide network access to the ChiselStore server.
#[async_trait]
pub trait StoreTransport {
    
    /// Send a store command message `msg` to `to_id` node. 
    /// msg = omnipaxos message
    fn send_sp_to_rpc(&self, to_id: u64, msg: Message<StoreCommand, ()>);

    // defining the ble message here for RPC communication
    fn send_ble_to_rpc(&self, to_id: u64, msg: BLEMessage);

}

/// Store command.
/// Comment from ChiselStore creator:
/// A store command is a SQL statement that is replicated in the Raft cluster.
/// A log entry = "{id : 1, sql: insert into * }"
#[derive(Clone, Debug)]
pub struct StoreCommand {
    /// Unique ID of this command.
    pub id: usize,
    /// The SQL statement of this command.
    pub sql: String,
}


#[derive(Debug)]
pub struct QueryResultsHandler {
    q_notifiers: HashMap<u64, Arc<Notify>>,
    q_results: HashMap<u64, Result<QueryResults, StoreError>>,
}
// Waits for execution of an SQL
// When it gets an input from rpc
// See query() async function
impl QueryResultsHandler {

    fn default() -> Self {
        Self { q_notifiers: HashMap::new(), q_results: HashMap::new()}
    }
    
    pub fn add_notifier(&mut self, id: u64, notifier: Arc<Notify>) {
        self.q_notifiers.insert(id, notifier);
    }

    pub fn add_result(&mut self, id: u64, result: Result<QueryResults, StoreError>) {
        if let Some(completion) = self.q_notifiers.remove(&(id)) {
            self.q_results.insert(id, result);
            completion.notify();
        }
    }

    pub fn remove_result(&mut self, id: &u64) -> Option<Result<QueryResults, StoreError>> {
        self.q_results.remove(id)
    }
}


/// Store configuration.
#[derive(Debug)]
struct StoreConfig {
    /// Connection pool size.
    conn_pool_size: usize,
    query_results: Arc<Mutex<QueryResultsHandler>>,
}

#[derive(Derivative)]
#[derivative(Debug)]
// Structure of Sequence paxos log (Local)
// Needed for recovery in case of crash and 
// cutting off too long logs (snapshot or trim, see omnipaxos documentation)
struct Store<S> where S: Snapshot<StoreCommand> { 
    /// ID of node in cluster
    id: u64,
    // All logged entries in-memory.
    log: Vec<StoreCommand>,
    /// Last promised round.
    n_prom: Ballot,
    /// Last accepted round.
    acc_round: Ballot,
    /// Length of the decided log.
    ld: u64,
    /// Garbage collected index.
    trimmed_idx: u64,
    /// Stored snapshot
    snapshot: Option<S>,
    /// Stored StopSign
    stopsign: Option<StopSignEntry>,
    // SQL
    #[derivative(Debug = "ignore")]
    conn_pool: Vec<Arc<Mutex<Connection>>>,
    conn_idx: usize,
    pending_transitions: Vec<StoreCommand>,

    // storing results
    query_results: Arc<Mutex<QueryResultsHandler>>,
}
// Implementation of Sequence paxos log (Local)
impl<S> Store<S> where S: Snapshot<StoreCommand> {
    pub fn new(id: u64, config: StoreConfig) -> Self {
        let mut conn_pool = vec![];
        let conn_pool_size = config.conn_pool_size;
        for _ in 0..conn_pool_size {
            let flags = OpenFlags::new()
                .set_read_write()
                .set_create()
                .set_no_mutex();
            let mut conn =
                Connection::open_with_flags(format!("node{}.db", id), flags).unwrap();
            conn.set_busy_timeout(5000).unwrap();
            conn_pool.push(Arc::new(Mutex::new(conn)));
        }
        let conn_idx = 0;
        Store {
            id,
            log: vec![],
            n_prom: Ballot::default(),
            acc_round: Ballot::default(),
            ld: 0,
            trimmed_idx: 0,
            snapshot: None,
            stopsign: None,
            conn_pool,
            conn_idx,
            pending_transitions: Vec::new(),
            query_results: config.query_results
        }
    }

    pub fn get_connection(&mut self) -> Arc<Mutex<Connection>> {
        let idx = self.conn_idx % self.conn_pool.len();
        let conn = &self.conn_pool[idx];
        self.conn_idx += 1;
        conn.clone()
    }
}

// Storage for sequence paxos
// Bunch of setters and getters to and from omnipaxos_core
impl<S> Storage<StoreCommand, S> for Store<S> where S: Snapshot<StoreCommand>
{
    fn append_entry(&mut self, entry: StoreCommand) -> u64 {
        self.log.push(entry);
        self.get_log_len()
    }

    fn append_entries(&mut self, entries: Vec<StoreCommand>) -> u64 {
        let mut e = entries;
        self.log.append(&mut e);
        self.get_log_len()
    }

    fn append_on_prefix(&mut self, from_idx: u64, entries: Vec<StoreCommand>) -> u64 {
        self.log.truncate(from_idx as usize);
        self.append_entries(entries)
    }

    fn set_promise(&mut self, n_prom: Ballot) {
        self.n_prom = n_prom;
    }

    fn set_decided_idx(&mut self, ld: u64) {
        
        let runqueries = self.log[(self.ld as usize)..(ld as usize)].to_vec();
        for q in runqueries.iter() {
            let conn = self.get_connection();
            let results = query(conn, q.sql.clone());

            let mut query_results = self.query_results.lock().unwrap();
            query_results.add_result(q.id as u64, results);
        }
        self.ld = ld;
    }

    fn get_decided_idx(&self) -> u64 {
        self.ld
    }

    fn set_accepted_round(&mut self, na: Ballot) {
        self.acc_round = na;
    }

    fn get_accepted_round(&self) -> Ballot {
        self.acc_round
    }

    fn get_entries(&self, from: u64, to: u64) -> &[StoreCommand] {
        self.log.get(from as usize..to as usize).unwrap_or(&[])
    }

    fn get_log_len(&self) -> u64 {
        self.log.len() as u64
    }

    fn get_suffix(&self, from: u64) -> &[StoreCommand] {
        match self.log.get(from as usize..) {
            Some(s) => s,
            None => &[],
        }
    }

    fn get_promise(&self) -> Ballot {
        self.n_prom
    }

    fn set_stopsign(&mut self, s: StopSignEntry) {
        self.stopsign = Some(s);
    }

    fn get_stopsign(&self) -> Option<StopSignEntry> {
        self.stopsign.clone()
    }

    fn trim(&mut self, trimmed_idx: u64) {
        self.log.drain(0..trimmed_idx as usize);
    }

    fn set_compacted_idx(&mut self, trimmed_idx: u64) {
        self.trimmed_idx = trimmed_idx;
    }

    fn get_compacted_idx(&self) -> u64 {
        self.trimmed_idx
    }

    fn set_snapshot(&mut self, snapshot: S) {
        self.snapshot = Some(snapshot);
    }

    fn get_snapshot(&self) -> Option<S> {
        self.snapshot.clone()
    }
}

// sql query execution 
fn query(conn: Arc<Mutex<Connection>>, sql: String) -> Result<QueryResults, StoreError> {
    let conn = conn.lock().unwrap();
    let mut rows = vec![];
    conn.iterate(sql, |pairs| {
        let mut row = QueryRow::new();
        for &(_, value) in pairs.iter() {
            row.values.push(value.unwrap().to_string());
        }
        rows.push(row);
        true
    })?;
    Ok(QueryResults { rows })
}

/// ChiselStore server.
#[derive(Derivative)]
#[derivative(Debug)]
pub struct StoreServer<T: StoreTransport + Send + Sync> {
    id: u64,
    command_id: AtomicU64,
    query_results: Arc<Mutex<QueryResultsHandler>>,
    #[derivative(Debug = "ignore")]
    sp: Arc<Mutex<SequencePaxos<StoreCommand, (), Store<()>>>>,
    #[derivative(Debug = "ignore")]
    ble: Arc<Mutex<BallotLeaderElection>>,
    sp_notifier_receiver: Receiver<Message<StoreCommand, ()>>,
    sp_notifier_sender: Sender<Message<StoreCommand, ()>>,
    ble_notifier_receiver: Receiver<BLEMessage>,
    ble_notifier_sender: Sender<BLEMessage>,
    transport: T,
}

/// Query row.
#[derive(Debug)]
pub struct QueryRow {
    /// Column values of the row.
    pub values: Vec<String>,
}

impl QueryRow {
    fn new() -> Self {
        QueryRow { values: Vec::new() }
    }
}

/// Query results.
#[derive(Debug)]
pub struct QueryResults {
    /// Query result rows.
    pub rows: Vec<QueryRow>,
}

impl<T: StoreTransport + Send + Sync> StoreServer<T> {
    /// Start a new server as part of a ChiselStore cluster.
    /// This is initiated once we start a new process (see gouged.rs)
    pub fn start(id: usize, peers: Vec<usize>, transport: T) -> Result<Self, StoreError> {

        let id = id as u64;
        let peers: Vec<u64> = peers.into_iter().map(|p| p as u64).collect();
        let command_id = AtomicU64::new(0);

        // Initiate Sequence Paxos 
        let configuration_id = 0;
        let mut sp_config = SequencePaxosConfig::default();
        // Setting values (seq paxos)
        sp_config.set_configuration_id(configuration_id);
        sp_config.set_pid(id);
        sp_config.set_peers(peers.to_vec()); 

        // Initiate query result handler
        let query_results = Arc::new(Mutex::new(QueryResultsHandler::default()));
        let config = StoreConfig{ 
            conn_pool_size: 20, 
            query_results: query_results.clone() 
        };
        // Initiate local store (with values required for recovery after crash and 
        // A bunch of other stuff
        let store = Store::new(id, config);
        let sp = Arc::new(Mutex::new(SequencePaxos::with(sp_config, store)));

        // Initiate ballot leader election 
        let mut ble_config = BLEConfig::default();
        // Setting ble values (omni paxos)
        ble_config.set_pid(id);
        ble_config.set_peers(peers);
        ble_config.set_hb_delay(1000); // can increase or decrease
        let ble = Arc::new(Mutex::new(BallotLeaderElection::with(ble_config)));

        // communication channels
        let (sp_notifier_sender, sp_notifier_receiver) = channel::unbounded();
        let (ble_notifier_sender, ble_notifier_receiver) = channel::unbounded();

        Ok(StoreServer {
            id,
            command_id,
            query_results,
            sp,
            ble,
            sp_notifier_receiver,
            sp_notifier_sender,
            ble_notifier_receiver,
            ble_notifier_sender,
            transport,
        })
    }

    /// Run the blocking event loop.
    pub fn run(&self) {
        loop {
            let mut sp = self.sp.lock().unwrap();
            let mut ble = self.ble.lock().unwrap();

            if let Some(leader) = ble.tick() {
                sp.handle_leader(leader);
            }

            // handle incoming messages
            match self.sp_notifier_receiver.try_recv() {
                Ok(msg) => {
                    sp.handle(msg);
                },
                _ => {}
            };

            match self.ble_notifier_receiver.try_recv() {
                Ok(msg) => {
                    ble.handle(msg);
                },
                _ => {}
            };

            // handle outgoing messages
            for msg in sp.get_outgoing_msgs() {
                let receiver = msg.to;
                self.transport.send_sp_to_rpc(receiver, msg);
            }

            for msg in ble.get_outgoing_msgs() {
                let receiver = msg.to;
                self.transport.send_ble_to_rpc(receiver,msg);
            }

            sleep(Duration::from_millis(1));
        }
    }

    // This function is called from rpc layer, i.e. the execute function
    // The execute function will wait for this to return
    // Ultimately, the execute function will return the SQL command
    pub async fn query<S: AsRef<str>>(&self, sql_statement: S) -> Result<QueryResults, StoreError> {
        
        let results = {
            let (notify, id) = {
                let id = self.command_id.fetch_add(1, Ordering::SeqCst);
                let command = StoreCommand {
                    id: id as usize,
                    sql: sql_statement.as_ref().to_string()
                };

                let notify = Arc::new(Notify::new());
                self.query_results.lock().unwrap().add_notifier(id, notify.clone());
                self.sp.lock().unwrap().append(command).expect("Failed to append");

                (notify, id)
            };

            notify.notified().await;
            let results = self.query_results.lock().unwrap().remove_result(&id).unwrap();

            results?
        };
        Ok(results)
    }
    
    /// Receive a sequence paxos message from the cluster.
    pub fn handle_sp_msg(&self, msg: Message<StoreCommand, ()>) {
        self.sp_notifier_sender.send(msg).unwrap();
    }

    /// Receive a ballot leader election message from the cluster.
    pub fn handle_ble_msg(&self, msg: BLEMessage) {
        self.ble_notifier_sender.send(msg).unwrap();
    }
}

// ---------------------------------------------------------------
// FIRST ATTEMPT: old server, not working atm
// TODO: REMOVE BELOW FOR FINAL COMMIT
// ---------------------------------------------------------------

/* 
//! ChiselStore server module.

use crate::errors::StoreError;
use async_notify::Notify;
use async_trait::async_trait;
use crossbeam_channel as channel;
use crossbeam_channel::{Receiver, Sender};
use derivative::Derivative;

/* 
use little_raft::{
    cluster::Cluster,
    message::Message,
    replica::{Replica, ReplicaID},
    state_machine::{StateMachine, StateMachineTransition, TransitionState},
}; */

use omnipaxos_core::{
    ballot_leader_election::{BLEConfig, BallotLeaderElection},
    sequence_paxos::{CompactionErr, ReconfigurationRequest, SequencePaxos, SequencePaxosConfig},
    storage::{memory_storage::MemoryStorage, Snapshot},
    util::LogEntry,
};
use sqlite::{Connection, OpenFlags};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// ChiselStore transport layer.
///
/// Your application should implement this trait to provide network access
/// to the ChiselStore server.
#[async_trait]
pub trait StoreTransport {
    /// Send a store command message `msg` to `to_id` node.
    fn send(&self, to_id: usize, msg: Message<StoreCommand>);


    /// Delegate command to another node.
    async fn delegate(
        &self,
        to_id: usize,
        sql: String,
        consistency: Consistency,
    ) -> Result<QueryResults, StoreError>;
}

/// Consistency mode.
#[derive(Debug)]
pub enum Consistency {
    /// Strong consistency. Both reads and writes go through the Raft leader,
    /// which makes them linearizable.
    Strong,
    /// Relaxed reads. Reads are performed on the local node, which relaxes
    /// read consistency and allows stale reads.
    RelaxedReads,
}

/// Store command.
///
/// A store command is a SQL statement that is replicated in the Raft cluster.
#[derive(Clone, Debug)]
pub struct StoreCommand {
    /// Unique ID of this command.
    pub id: usize,
    /// The SQL statement of this command.
    pub sql: String,
}


impl StateMachineTransition for StoreCommand {
    type TransitionID = usize;

    fn get_id(&self) -> Self::TransitionID {
        self.id
    }
}

/// Store configuration.
#[derive(Debug)]
struct StoreConfig {
    /// Connection pool size.
    conn_pool_size: usize,
}

#[derive(Derivative)]
#[derivative(Debug)]
struct Store<T: StoreTransport + Send + Sync> {
    /// ID of the node this Cluster objecti s on.
    this_id: usize,
    /// Is this node the leader?
    leader: Option<usize>,
    leader_exists: AtomicBool,
    waiters: Vec<Arc<Notify>>,
    /// Pending messages
    pending_messages: Vec<Message<StoreCommand>>,
    /// Transport layer.
    transport: Arc<T>,
    #[derivative(Debug = "ignore")]
    conn_pool: Vec<Arc<Mutex<Connection>>>,
    conn_idx: usize,
    pending_transitions: Vec<StoreCommand>,
    command_completions: HashMap<u64, Arc<Notify>>,
    results: HashMap<u64, Result<QueryResults, StoreError>>,
}

impl<T: StoreTransport + Send + Sync> Store<T> {
    pub fn new(this_id: usize, transport: T, config: StoreConfig) -> Self {
        let mut conn_pool = vec![];
        let conn_pool_size = config.conn_pool_size;
        for _ in 0..conn_pool_size {
            // FIXME: Let's use the 'memdb' VFS of SQLite, which allows concurrent threads
            // accessing the same in-memory database.
            let flags = OpenFlags::new()
                .set_read_write()
                .set_create()
                .set_no_mutex();
            let mut conn =
                Connection::open_with_flags(format!("node{}.db", this_id), flags).unwrap();
            conn.set_busy_timeout(5000).unwrap();
            conn_pool.push(Arc::new(Mutex::new(conn)));
        }
        let conn_idx = 0;
        Store {
            this_id,
            leader: None,
            leader_exists: AtomicBool::new(false),
            waiters: Vec::new(),
            pending_messages: Vec::new(),
            transport: Arc::new(transport),
            conn_pool,
            conn_idx,
            pending_transitions: Vec::new(),
            command_completions: HashMap::new(),
            results: HashMap::new(),
        }
    }

    pub fn is_leader(&self) -> bool {
        match self.leader {
            Some(id) => id == self.this_id,
            _ => false,
        }
    }

    pub fn get_connection(&mut self) -> Arc<Mutex<Connection>> {
        let idx = self.conn_idx % self.conn_pool.len();
        let conn = &self.conn_pool[idx];
        self.conn_idx += 1;
        conn.clone()
    }
}

fn query(conn: Arc<Mutex<Connection>>, sql: String) -> Result<QueryResults, StoreError> {
    let conn = conn.lock().unwrap();
    let mut rows = vec![];
    conn.iterate(sql, |pairs| {
        let mut row = QueryRow::new();
        for &(_, value) in pairs.iter() {
            row.values.push(value.unwrap().to_string());
        }
        rows.push(row);
        true
    })?;
    Ok(QueryResults { rows })
}

impl<T: StoreTransport + Send + Sync> StateMachine<StoreCommand> for Store<T> {
    fn register_transition_state(&mut self, transition_id: usize, state: TransitionState) {
        if state == TransitionState::Applied {
            if let Some(completion) = self.command_completions.remove(&(transition_id as u64)) {
                completion.notify();
            }
        }
    }

    fn apply_transition(&mut self, transition: StoreCommand) {
        if transition.id == NOP_TRANSITION_ID {
            return;
        }
        let conn = self.get_connection();
        let results = query(conn, transition.sql);
        if self.is_leader() {
            self.results.insert(transition.id as u64, results);
        }
    }

    fn get_pending_transitions(&mut self) -> Vec<StoreCommand> {
        let cur = self.pending_transitions.clone();
        self.pending_transitions = Vec::new();
        cur
    }
}

impl<T: StoreTransport + Send + Sync> Cluster<StoreCommand> for Store<T> {
    fn register_leader(&mut self, leader_id: Option<ReplicaID>) {
        if let Some(id) = leader_id {
            self.leader = Some(id);
            self.leader_exists.store(true, Ordering::SeqCst);
        } else {
            self.leader = None;
            self.leader_exists.store(false, Ordering::SeqCst);
        }
        let waiters = self.waiters.clone();
        self.waiters = Vec::new();
        for waiter in waiters {
            waiter.notify();
        }
    }

    fn send_message(&mut self, to_id: usize, message: Message<StoreCommand>) {
        self.transport.send(to_id, message);
    }
    
d
    fn receive_messages(&mut self) -> Vec<Message<StoreCommand>> {
        let cur = self.pending_messages.clone();
        self.pending_messages = Vec::new();
        cur
    }

    fn halt(&self) -> bool {
        false
    }
}

type StoreReplica<T> = Replica<Store<T>, StoreCommand, Store<T>>;

/// ChiselStore server.
#[derive(Derivative)]
#[derivative(Debug)]
pub struct StoreServer<T: StoreTransport + Send + Sync> {
    next_cmd_id: AtomicU64,
    store: Arc<Mutex<Store<T>>>,
    #[derivative(Debug = "ignore")]
    replica: Arc<Mutex<StoreReplica<T>>>,
    message_notifier_rx: Receiver<()>,
    message_notifier_tx: Sender<()>,
    transition_notifier_rx: Receiver<()>,
    transition_notifier_tx: Sender<()>,
}

/// Query row.
#[derive(Debug)]
pub struct QueryRow {
    /// Column values of the row.
    pub values: Vec<String>,
}

impl QueryRow {
    fn new() -> Self {
        QueryRow { values: Vec::new() }
    }
}

/// Query results.
#[derive(Debug)]
pub struct QueryResults {
    /// Query result rows.
    pub rows: Vec<QueryRow>,
}

const NOP_TRANSITION_ID: usize = 0;
const HEARTBEAT_TIMEOUT: Duration = Duration::from_millis(500);
const MIN_ELECTION_TIMEOUT: Duration = Duration::from_millis(750);
const MAX_ELECTION_TIMEOUT: Duration = Duration::from_millis(950);

impl<T: StoreTransport + Send + Sync> StoreServer<T> {
    /// Start a new server as part of a ChiselStore cluster.
    pub fn start(this_id: usize, peers: Vec<usize>, transport: T) -> Result<Self, StoreError> {
        let config = StoreConfig { conn_pool_size: 20 };
        let store = Arc::new(Mutex::new(Store::new(this_id, transport, config)));
        let noop = StoreCommand {
            id: NOP_TRANSITION_ID,
            sql: "".to_string(),
        };
        let (message_notifier_tx, message_notifier_rx) = channel::unbounded();
        let (transition_notifier_tx, transition_notifier_rx) = channel::unbounded();
        let replica = Replica::new(
            this_id,
            peers,
            store.clone(),
            store.clone(),
            noop,
            HEARTBEAT_TIMEOUT,
            (MIN_ELECTION_TIMEOUT, MAX_ELECTION_TIMEOUT),
        );
        let replica = Arc::new(Mutex::new(replica));
        Ok(StoreServer {
            next_cmd_id: AtomicU64::new(1), // zero is reserved for no-op.
            store,
            replica,
            message_notifier_rx,
            message_notifier_tx,
            transition_notifier_rx,
            transition_notifier_tx,
        })
    }

    /// Run the blocking event loop.
    pub fn run(&self) {
        self.replica.lock().unwrap().start(
            self.message_notifier_rx.clone(),
            self.transition_notifier_rx.clone(),
        );
    }

    /// Execute a SQL statement on the ChiselStore cluster.
    pub async fn query<S: AsRef<str>>(
        &self,
        stmt: S,
        consistency: Consistency,
    ) -> Result<QueryResults, StoreError> {
        // If the statement is a read statement, let's use whatever
        // consistency the user provided; otherwise fall back to strong
        // consistency.
        let consistency = if is_read_statement(stmt.as_ref()) {
            consistency
        } else {
            Consistency::Strong
        };
        let results = match consistency {
            Consistency::Strong => {
                self.wait_for_leader().await;
                let (delegate, leader, transport) = {
                    let store = self.store.lock().unwrap();
                    (!store.is_leader(), store.leader, store.transport.clone())
                };
                if delegate {
                    if let Some(leader_id) = leader {
                        return transport
                            .delegate(leader_id, stmt.as_ref().to_string(), consistency)
                            .await;
                    }
                    return Err(StoreError::NotLeader);
                }
                let (notify, id) = {
                    let mut store = self.store.lock().unwrap();
                    let id = self.next_cmd_id.fetch_add(1, Ordering::SeqCst);
                    let cmd = StoreCommand {
                        id: id as usize,
                        sql: stmt.as_ref().to_string(),
                    };
                    let notify = Arc::new(Notify::new());
                    store.command_completions.insert(id, notify.clone());
                    store.pending_transitions.push(cmd);
                    (notify, id)
                };
                self.transition_notifier_tx.send(()).unwrap();
                notify.notified().await;
                let results = self.store.lock().unwrap().results.remove(&id).unwrap();
                results?
            }
            Consistency::RelaxedReads => {
                let conn = {
                    let mut store = self.store.lock().unwrap();
                    store.get_connection()
                };
                query(conn, stmt.as_ref().to_string())?
            }
        };
        Ok(results)
    }

    /// Wait for a leader to be elected.
    pub async fn wait_for_leader(&self) {
        loop {
            let notify = {
                let mut store = self.store.lock().unwrap();
                if store.leader_exists.load(Ordering::SeqCst) {
                    break;
                }
                let notify = Arc::new(Notify::new());
                store.waiters.push(notify.clone());
                notify
            };
            if self
                .store
                .lock()
                .unwrap()
                .leader_exists
                .load(Ordering::SeqCst)
            {
                break;
            }
            // TODO: add a timeout and fail if necessary
            notify.notified().await;
        }
    }

    /// Receive a message from the ChiselStore cluster.
    pub fn recv_msg(&self, msg: little_raft::message::Message<StoreCommand>) {
        let mut cluster = self.store.lock().unwrap();
        cluster.pending_messages.push(msg);
        self.message_notifier_tx.send(()).unwrap();
    }
}

fn is_read_statement(stmt: &str) -> bool {
    stmt.to_lowercase().starts_with("select")
}

 ?*/

// ------------------------------