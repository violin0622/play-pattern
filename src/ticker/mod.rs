#![allow(dead_code)]
#![allow(unused_variables)]

use std::{
    cmp::min,
    collections::{HashMap, HashSet},
    hash::Hash,
};

struct Core<'a, A>
where
    A: ID,
{
    id: A,
    term: u64,
    peers: HashSet<A>,
    voted_for: Option<A>,
    logs: Vec<LogEntry<'a>>,
    cfg: RaftConfig,

    // Volatile state
    tick: u64,
    commited_idx: usize,
    last_append_idx: usize,
}

impl<'a, A> Core<'a, A>
where
    A: ID,
{
    pub fn new(
        term: u64,
        cfg: RaftConfig,
        id: A,
        peers: Vec<A>,
        voted_for: Option<A>,
        logs: Vec<LogEntry<'a>>,
    ) -> Self {
        Self {
            term,
            id,
            cfg,
            logs,
            voted_for,
            tick: 0,
            commited_idx: 0,
            last_append_idx: 0,
            peers: peers.into_iter().collect(),
        }
    }
}

#[derive(Debug, Default, Clone, Copy)]
struct RaftConfig {
    election_timeout: u64,
}

#[derive(Debug, Clone, Default)]
struct PersistStates<A>
where
    A: ID,
{
    id: A,
    term: u64,
    peers: HashSet<A>,
    voted_for: Option<A>,
}

struct VolatileStates {
    commited_idx: usize,
    last_append_idx: usize,
}

struct Raft<'a, A>
where
    A: ID,
{
    core: Core<'a, A>,
    role: RaftRole<A>,
}

struct RaftV2<'a, A>
where
    A: ID,
{
    vs: VolatileStates,
    ps: PersistStates<A>,
    logs: Vec<LogEntry<'a>>,
}

struct OutputV2<'a, A>
where
    A: ID,
{
    req: Option<Vec<Message<Req<'a>, A>>>,
    rsp: Option<Message<Rsp, A>>,
    commit: Option<Vec<LogEntry<'a>>>,
    vs: Option<VolatileStates>,
}

type OutputResult<'a, A> = std::result::Result<OutputV2<'a, A>, Box<dyn std::error::Error>>;

trait ID: Default + PartialEq + Eq + Hash + Copy + std::fmt::Debug + std::fmt::Display {}

impl<'a, A> Raft<'a, A>
where
    A: ID,
{
    fn new(
        term: u64,
        cfg: RaftConfig,
        id: A,
        peers: Vec<A>,
        voted_for: Option<A>,
        logs: Vec<LogEntry<'a>>,
    ) -> Self {
        Self {
            core: Core::new(term, cfg, id, peers, voted_for, logs),
            role: RaftRole::new(),
        }
    }

    fn run(&mut self, input: Inputs<'a, A>) -> Output<A> {
        self.role.maybe_update_term(&mut self.core, &input);
        let mut output = Output::default();
        if let Some(msg) = self.role.maybe_deny_req(&self.core, &input) {
            output.rsp = Some(msg);
            return output;
        }
        if let Some(output) = self.role.maybe_ignore_rsp(&self.core, &input) {
            return output;
        }

        match input {
            Inputs::Tick => {}
            Inputs::Req(msg) => {
                output.rsp = match msg.msg {
                    Req::AppendEntries(req) => {
                        let rsp = match &mut self.role {
                            RaftRole::Leader(leader) => {
                                leader.handle_append_entries_req(&mut self.core, req)
                            }
                            RaftRole::Follower(follower) => {
                                follower.handle_append_entries_req(&mut self.core, req)
                            }
                        };
                        Some(Message {
                            msg: Rsp::AppendEntries(rsp),
                            to: msg.from,
                            from: msg.to,
                        })
                    }
                    Req::RequestVote(req) => {
                        let rsp = match &mut self.role {
                            RaftRole::Leader(leader) => {
                                leader.handle_request_vote_req(&mut self.core, req)
                            }
                            RaftRole::Follower(follower) => {
                                follower.handle_request_vote_req(&mut self.core, req, msg.from)
                            }
                        };
                        Some(Message {
                            to: msg.from,
                            from: msg.to,
                            msg: Rsp::RequestVote(rsp),
                        })
                    }
                };
            }
            Inputs::Rsp(msg) => {
                match (&mut self.role, msg.msg) {
                    (RaftRole::Leader(leader), Rsp::AppendEntries(rsp)) => {
                        leader.handle_append_entries_rsp(&mut self.core, rsp, msg.from)
                    }
                    (RaftRole::Leader(leader), Rsp::RequestVote(_)) => {
                        leader.handle_request_vote_rsp()
                    }
                    (RaftRole::Follower(follower), Rsp::AppendEntries(_)) => {
                        follower.handle_append_entries_rsp()
                    }
                    (RaftRole::Follower(follower), Rsp::RequestVote(_)) => {
                        follower.handle_request_vote_rsp()
                    }
                };
            }
            Inputs::Proposql(items) => {
                let RaftRole::Leader(leader) = &self.role else {
                    panic!("only leader can handle proposal")
                };
                output = leader.proposal(&mut self.core, items);
            }
        }

        output
    }

    fn run_v2(&mut self, input: Inputs<'a, A>) -> Output<'a, A> {
        match (input, &mut self.role) {
            (Inputs::Req(message), RaftRole::Leader(leader)) => todo!(),
            (Inputs::Req(message), RaftRole::Follower(follower)) => todo!(),
            (Inputs::Rsp(message), RaftRole::Leader(leader)) => todo!(),
            (Inputs::Rsp(message), RaftRole::Follower(follower)) => todo!(),
            (Inputs::Tick, RaftRole::Leader(leader)) => todo!(),
            (Inputs::Tick, RaftRole::Follower(follower)) => todo!(),
            (Inputs::Proposql(items), RaftRole::Leader(leader)) => {
                leader.proposal(&mut self.core, items)
            }
            (Inputs::Proposql(items), RaftRole::Follower(follower)) => todo!(),
        }
    }
}

trait Logs {
    fn read(&self, begin: usize, end: usize) -> &[LogEntry];
    fn append(&mut self, logs: &[LogEntry]);
    fn truncate(&mut self, begin: usize);
}

#[derive(Debug, PartialEq, Eq)]
struct Message<B, A: ID> {
    to: A,
    from: A,
    msg: B,
}

struct Tick;
#[derive(Debug, Default, PartialEq, Eq)]
struct AppendEntriesReq<'a> {
    term: u64,
    // leader_id: u64,
    prev_log_index: u64,
    prev_log_term: u64,
    leader_commit: u64,
    entries: Vec<LogEntry<'a>>,
}

#[derive(Debug)]
struct AppendEntriesRsp {
    ok: bool,
    match_idx: u64,
    term: u64,
}

#[derive(Debug)]
enum Inputs<'a, A>
where
    A: ID,
{
    Req(Message<Req<'a>, A>),
    Rsp(Message<Rsp, A>),
    Tick,
    Proposql(&'a [u8]),
}

#[derive(Debug, PartialEq, Eq)]
enum Req<'a> {
    AppendEntries(AppendEntriesReq<'a>),
    RequestVote(RequestVoteReq),
}

#[derive(Debug, Default, PartialEq, Eq)]
struct RequestVoteReq {
    term: u64,
    last_log_term: u64,
    last_log_index: usize,
}

#[derive(Debug)]
struct RequestVoteRsp {
    term: u64,
    granted: bool,
}

#[derive(Debug)]
enum Rsp {
    AppendEntries(AppendEntriesRsp),
    RequestVote(RequestVoteRsp),
}

#[derive(Debug, Default)]
struct Output<'a, A: ID> {
    req: Option<Vec<Message<Req<'a>, A>>>,
    rsp: Option<Message<Rsp, A>>,
    commit: Option<Vec<LogEntry<'a>>>,
}

#[derive(Debug, PartialEq, Eq, Clone)]
struct LogEntry<'a> {
    term: u64,
    idx: u64,
    data: &'a [u8],
}

struct Want {}

#[cfg(test)]
mod test {
    use super::{Inputs, Raft};
    use crate::ticker::{AppendEntriesReq, LogEntry, Message, RaftConfig, Req};

    #[test]
    fn proposal() {
        let cfg = RaftConfig {
            election_timeout: 10,
        };
        let mut raft = Raft::new(1, cfg, 1, vec![2, 3], None, vec![]);

        let data = &[0, 1, 2];
        let output = raft.run(Inputs::Proposql(data));

        let expect = vec![Message {
            to: 2,
            from: 1,
            msg: Req::AppendEntries(AppendEntriesReq {
                term: 1,
                prev_log_index: 0,
                prev_log_term: 1,
                leader_commit: 0,
                entries: vec![LogEntry {
                    data,
                    term: 1,
                    idx: 1,
                }],
            }),
        }];

        assert!(output.req.is_some());
        assert_eq!(
            output
                .req
                .unwrap()
                .iter()
                .inspect(|x| println!("{x:?}"))
                .zip(&expect)
                .filter(|&(a, b)| *a == *b)
                .count(),
            expect.len(),
        );
    }

    fn vector() {
        let mut v = vec![];
        let a = std::time::Instant::now();
        v.push(&a);

        println!("{:?}", v[0]);
    }
}

impl ID for u64 {}

#[derive(Clone)]
struct Leader<A: ID> {
    // Volatile state on leader
    next_idx: Option<HashMap<A, usize>>, // init as [1:3] when become leader.
    match_idx: Option<HashMap<A, usize>>,
}

impl<A: ID> Leader<A> {
    fn new(peers: &HashSet<A>) -> Self {
        let mi = peers.clone().into_iter().map(|x| (x, 1)).collect();
        let ni = peers.clone().into_iter().map(|x| (x, 0)).collect();
        Self {
            next_idx: Some(ni),
            match_idx: Some(mi),
        }
    }
}

impl<A: ID> Leader<A> {
    fn handle_append_entries_req(&mut self, core: &mut Core<'_, A>, req: AppendEntriesReq) -> ! {
        unreachable!("leader shouldn't handle append entries request!");
    }

    fn handle_append_entries_rsp<'a>(
        &mut self,
        core: &mut Core<'a, A>,
        rsp: AppendEntriesRsp,
        from: A,
    ) -> Output<'a, A> {
        if rsp.ok {
            self.handle_append_entries_rsp_ok(core, rsp, from)
        } else {
            self.handle_append_entries_rsp_bad(core, rsp, from)
        }
    }

    fn agree(&self, core: &Core<'_, A>, idx: usize) -> bool {
        let Some(m) = &self.match_idx else {
            return false;
        };

        for peer in &core.peers {
            if m[peer] < idx {
                return false;
            }
        }

        true
    }

    fn agree_index(&self, core: &Core<'_, A>) -> usize {
        let l = core.logs.len();
        let Some(i) = core.logs.iter().rev().enumerate().find(|(i, log)| {
            let idx = l - i - 1;
            self.agree(core, idx)
        }) else {
            return 0;
        };
        l - i.0 - 1
    }

    fn handle_append_entries_rsp_ok<'a>(
        &mut self,
        core: &mut Core<'a, A>,
        rsp: AppendEntriesRsp,
        from: A,
    ) -> Output<'a, A> {
        let mut output = Output::<'a, A>::default();
        if let Some(m) = &mut self.next_idx {
            m.insert(from, rsp.match_idx as usize + 1);
        }
        if let Some(m) = &mut self.match_idx {
            m.insert(from, rsp.match_idx as usize);
        }

        if self.agree_index(core) > core.commited_idx {
            let commit = core.logs[core.commited_idx..=self.agree_index(core)].to_vec();
            output.commit = Some(commit);
        }

        output
    }

    fn handle_append_entries_rsp_bad<'a>(
        &mut self,
        core: &mut Core<'a, A>,
        rsp: AppendEntriesRsp,
        from: A,
    ) -> Output<'a, A> {
        if let Some(m) = &mut self.next_idx {
            m.entry(from).and_modify(|v| *v -= 1).or_insert(1);
        }

        let prev_idx = self.next_idx.as_ref().map_or(0, |m| m[&from] - 1);
        let last_entry_idx = min(core.logs.len(), prev_idx + 1);

        let msg = Message {
            from: core.id,
            to: from,
            msg: Req::AppendEntries(AppendEntriesReq {
                term: core.term,
                prev_log_index: prev_idx as u64,
                prev_log_term: core.logs[prev_idx].term,
                leader_commit: core.commited_idx as u64,
                entries: core.logs[prev_idx..last_entry_idx].to_vec(),
            }),
        };
        Output {
            req: Some(vec![msg]),
            rsp: None,
            commit: None,
        }
    }

    fn handle_request_vote_req(&mut self, core: &mut Core<'_, A>, req: RequestVoteReq) -> ! {
        unreachable!("leader should not process request vote");
    }

    fn handle_request_vote_rsp<'a>(&mut self) -> Output<'a, A> {
        Output {
            req: None,
            rsp: None,
            commit: None,
        }
    }

    fn proposal<'a>(&self, core: &mut Core<'a, A>, n: &'a [u8]) -> Output<'a, A> {
        core.logs.push(LogEntry {
            term: core.term,
            idx: 1,
            data: n,
        });

        let reqs = core
            .peers
            .iter()
            .map(|peer| {
                let prev_idx = self.next_idx.as_ref().map_or(0, |m| m[peer] - 1);
                let last_entry_idx = min(core.logs.len(), prev_idx + 1);

                Message {
                    from: core.id,
                    to: *peer,
                    msg: Req::AppendEntries(AppendEntriesReq {
                        term: core.term,
                        prev_log_index: prev_idx as u64,
                        prev_log_term: core.logs[prev_idx].term,
                        leader_commit: core.commited_idx as u64,
                        entries: core.logs[prev_idx..last_entry_idx].to_vec(),
                    }),
                }
            })
            .collect();
        Output {
            req: Some(reqs),
            rsp: None,
            commit: None,
        }
    }
}

enum RaftRole<A>
where
    A: ID,
{
    Leader(Leader<A>),
    Follower(Follower),
}

struct Follower {
    last_append_entries_req: u64,
}

impl Follower {
    fn new() -> Self {
        Self {
            last_append_entries_req: 0,
        }
    }
}

impl Follower {
    fn append_entries_ok(&self, req: AppendEntriesReq<'_>) -> AppendEntriesRsp {
        AppendEntriesRsp {
            ok: true,
            term: req.term,
            match_idx: req.prev_log_index + req.entries.len() as u64,
        }
    }

    fn append_entries_bad<A: ID>(&self, core: &Core<'_, A>) -> AppendEntriesRsp {
        AppendEntriesRsp {
            match_idx: 0,
            ok: false,
            term: core.term,
        }
    }

    fn handle_append_entries_req<'a, A: ID>(
        &mut self,
        core: &mut Core<'a, A>,
        req: AppendEntriesReq<'a>,
    ) -> AppendEntriesRsp {
        if req.term < core.term {
            return self.append_entries_bad(core);
        }
        match (req.prev_log_index as usize, core.logs.len()) {
            (0, 0) => self.append_entries_ok(req),
            (0, llen) if llen > 0 => {
                core.logs.clear();
                core.logs.append(&mut req.entries.clone());
                self.append_entries_ok(req)
            }
            (pidx, 0) if pidx > 0 => self.append_entries_bad(core),
            (pidx, llen) => {
                if llen <= pidx {
                    return self.append_entries_bad(core);
                }
                if req.prev_log_term != core.logs[pidx].term {
                    return self.append_entries_bad(core);
                }
                if llen > pidx + 1 {
                    core.logs.truncate(pidx + 1);
                }
                core.logs.append(&mut req.entries.clone());
                self.append_entries_ok(req)
            }
        }
    }

    fn handle_append_entries_rsp<'a, A: ID>(&self) -> Output<'a, A> {
        Output {
            req: None,
            rsp: None,
            commit: None,
        }
    }

    fn handle_request_vote_rsp<'a, A: ID>(&self) -> Output<'a, A> {
        Output {
            req: None,
            rsp: None,
            commit: None,
        }
    }

    fn handle_request_vote_req<A: ID>(
        &mut self,
        core: &mut Core<'_, A>,
        req: RequestVoteReq,
        from: A,
    ) -> RequestVoteRsp {
        let last_term = core.logs.last().map_or(0, |e| e.term);
        let log_ok = req.last_log_term > last_term
            || (req.last_log_term == last_term && req.last_log_index >= core.logs.len());
        let grant = log_ok && core.voted_for.is_none_or(|x| x == from);

        if grant {
            core.voted_for = Some(from);
        }
        RequestVoteRsp {
            term: core.term,
            granted: grant,
        }
    }
}

struct RoleOutput<'a, A>
where
    A: ID,
{
    role: RaftRole<A>,
    msg: Output<'a, A>,
}

impl<A: ID> RaftRole<A> {
    fn new() -> Self {
        Self::Follower(Follower::new())
    }

    fn maybe_update_term<'a>(&mut self, core: &mut Core<'a, A>, input: &Inputs<'a, A>) {
        let Inputs::Req(Message { msg, .. }) = input else {
            return;
        };
        let Req::AppendEntries(req) = &msg else {
            return;
        };
        if req.term <= core.term {
            return;
        }
        core.term = req.term;
        core.voted_for = None;
        *self = RaftRole::Follower(Follower::new());
    }

    fn maybe_ignore_rsp<'a>(
        &self,
        core: &Core<'a, A>,
        input: &Inputs<'a, A>,
    ) -> Option<Output<'a, A>> {
        let Inputs::Rsp(Message { msg, .. }) = input else {
            return None;
        };

        match msg {
            Rsp::AppendEntries(rsp) if rsp.term < core.term => Some(Output::default()),
            Rsp::RequestVote(rsp) if rsp.term < core.term => Some(Output::default()),
            _ => None,
        }
    }

    fn maybe_deny_req<'a>(
        &self,
        core: &Core<'a, A>,
        input: &Inputs<'a, A>,
    ) -> Option<Message<Rsp, A>> {
        let Inputs::Req(Message { msg, from, to }) = input else {
            return None;
        };

        match msg {
            Req::AppendEntries(req) if req.term < core.term => Some(Message {
                to: *from,
                from: *to,
                msg: Rsp::AppendEntries(AppendEntriesRsp {
                    ok: false,
                    match_idx: 0,
                    term: core.term,
                }),
            }),
            Req::RequestVote(req) if req.term < core.term => Some(Message {
                to: *from,
                from: *to,
                msg: Rsp::RequestVote(RequestVoteRsp {
                    term: core.term,
                    granted: false,
                }),
            }),
            _ => None,
        }
    }
}
