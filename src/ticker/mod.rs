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
    term: u64,
    tick: u64,
    id: A,
    peers: HashSet<A>,
    voted_for: Option<A>,
    logs: Vec<LogEntry<'a>>,

    // Volatile state
    commited_idx: usize,
    last_append_idx: usize,
}

impl<'a, A> Core<'a, A>
where
    A: ID,
{
    pub fn new(
        term: u64,
        id: A,
        peers: Vec<A>,
        voted_for: Option<A>,
        logs: Vec<LogEntry<'a>>,
    ) -> Self {
        Self {
            term,
            id,
            logs,
            voted_for,
            tick: 0,
            commited_idx: 0,
            last_append_idx: 0,
            peers: peers.into_iter().collect(),
        }
    }
}

struct Raft<'a, A>
where
    A: ID,
{
    core: Core<'a, A>,
    role: RaftRole<A>,
}

trait ID: Default + PartialEq + Eq + Hash + Copy + std::fmt::Debug + std::fmt::Display {}

impl<'a, A> Raft<'a, A>
where
    A: ID,
{
    fn new(term: u64, id: A, peers: Vec<A>, voted_for: Option<A>, logs: Vec<LogEntry<'a>>) -> Self {
        Self {
            core: Core::new(term, id, peers, voted_for, logs),
            role: RaftRole::new(),
        }
    }

    fn run(&mut self, input: Inputs<'a, A>) -> Output<A> {
        self.role.maybe_update_term(&mut self.core, &input);
        if let Some(msg) = self.role.maybe_deny_req(&self.core, &input) {
            return Output {
                req: None,
                rsp: Some(msg),
                commit: None,
            };
        }
        if let Some(output) = self.role.maybe_ignore_rsp(&self.core, &input) {
            return output;
        }

        let mut output = Output {
            req: None,
            rsp: None,
            commit: None,
        };
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
                    Req::RequestVote(req) => todo!(),
                };
            }
            Inputs::Rsp(msg) => {
                match (&mut self.role, msg.msg) {
                    (RaftRole::Leader(leader), Rsp::AppendEntries(rsp)) => {
                        leader.handle_append_entries_rsp(&mut self.core, rsp, msg.from)
                    }
                    (RaftRole::Leader(leader), Rsp::RequestVote(request_vote_rsp)) => todo!(),
                    (RaftRole::Follower(follower), Rsp::AppendEntries(append_entries_rsp)) => {
                        todo!()
                    }
                    (RaftRole::Follower(follower), Rsp::RequestVote(request_vote_rsp)) => todo!(),
                };
            }
            Inputs::Proposql(items) => todo!(),
        }
        output
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

// struct Inputs<'a, A>
// where
//     A: ID,
// {
//     tick: u64, //only 1 supported
//     req: Option<Message<Req<'a>, A>>,
//     rsp: Option<Message<Rsp, A>>,
//     proposal: &'a [u8],
// }

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
    use super::{Core, Input, Raft};
    use crate::ticker::{AppendEntriesReq, LogEntry, Message, Req};

    #[test]
    fn proposal() {
        let mut core = Raft::new(1, 1, vec![2, 3], None, vec![]);

        let data = &[0, 1, 2];
        let output = core.run(Input::Proposql(data));

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
}

impl ID for u64 {}

#[derive(Clone)]
struct Leader<A: ID> {
    // Volatile state on leader
    next_idx: Option<HashMap<A, u64>>, // init as [1:3] when become leader.
    match_idx: Option<HashMap<A, u64>>,
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
    fn handle_append_entries_req<'a>(
        &mut self,
        core: &mut Core<'a, A>,
        req: AppendEntriesReq,
    ) -> ! {
        unreachable!("leader shouldn't handle append entries request!");
    }

    fn handle_append_entries_rsp<'a>(
        &mut self,
        core: &mut Core<'a, A>,
        rsp: AppendEntriesRsp,
        from: A,
    ) -> Output<'a, A> {
        if rsp.ok {
            if let Some(m) = &mut self.next_idx {
                m.entry(from).and_modify(|v| *v += 1).or_insert(1);
            }
            if let Some(m) = &mut self.match_idx {
                m.insert(from, rsp.match_idx);
            }
        }

        if let Some(m) = &mut self.next_idx {
            m.entry(from).and_modify(|v| *v -= 1);
        }
        Output {
            req: Some(),
            rsp: None,
            commit: None,
        }
    }

    fn handle_append_entries_ok<'a>(
        &self,
        core: &mut Core<'a, A>,
        rsp: AppendEntriesRsp,
    ) -> Option<Vec<LogEntry<'_>>> {
        None
    }

    fn handle_request_vote_req<'a>(
        &mut self,
        core: &mut Core<'a, A>,
        input: Input,
    ) -> RoleOutput<'a, A> {
        RoleOutput {
            role: RaftRole::Follower(Follower::new()),
            msg: Output {
                req: None,
                rsp: None,
                commit: None,
            },
        }
    }
    fn handle_request_vote_rsp<'a>(
        &mut self,
        core: &mut Core<'a, A>,
        input: Input,
    ) -> RoleOutput<'a, A> {
        RoleOutput {
            role: RaftRole::Follower(Follower::new()),
            msg: Output {
                req: None,
                rsp: None,
                commit: None,
            },
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
                let last_entry_idx = min(core.logs.len(), prev_idx as usize + 1);

                Message {
                    from: core.id,
                    to: *peer,
                    msg: Req::AppendEntries(AppendEntriesReq {
                        term: core.term,
                        prev_log_index: prev_idx,
                        prev_log_term: core.logs[prev_idx as usize].term,
                        leader_commit: core.commited_idx as u64,
                        entries: core.logs[prev_idx as usize..last_entry_idx].to_vec(),
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

struct Follower {}
impl Follower {
    fn new() -> Self {
        Self {}
    }
}

impl Follower {
    fn handle<'a, A: ID>(&mut self, core: &mut Core<'a, A>, _in: Input) -> RoleOutput<'a, A> {
        if true {
            RoleOutput {
                role: RaftRole::Follower(Follower::new()),
                msg: Output {
                    req: None,
                    rsp: None,
                    commit: None,
                },
            }
        } else {
            RoleOutput {
                role: RaftRole::Leader(Leader::new(&core.peers)),
                msg: Output {
                    req: None,
                    rsp: None,
                    commit: None,
                },
            }
        }
    }

    fn append_entries_ok<'a>(&self, req: AppendEntriesReq<'a>) -> AppendEntriesRsp {
        AppendEntriesRsp {
            ok: true,
            term: req.term,
            match_idx: req.prev_log_index + req.entries.len() as u64,
        }
    }

    fn append_entries_bad<'a, A: ID>(&self, core: &Core<'a, A>) -> AppendEntriesRsp {
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
            (0, 0) => return self.append_entries_ok(req),
            (0, llen) if llen > 0 => {
                core.logs.clear();
                core.logs.append(&mut req.entries.clone());
                return self.append_entries_ok(req);
            }
            (pidx, 0) if pidx > 0 => return self.append_entries_bad(core),
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
                return self.append_entries_ok(req);
            }
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
        if let Inputs::Req(msg) = input {
            if let Req::AppendEntries(req) = &msg.msg {
                if req.term > core.term {
                    core.term = req.term;
                    core.voted_for = None;
                    *self = RaftRole::Follower(Follower::new());
                }
            }
        }
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
