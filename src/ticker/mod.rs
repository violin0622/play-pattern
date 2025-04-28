#![allow(dead_code)]

use std::{
    cmp::min,
    collections::{HashMap, HashSet},
    hash::Hash,
};

#[derive(Default, Debug)]
pub struct Core<'a, A>
where
    A: ID,
{
    term: u64,
    tick: u64,
    id: A,
    peers: HashSet<A>,
    voted_for: Vec<A>,
    logs: Vec<LogEntry<'a>>,

    // Volatile state
    commited_idx: u64,
    last_append_idx: u64,

    // Volatile state on leader
    next_idx: Option<HashMap<A, u64>>, // init as [1:3] when become leader.
    match_idx: Option<HashMap<A, u64>>,
}

trait ID: PartialEq + Eq + Hash + std::fmt::Debug + std::fmt::Display {}

impl<'a, A> Core<'a, A>
where
    A: ID,
{
    fn new(term: u64, id: A, peers: Vec<A>, voted_for: Vec<A>, logs: Vec<LogEntry<'a>>) -> Self {
        let set: HashSet<A> = peers.into_iter().collect();
        Self {
            term,
            id,
            voted_for,
            logs,

            peers: set,
            tick: 0,
            commited_idx: 0,
            last_append_idx: 0,
            next_idx: None,
            match_idx: None,
        }
    }

    fn run(&mut self, input: Input<'a>) -> Output {
        match input {
            Input::Tick => {
                self.tick += 1;
                println!("tick");
                Output {
                    req: None,
                    rsp: None,
                    commit: None,
                }
            }

            Input::Req(Req::AppendEntries(req)) => {
                println!("append entries request: {req:?}");
                Output {
                    req: None,
                    rsp: None,
                    commit: None,
                }
            }

            Input::Rsp(Rsp::AppendEntries(rsp)) => {
                println!("append entries response: {rsp:?}");
                Output {
                    req: None,
                    rsp: None,
                    commit: None,
                }
            }
            Input::Proposql(n) => {
                println!("{:?}", n);

                self.logs.push(LogEntry {
                    term: self.term,
                    idx: 1,
                    data: n,
                });
                let reqs = self
                    .peers
                    .iter()
                    .map(|peer| {
                        let prev_log_index =
                            self.next_idx.as_ref().map_or(0, |x| x[*peer as usize - 2]) - 1;
                        let last_entry_index = min(self.logs.len(), prev_log_index as usize + 1);
                        Message {
                            from: self.id,
                            to: *peer,
                            msg: Req::AppendEntries(AppendEntriesReq {
                                term: self.term,
                                prev_log_index,
                                prev_log_term: self.logs[prev_log_index as usize].term,
                                leader_commit: self.commited_idx,
                                entries: self.logs[prev_log_index as usize..last_entry_index]
                                    .to_vec(),
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
            _ => Output {
                req: None,
                rsp: None,
                commit: None,
            },
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
struct Message<A> {
    to: u64,
    from: u64,
    msg: A,
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
    term: u64,
}

#[derive(Debug)]
enum Input<'a> {
    Req(Req<'a>),
    Rsp(Rsp),
    Tick,
    Proposql(&'a [u8]),
}

#[derive(Debug, PartialEq, Eq)]
enum Req<'a> {
    AppendEntries(AppendEntriesReq<'a>),
    RequestVote,
}

#[derive(Debug)]
enum Rsp {
    AppendEntries(AppendEntriesRsp),
    RequestVote,
}

struct Output<'a> {
    req: Option<Vec<Message<Req<'a>>>>,
    rsp: Option<Vec<Message<Rsp>>>,
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
    use std::collections::HashMap;

    use crate::ticker::{AppendEntriesReq, LogEntry, Message, Req};

    use super::{Core, Input};

    #[test]
    fn run() {
        let mut core = Core::new(1, 1, vec![2, 3], vec![], vec![]);

        (1..10).for_each(|_| {
            core.run(Input::Tick);
        });
    }

    #[test]
    fn proposal() {
        let mut core = Core::new(1, 1, vec![2, 3], vec![], vec![]);
        // core.peers = vec![2, 3];
        core.id = 1;

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

    fn foo() {
        let mut m = Core::new(1, 1, vec![1u64, 2, 3], vec![], vec![]);
        let mut next_idx = HashMap::new();
        next_idx.insert(1, 1);
        next_idx.insert(2, 1);
        next_idx.insert(3, 1);
        m.next_idx = Some(next_idx);
        let mut match_idx = HashMap::new();
        match_idx.insert(1, 0);
        match_idx.insert(2, 0);
        match_idx.insert(3, 0);
        m.match_idx = Some(match_idx);
    }
}

impl ID for u64 {}
