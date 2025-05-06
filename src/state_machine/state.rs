use std::collections::{HashMap, HashSet};
use std::hash::Hash;

pub trait ID: PartialEq + Eq + Hash + Copy + std::fmt::Debug + std::fmt::Display {}
pub trait State {}

pub struct Leader<A>
where
    A: ID,
{
    match_idx: HashMap<A, u64>,
    next_idx: HashMap<A, u64>,
}

impl<A> Leader<A>
where
    A: ID,
{
    pub fn new(peers: HashSet<A>) -> Self {
        Self {
            match_idx: peers.clone().into_iter().map(|x| (x, 1)).collect(),
            next_idx: peers.into_iter().map(|x| (x, 1)).collect(),
        }
    }
}
impl<A> State for Leader<A> where A: ID {}

pub struct Follower;
impl State for Follower {}

pub struct Candidate;
impl State for Candidate {}
