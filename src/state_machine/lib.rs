#![allow(refining_impl_trait)]
#![allow(dead_code)]

trait State {
    fn handle(self: Box<Self>, e: Event) -> Output;
}

// enum RaftState {
//     Leader(Leader),
//     Follower(Follower),
//     // Candidate(Candidate),
// }

struct Output {
    next: Box<dyn State>,
    // err: Option<Box<dyn std::error::Error>>,
    reply: Option<Event>,
}

struct Leader;
impl State for Leader {
    fn handle(self: Box<Self>, _: Event) -> Output {
        Output {
            next: Box::new(Follower),
            reply: None,
        }
    }
}

struct Follower;
impl State for Follower {
    fn handle(self: Box<Self>, _: Event) -> Output {
        Output {
            next: Box::new(Leader),
            reply: None,
        }
    }
}

struct Candidate;
impl State for Candidate {
    fn handle(self: Box<Self>, e: Event) -> Output {
        match e {
            Event::AppendEntries => {}
            Event::Tick => {}
        }
        Output {
            reply: None,
            next: Box::new(Leader),
        }
    }
}

enum Event {
    Tick,
    AppendEntries,
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn transition() {
        let mut s: Box<dyn State> = Box::new(Follower);
        s = s.handle(Event::Tick).next;
    }
}

fn foo() {
    sub_mod::bar();
}

mod sub_mod {
    pub fn bar() {
        sub_sub_mod::generate();
    }

    mod sub_sub_mod {
        pub fn generate() {
            println!("hello");
        }

        fn parent() {
            super::super::foo();
        }
    }
}
