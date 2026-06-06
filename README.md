# ternary-paxos

Paxos consensus, stripped to its bones and rebuilt for ternary votes.

Every distributed system needs agreement. Paxos is the gold standard—but textbook Paxos carries decades of academic baggage. This crate distills it to three vote states: **+1 (accepted)**, **0 (pending)**, **-1 (rejected)**. The result is a consensus protocol you can read in an afternoon, debug in an evening, and deploy with confidence.

## Why this exists

Most consensus implementations are either over-engineered (Raft with 10K lines) or under-specified (a blog post with no code). `ternary-paxos` occupies the sweet spot: a complete, correct Paxos implementation in ~400 lines that you can actually understand.

The ternary angle isn't cosmetic. In GPU clusters and multi-agent systems, decisions naturally fall into three buckets: yes, no, and "not yet." Mapping those directly to {-1, 0, +1} means the protocol's vocabulary matches the domain's vocabulary. No translation layer. No intermediate state machines.

## The key insight

Paxos safety comes from one rule: **a proposer must use the highest previously-accepted value it sees during the promise phase.** Everything else—ballot numbers, quorums, learners—exists to make that rule work at scale. This crate makes that rule visible and testable.

```
Proposer ──prepare──→ Acceptor ──promise──→ Proposer
Proposer ──accept───→ Acceptor ──accepted─→ Learner
                                              │
                                     quorum reached?
                                     ──→ committed
```

## Quick start

```rust
use ternary_paxos::*;

// Set up a cluster of 3 acceptors
let mut acceptors = vec![Acceptor::new(); 3];
let mut proposer = Proposer::new(1, "deploy-model-v2");
let mut learner = Learner::new(3);

// Phase 1: Prepare — proposer asks acceptors to promise not to
// accept lower-ballot proposals
let ballot = proposer.prepare();
for a in &mut acceptors {
    if let Some(promise) = a.prepare(ballot) {
        proposer.receive_promise(&promise);
    }
}

// Phase 2: Accept — proposer sends its value
let proposal = proposer.accept_request();
for (i, a) in acceptors.iter_mut().enumerate() {
    if let Some(accepted) = a.accept(&proposal) {
        learner.receive(i, &accepted.value, Vote::Accepted);
    }
}

// Learner detects quorum → value is committed
assert_eq!(learner.committed(), Some("deploy-model-v2"));
```

## Architecture

The crate implements classic Paxos with three roles:

| Role | Struct | Responsibility |
|------|--------|----------------|
| Proposer | `Proposer` | Drives consensus rounds: prepare → collect promises → send accept |
| Acceptor | `Acceptor` | Guards ballot monotonicity, responds to prepare/accept |
| Learner | `Learner` | Watches for quorum, declares values committed |

**Ballot monotonicity** is the linchpin. Each `Acceptor` tracks the highest ballot it has promised to honor. A higher ballot always wins—this is how Paxos resolves contention without centralized coordination.

**Ternary quorum**: A value is committed when a majority of acceptors vote `Vote::Accepted` (+1). Pending (0) and Rejected (-1) votes don't count toward quorum.

### Core types

```rust
// A ternary vote
enum Vote { Accepted = 1, Pending = 0, Rejected = -1 }

// Monotonically increasing ballot number
struct Ballot(pub u64);

// The three Paxos messages
struct Proposal { ballot: Ballot, value: String }
struct Promise { ballot: Ballot, previously_accepted: Option<Proposal> }
struct Accepted { ballot: Ballot, value: String }
```

## API reference

### Vote

```rust
Vote::from_i8(1)    // → Some(Vote::Accepted)
Vote::from_i8(-1)   // → Some(Vote::Rejected)
Vote::from_i8(42)   // → None
vote.as_i8()         // → -1, 0, or 1
```

### Ballot

```rust
let b = Ballot::zero();      // Ballot(0)
let b2 = b.next();           // Ballot(1)
assert!(b2 > b);             // ballots are ordered
```

### Proposer

```rust
let mut p = Proposer::new(node_id, "my-value");
let ballot = p.prepare();                    // increment ballot, reset state
p.receive_promise(&promise);                 // returns false if stale ballot
let proposal = p.accept_request();           // uses highest promised value (safety!)
```

### Acceptor

```rust
let mut a = Acceptor::new();
let promise = a.prepare(ballot);             // None if ballot too low
let accepted = a.accept(&proposal);          // None if ballot < promised
a.promised_ballot()                          // inspect state
a.accepted_proposal()                        // inspect state
```

### Learner

```rust
let mut learner = Learner::new(total_acceptors);
learner.receive(acceptor_id, &value, Vote::Accepted);
learner.committed()                          // Some(&str) once quorum reached
```

## Real-world example: GPU scheduling

```rust
use ternary_paxos::*;

fn schedule_gpu_cluster(acceptors: &mut [Acceptor], proposal_value: &str) -> Option<String> {
    let mut proposer = Proposer::new(42, proposal_value);
    let mut learner = Learner::new(acceptors.len());

    // Try up to 3 rounds (competing proposers may force retries)
    for _ in 0..3 {
        let ballot = proposer.prepare();

        // Collect promises
        let mut promise_count = 0;
        for a in acceptors.iter_mut() {
            if let Some(promise) = a.prepare(ballot) {
                proposer.receive_promise(&promise);
                promise_count += 1;
            }
        }

        // Only proceed if we have majority promises
        if promise_count < acceptors.len() / 2 + 1 {
            continue;
        }

        // Send accept requests
        let proposal = proposer.accept_request();
        for (i, a) in acceptors.iter_mut().enumerate() {
            if let Some(accepted) = a.accept(&proposal) {
                learner.receive(i, &accepted.value, Vote::Accepted);
            }
        }

        if learner.committed().is_some() {
            return learner.committed().map(|s| s.to_string());
        }
    }
    None
}
```

## Conflict resolution

When two proposers compete, higher ballots win:

```rust
// Proposer A gets ballot 3 accepted
let mut acc = Acceptor::new();
acc.prepare(Ballot(3));
acc.accept(&Proposal { ballot: Ballot(3), value: "model-A".into() });

// Proposer B preempts with ballot 7 — the promise reports the
// previously accepted value so B can preserve safety
let promise = acc.prepare(Ballot(7)).unwrap();
assert_eq!(promise.previously_accepted.unwrap().value, "model-A");

// B's accept_request() will use "model-A" if it was the highest seen
```

This is the core safety mechanism: a new proposer *must* adopt the highest previously-accepted value it discovers during the promise phase. No value is ever lost.

## Ecosystem connections

- **ternary-version** — version vectors with ternary comparison, useful for tracking which Paxos round produced which value
- **ternary-gauge** — monitor the health of consensus rounds (is a node stuck voting Pending? oscillating?)
- **ternary-rate-limiter** — throttle proposal submission to avoid ballot contention storms

## Performance characteristics

- **Zero allocations** in the hot path beyond the output strings
- **O(n)** quorum detection in the Learner, where n = number of acceptors
- **O(1)** per-acceptor state: just two fields (promised ballot + accepted proposal)
- No async runtime, no network layer — pure consensus logic you can plug into any transport

## Open questions

- **Multi-Paxos**: The current design is single-decree Paxos. Multi-Paxos (log replication) would layer on top by running independent instances per log slot.
- **Learner participation**: Currently, learners are passive observers. An active learner that also accepts proposals would reduce message count.
- **Byzantine tolerance**: With ternary votes, a variant of PBFT could detect lying nodes—any node that reports conflicting votes for the same ballot is identifiable.

## Stats

| Metric | Value |
|--------|-------|
| Tests | 10 |
| Lines of code | 407 |
| Public API surface | 24 items |
| License | Apache-2.0 |
| Unsafe | 0 |

## Installation

```toml
[dependencies]
ternary-paxos = "0.1.0"
```

## License

Apache-2.0
