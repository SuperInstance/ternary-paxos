//! Simplified Paxos consensus for GPU cluster decisions with ternary votes.
//!
//! Vote states: +1 (accepted), 0 (pending), -1 (rejected).
//! Roles: Proposer, Acceptor, Learner.
//! Two-phase commit: prepare → accept.


/// Ternary vote state for a cluster decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Vote {
    /// Accepted — the node agrees.
    Accepted = 1,
    /// Pending — the node has not yet voted.
    Pending = 0,
    /// Rejected — the node disagrees.
    Rejected = -1,
}

impl Vote {
    pub fn from_i8(v: i8) -> Option<Self> {
        match v {
            1 => Some(Vote::Accepted),
            0 => Some(Vote::Pending),
            -1 => Some(Vote::Rejected),
            _ => None,
        }
    }

    pub fn as_i8(self) -> i8 {
        self as i8
    }
}

/// Monotonically increasing ballot number. Higher wins conflicts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Ballot(pub u64);

impl Ballot {
    pub fn zero() -> Self {
        Ballot(0)
    }

    pub fn next(self) -> Self {
        Ballot(self.0 + 1)
    }
}

/// A value proposed for consensus (e.g. "use GPU-7 for inference").
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Proposal {
    pub ballot: Ballot,
    pub value: String,
}

/// Promise returned by an acceptor during the prepare phase.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Promise {
    pub ballot: Ballot,
    /// If the acceptor previously accepted a proposal, it reports it back.
    pub previously_accepted: Option<Proposal>,
}

/// Response from an acceptor during the accept phase.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Accepted {
    pub ballot: Ballot,
    pub value: String,
}

// ---------------------------------------------------------------------------
// Proposer
// ---------------------------------------------------------------------------

/// Drives a Paxos round: issues a prepare, collects promises, then sends accept.
#[derive(Debug, Clone)]
pub struct Proposer {
    pub id: u64,
    pub ballot: Ballot,
    pub value: String,
    /// Highest proposal seen from promises (used to pick safe value).
    highest_promised: Option<Proposal>,
}

impl Proposer {
    pub fn new(id: u64, value: impl Into<String>) -> Self {
        Proposer {
            id,
            ballot: Ballot(id), // Use proposer id as base so different proposers get different ballots.
            value: value.into(),
            highest_promised: None,
        }
    }

    /// Increment to a new ballot (must be monotonically higher).
    pub fn prepare(&mut self) -> Ballot {
        self.ballot = self.ballot.next();
        self.highest_promised = None;
        self.ballot
    }

    /// Record a promise from an acceptor. Returns false if the promise is for
    /// a different (stale) ballot.
    pub fn receive_promise(&mut self, promise: &Promise) -> bool {
        if promise.ballot != self.ballot {
            return false;
        }
        if let Some(ref prev) = promise.previously_accepted {
            match &self.highest_promised {
                None => self.highest_promised = Some(prev.clone()),
                Some(current) if prev.ballot > current.ballot => {
                    self.highest_promised = Some(prev.clone());
                }
                _ => {}
            }
        }
        true
    }

    /// Build the accept request. Uses the highest previously-accepted value
    /// if one exists (Paxos safety), otherwise uses our own value.
    pub fn accept_request(&self) -> Proposal {
        match &self.highest_promised {
            Some(p) => Proposal {
                ballot: self.ballot,
                value: p.value.clone(),
            },
            None => Proposal {
                ballot: self.ballot,
                value: self.value.clone(),
            },
        }
    }
}

// ---------------------------------------------------------------------------
// Acceptor
// ---------------------------------------------------------------------------

/// Responds to prepare and accept requests, enforcing ballot monotonicity.
#[derive(Debug, Clone)]
pub struct Acceptor {
    /// Highest ballot this acceptor has promised to honour.
    promised_ballot: Ballot,
    /// The proposal (if any) this acceptor has accepted.
    accepted: Option<Proposal>,
}

impl Acceptor {
    pub fn new() -> Self {
        Acceptor {
            promised_ballot: Ballot::zero(),
            accepted: None,
        }
    }

    /// Handle a prepare request. Returns a promise only if the ballot is
    /// strictly higher than any previously promised ballot.
    pub fn prepare(&mut self, ballot: Ballot) -> Option<Promise> {
        if ballot > self.promised_ballot {
            self.promised_ballot = ballot;
            Some(Promise {
                ballot,
                previously_accepted: self.accepted.clone(),
            })
        } else {
            None // NACK — lower or equal ballot
        }
    }

    /// Handle an accept request. Accepts only if the ballot is >= the
    /// promised ballot.
    pub fn accept(&mut self, proposal: &Proposal) -> Option<Accepted> {
        if proposal.ballot >= self.promised_ballot {
            self.promised_ballot = proposal.ballot;
            self.accepted = Some(proposal.clone());
            Some(Accepted {
                ballot: proposal.ballot,
                value: proposal.value.clone(),
            })
        } else {
            None
        }
    }

    pub fn promised_ballot(&self) -> Ballot {
        self.promised_ballot
    }

    pub fn accepted_proposal(&self) -> Option<&Proposal> {
        self.accepted.as_ref()
    }
}

impl Default for Acceptor {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Learner
// ---------------------------------------------------------------------------

/// Observes accepted messages and tracks committed values once a quorum is
/// reached. Ternary quorum: a majority of acceptors must vote +1 (Accepted)
/// for the value to be committed.
#[derive(Debug, Clone)]
pub struct Learner {
    /// value → set of acceptor ids that accepted it
    accepted_votes: std::collections::HashMap<String, Vec<Vote>>,
    total_acceptors: usize,
    /// The value (if any) that achieved quorum.
    committed: Option<String>,
}

impl Learner {
    pub fn new(total_acceptors: usize) -> Self {
        Learner {
            accepted_votes: std::collections::HashMap::new(),
            total_acceptors,
            committed: None,
        }
    }

    /// Record an accepted vote for a value.
    pub fn receive(&mut self, acceptor_id: usize, value: &str, vote: Vote) {
        if self.committed.is_some() {
            return; // Already committed, ignore.
        }
        let votes = self.accepted_votes.entry(value.to_string()).or_default();
        // Ensure we have enough slots; push or overwrite.
        if acceptor_id >= votes.len() {
            votes.resize(acceptor_id + 1, Vote::Pending);
        }
        votes[acceptor_id] = vote;

        // Check ternary quorum: majority of acceptors must be Accepted.
        let accepted_count = votes.iter().filter(|&&v| v == Vote::Accepted).count();
        let majority = self.total_acceptors / 2 + 1;
        if accepted_count >= majority {
            self.committed = Some(value.to_string());
        }
    }

    /// Returns the committed value, if quorum was reached.
    pub fn committed(&self) -> Option<&str> {
        self.committed.as_deref()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vote_conversions() {
        assert_eq!(Vote::from_i8(1), Some(Vote::Accepted));
        assert_eq!(Vote::from_i8(0), Some(Vote::Pending));
        assert_eq!(Vote::from_i8(-1), Some(Vote::Rejected));
        assert_eq!(Vote::from_i8(2), None);
        assert_eq!(Vote::Accepted.as_i8(), 1);
        assert_eq!(Vote::Pending.as_i8(), 0);
        assert_eq!(Vote::Rejected.as_i8(), -1);
    }

    #[test]
    fn test_ballot_monotonic_increase() {
        let b = Ballot::zero();
        assert!(b.next() > b);
        assert!(b.next().next() > b.next());
        assert_eq!(Ballot(5).next(), Ballot(6));
    }

    #[test]
    fn test_proposer_prepare_increments_ballot() {
        let mut p = Proposer::new(1, "gpu-7");
        let b1 = p.prepare();
        let b2 = p.prepare();
        assert!(b2 > b1);
        assert_eq!(b2, Ballot(3)); // id=1 → first next=2, second next=3
    }

    #[test]
    fn test_acceptor_prepare_rejects_lower_ballot() {
        let mut a = Acceptor::new();
        let p1 = a.prepare(Ballot(5));
        assert!(p1.is_some());
        // Same ballot should be rejected (not strictly greater).
        let p2 = a.prepare(Ballot(5));
        assert!(p2.is_none());
        // Lower ballot rejected.
        let p3 = a.prepare(Ballot(3));
        assert!(p3.is_none());
        // Higher ballot accepted.
        let p4 = a.prepare(Ballot(10));
        assert!(p4.is_some());
        assert_eq!(p4.unwrap().ballot, Ballot(10));
    }

    #[test]
    fn test_acceptor_accept_only_if_ballot_geq_promised() {
        let mut a = Acceptor::new();
        a.prepare(Ballot(5));
        // Lower ballot reject.
        let r1 = a.accept(&Proposal { ballot: Ballot(4), value: "x".into() });
        assert!(r1.is_none());
        // Equal/higher ballot accept.
        let r2 = a.accept(&Proposal { ballot: Ballot(5), value: "gpu-3".into() });
        assert!(r2.is_some());
        assert_eq!(r2.unwrap().value, "gpu-3");
    }

    #[test]
    fn test_learner_quorum_commit() {
        let mut learner = Learner::new(3); // 3 acceptors → need 2 Accepted.
        learner.receive(0, "gpu-7", Vote::Accepted);
        assert!(learner.committed().is_none());
        learner.receive(1, "gpu-7", Vote::Accepted);
        assert_eq!(learner.committed(), Some("gpu-7"));
    }

    #[test]
    fn test_learner_rejected_votes_do_not_commit() {
        let mut learner = Learner::new(3);
        learner.receive(0, "gpu-5", Vote::Rejected);
        learner.receive(1, "gpu-5", Vote::Rejected);
        assert!(learner.committed().is_none());
    }

    #[test]
    fn test_conflict_resolution_higher_ballot_wins() {
        // Two proposers compete; higher ballot wins.
        let mut a = Acceptor::new();

        // Proposer 1 with ballot 3
        let prom1 = a.prepare(Ballot(3));
        assert!(prom1.is_some());
        let acc1 = a.accept(&Proposal { ballot: Ballot(3), value: "val-A".into() });
        assert!(acc1.is_some());

        // Proposer 2 with higher ballot 7
        let prom2 = a.prepare(Ballot(7));
        assert!(prom2.is_some());
        // The promise should report the previously accepted proposal.
        assert_eq!(prom2.unwrap().previously_accepted.unwrap().value, "val-A");

        // Proposer 2's accept overwrites.
        let acc2 = a.accept(&Proposal { ballot: Ballot(7), value: "val-B".into() });
        assert!(acc2.is_some());
        assert_eq!(a.accepted_proposal().unwrap().value, "val-B");
    }

    #[test]
    fn test_full_paxos_round_trip() {
        // 3 acceptors, 1 proposer, 1 learner — full happy path.
        let mut acceptors = vec![Acceptor::new(); 3];
        let mut proposer = Proposer::new(1, "schedule-gpu-cluster");
        let mut learner = Learner::new(3);

        // Phase 1: prepare
        let ballot = proposer.prepare();
        for a in &mut acceptors {
            let promise = a.prepare(ballot).expect("should promise");
            proposer.receive_promise(&promise);
        }

        // Phase 2: accept
        let proposal = proposer.accept_request();
        for (i, a) in acceptors.iter_mut().enumerate() {
            if let Some(accepted) = a.accept(&proposal) {
                learner.receive(i, &accepted.value, Vote::Accepted);
            }
        }

        assert_eq!(learner.committed(), Some("schedule-gpu-cluster"));
    }

    #[test]
    fn test_proposer_uses_highest_promised_value() {
        // Proposer should adopt the highest previously-accepted value for safety.
        let mut proposer = Proposer::new(10, "my-val");

        let ballot = proposer.prepare();

        // Simulate a promise reporting a previously accepted value with ballot 3.
        let p1 = Promise {
            ballot,
            previously_accepted: Some(Proposal { ballot: Ballot(3), value: "existing-val".into() }),
        };
        proposer.receive_promise(&p1);

        // Another promise with a higher previously-accepted ballot (5).
        let p2 = Promise {
            ballot,
            previously_accepted: Some(Proposal { ballot: Ballot(5), value: "newer-val".into() }),
        };
        proposer.receive_promise(&p2);

        // The accept request should use "newer-val" (highest ballot).
        let req = proposer.accept_request();
        assert_eq!(req.value, "newer-val");
    }
}
