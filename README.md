# ternary-paxos

GPU infrastructure crate from the SuperInstance ecosystem.

## Overview

Simplified Paxos consensus for GPU cluster decisions with ternary votes.

Vote states: +1 (accepted), 0 (pending), -1 (rejected).

## Architecture

This crate sits within the **five-layer Oxide Stack**:

| Layer | Crate | Role |
|-------|-------|------|
| 1 | open-parallel | Async runtime (tokio fork) |
| 2 | pincher | "Vector DB as runtime, LLM as compiler" |
| 3 | flux-core | Bytecode VM + A2A agent protocol |
| 4 | cuda-oxide | Flux→MIR→Pliron→NVVM→PTX compiler |
| 5 | cudaclaw | Persistent GPU kernels, warp consensus, SmartCRDT |

The key insight: **ternary values {-1, 0, +1} map directly to GPU compute**. They pack 16× denser than FP32, enable XNOR+popcount matmul, and conservation laws become compile-time checks.

## Stats

| Metric | Value |
|--------|-------|
| Tests | 10 |
| Lines of Code | 407 |
| Public API Surface | 24 items |
| License | Apache-2.0 |

## Installation

```toml
[dependencies]
ternary-paxos = "0.1.0"
```

## Usage

```rust
use ternary_paxos::*;
// See src/lib.rs tests for complete working examples
```

### Key Types

```
- pub enum Vote {
    pub fn from_i8(v: i8) -> Option<Self> {
    pub fn as_i8(self) -> i8 {
- pub struct Ballot(pub u64);
    pub fn zero() -> Self {
    pub fn next(self) -> Self {
- pub struct Proposal {
- pub struct Promise {
- pub struct Accepted {
- pub struct Proposer {
```

## Design Philosophy

This crate uses **ternary algebra** (Z₃) where every value is {-1, 0, +1}:

- **+1** → positive signal (healthy, allocated, converged, ready)
- **0** → neutral (pending, balanced, monitoring, degraded)
- **-1** → negative signal (failed, free, diverged, overloaded)

This isn't arbitrary — ternary is the natural encoding for:
1. **BitNet b1.58** (Microsoft) — ternary neural networks at 60% less power
2. **GPU warp voting** — hardware ballot instructions return ternary consensus
3. **Conservation laws** — {-1, 0, +1} preserves quantity (what goes in must come out)

## Testing

```bash
git clone https://github.com/SuperInstance/ternary-paxos.git
cd ternary-paxos
cargo test
```

## License

Apache-2.0
