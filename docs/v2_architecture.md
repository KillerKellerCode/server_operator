# Operator V2 Architecture

This document defines the target architecture for V2 migration work. It is intentionally specific so code changes can be evaluated against clear constraints.

## 1) Current V1 Limitations

V1 is reliable for simple runs but has structural limits:
- flat task list only; no group-level coordination
- strict sequential execution only
- generic shell loop for many operations that should be handled by typed Python logic
- timeout behavior exists but is mostly executor-local, not clearly planner-visible
- logs are human-readable, but structured events are not first-class for live progress consumers
- Codex is used for coding tasks, but setup-heavy coding workflows are split awkwardly across shell/codex boundaries

## 2) V2 Target Architecture

V2 replaces the flat list with a dependency-aware execution model while keeping the current CLI entrypoints stable.

Core V2 layers:
- planner layer: produces a `JobPlan` with ordered task groups and dependency edges
- scheduler layer: evaluates readiness and dispatches independently runnable work
- executor layer: runs concrete task types and returns typed result/events
- persistence layer: writes durable run state, task artifacts, and append-only structured events
- human logs layer: continues writing readable `run.log` and `task.log`

Design principle:
- scheduling decisions and execution mechanics remain separate modules

## 3) Task Groups + Dependency Graph Execution

Target plan shape:
- job contains ordered groups (`group_index` for deterministic presentation)
- each group contains tasks
- dependencies are explicit:
  - group-level dependencies (`group B depends on group A`)
  - task-level dependencies where needed (`task X depends on task Y`)

Execution semantics:
- scheduler repeatedly computes ready groups/tasks from dependency satisfaction
- independent ready units may run concurrently
- within a group, ordering is explicit per task dependencies (not implied by list position alone)
- failure handling records blocked dependents and stops or degrades according to policy

Determinism requirements:
- persisted graph state must allow replay/inspection of readiness decisions
- state transitions are explicit and append-only in event history

## 4) Structured Event Logging

V2 introduces structured append-only events suitable for future UI streaming and daemon status APIs.

Event requirements:
- append-only JSON events persisted in run storage
- stable event envelope fields:
  - `timestamp`
  - `job_id`
  - `group_id` (optional)
  - `task_id` (optional)
  - `event_type`
  - `payload` (JSON-serializable object)
- events are emitted for planning, scheduling, execution lifecycle, command output, retries/adaptation, and terminal outcomes

Human-readable logs remain required:
- `run.log` and `task.log` continue to be written for operators
- structured events are additive, not a replacement

## 5) Typed Executors

V2 expands executor types to reduce shell-fragility and improve observability:
- `shell` executor: controlled command execution with explicit timeout and captured stdout/stderr
- `codex` executor: delegated coding worker, including coding-project setup when setup is coding-context-dependent
- native typed executors (planned):
  - filesystem operations
  - download/extract operations
  - install/package operations

Typed executors should:
- expose explicit typed input/output schemas
- emit lifecycle and result events consistently
- avoid hidden side effects

## 6) Timeout-Aware Shell Execution

Timeout becomes planner-visible and task-configurable in V2.

Rules:
- each shell task can specify timeout policy explicitly (with defaults from config)
- timeout value is persisted with task definition/state
- timeout expiration produces explicit failure/interruption events
- scheduler receives terminal reason so dependent scheduling is deterministic

This avoids implicit executor behavior and lets plans reflect expected command duration risk.

## 7) Codex-Owned Coding Project Setup

V2 policy shift:
- if setup is part of preparing a coding workspace (project scaffolding, test harness wiring, codebase-local conventions), prefer codex tasks
- shell tasks remain for generic machine operations (installing packages, moving files, launching tools)

Why:
- coding setup often depends on repository context that codex can reason about better than generic shell loops
- keeps task intent clearer: machine ops vs coding-context work

## 8) V3 Readiness (Web UI + Daemon Mode)

V2 explicitly prepares for V3 without implementing it yet.

V3 prerequisites delivered by V2:
- structured append-only events that can be tailed/streamed
- clear scheduler state model for live progress views
- deterministic task/group status transitions
- executor boundaries that support local CLI now and service orchestration later

Not in scope for V2:
- no web UI implementation
- no background daemon/service runtime
- no distributed worker system
- no database requirement for initial V2 migration
