# Agent Instructions: Kosmos Downloader

**Kosmos Downloader** is an Internet Download Manager (IDM) desktop application built with Rust 2024, Slint, Tokio, and Reqwest.

## Core Directives for Agents

1. **Strict Modularity**:
   - Every component lives in its own dedicated module with a clean, minimal public API.
   - `client`: Network operations, HTTP HEAD requests, `Range` header support, Content-Length extraction.
   - `engine`: Download session coordination, chunk partitioning, worker spawning, resume tracking.
   - `storage`: Target file pre-allocation and direct concurrent offset writes.
   - `ui`: Slint presentation layer and event/state bindings. No download logic in UI code.

2. **Simplicity & Maintainability (KISS / YAGNI)**:
   - Write short, concise, readable code. No complex or speculative abstractions.
   - Use concrete structs and functions; avoid traits unless mocking or multiple implementations are strictly required.
   - Reuse existing code and dependencies before adding helpers or crates. Prefer the standard library when it suffices.

3. **Rust Best Practices & Coding Style**:
   - **Formatting & Lints**: Use `cargo fmt` and fix Clippy warnings rather than suppressing them. Keep any necessary lint allowance narrow and explain it.
   - **Error Handling**: Use typed errors (`thiserror`) and propagate with `?`, preserving underlying causes. Never use `.unwrap()` or `.expect()` in production code paths. Do not silently discard errors; explicitly handle intentional best-effort operations.
   - **Ownership & APIs**: Prefer borrowed inputs (`&str`, `&Path`, slices) unless ownership is needed. Avoid unnecessary clones and allocations; keep visibility as narrow as callers permit.
   - **Async & Concurrency**: Keep blocking I/O and CPU-heavy work off Tokio workers and the UI thread; use async APIs or `spawn_blocking`. Prefer message-passing for coordination, keep lock scopes short, and never hold a blocking lock guard across `.await`.
   - **Byte Arithmetic**: Use `u64` for file sizes and offsets. Check arithmetic and fallible conversions where remote metadata or input could cause overflow or truncation.
   - **Safety**: Prefer safe Rust. Any necessary `unsafe` block must document its safety invariants.
   - **Verification**: Add focused regression tests for behavior changes, including relevant boundary and failure cases. Keep network tests local and deterministic, and isolate temporary files between tests.

## Cross-Platform & Resource Efficiency
- Design for Windows, macOS, and Linux. Isolate OS-specific behavior and avoid assumptions about installed fonts, filesystem paths, or platform APIs.
- Very low RAM usage and CPU overhead are core product requirements, both during downloads and at idle. Keep idle work negligible.
- Prefer event-driven updates, bounded buffers and concurrency, and direct streaming to disk. Avoid unnecessary polling, redraws, allocations, copies, and unbounded caches.
- Measure performance-sensitive changes in release builds rather than assuming they are efficient; include graphics memory and GPU activity when evaluating renderer choices. Never sacrifice download integrity, error handling, or accessibility for lower resource usage.

## Download Safety
- Validate range response status, `Content-Range`, and expected byte counts; never write a full-body response as a requested chunk.
- Before resuming, validate saved ranges and remote resource identity; never knowingly combine bytes from different resource versions.
- Without a strong ETag, sampling-based checks are an explicit exception for ranged resume and parallel downloads: validate remote metadata, sample the first and last saved bytes of every chunk before resume and completion, and verify a bounded overlap before appending to a partial chunk. Sampling can miss changes outside the checked regions and cannot guarantee whole-file identity.
- A detected content mismatch must stop all old writers, discard the old partial data, and restart from byte zero. Limit automatic content-change recovery to two restarts; retain recoverable partial data for temporary network failures instead.
- Write chunks directly at validated, non-overlapping offsets, pre-allocating when the total size is known. Avoid merge passes.
- Never silently overwrite unrelated files. Preserve recoverable partial data on failure or cancellation, and report completion only after all writes and required flushes succeed.

## UI Style Preferences
- Use square corners throughout the app, including popups, fields, buttons, menus, and slider handles. Do not use rounded corners.
- Avoid permanent outlines around every control or panel. Use spacing, alignment, and subtle surface-color changes to establish hierarchy; reserve borders for focus, errors, or necessary separators.
- Keep layouts compact and consistent with the app's dark theme. Use a shared visual language for typography, spacing, controls, and action placement across screens and dialogs.
- Group related inputs, use clear labels, and distinguish primary actions from secondary or destructive actions. Prefer sensible, platform-aware defaults over hard-coded user-specific values.
- Preserve visible keyboard focus, readable contrast, and native input/accessibility behavior when customizing controls.

## Agent Workflow
- Read the affected code and trace callers before editing. Fix bugs at their shared cause rather than patching individual symptoms.
- Keep diffs focused and minimal; preserve unrelated worktree changes. Do not introduce speculative abstractions or refactor unrelated code.
- For Rust changes, run `cargo fmt --check`, `cargo test`, and `cargo clippy --all-targets -- -D warnings`. Format with `cargo fmt` when needed.
- Report what changed, which checks ran, and any failures or checks that could not run. Documentation-only changes do not require Cargo checks.
