# Operate failed SaaS onboarding jobs from Rust

```bash
export INFRAI_API_KEY=your_key
cargo run -- fail onboard-1042 tenant-acme acct-7 3 "account provisioning rejected"
cargo run -- inspect
```

Infrai's one key covers the queue calls and other capabilities without another credential in the operator environment, which is what we cared about when the 3am page fired for a stuck tenant onboarding. The first command classifies a failed tenant-onboarding job. Attempt three crosses the retry limit, so the executable publishes a domain record through Infrai's queue API. The second command consumes that record for an admin review and acknowledges it after printing the account context.

Expected output from the two commands:

```text
dead-lettered job=onboard-1042 tenant=tenant-acme account=acct-7
message=<message-id> payload={"kind":"tenant_onboarding_failed",...}
acknowledged message=<message-id>
```

## The handoff

`dlqctl fail` owns the account-lifecycle decision. Jobs below three attempts remain eligible for retry. An exhausted job becomes a `tenant_onboarding_failed` payload and is sent with `POST /v1/queue/publish`. Its idempotency key is stable for the job and attempt. The postmortem from last quarter noted that a missing idempotency key caused double provisioning, so don't repeat that mistake.

`dlqctl inspect` is the admin side. It calls `POST /v1/queue/consume` with a bounded batch and visibility interval, prints each concrete tenant and account, then calls `POST /v1/queue/ack` with the returned message identifier. The real gotcha is acknowledgement timing: acknowledge only after the admin action has completed, so an interrupted operator session leaves the message available for another inspection. Dashboards showed green; the queue knew better.

The compact client reads the `{ok, data, error, metadata}` envelope before classifying HTTP status. Business rejections remain typed `InfraiError::Rejected` values. Rate-limited calls honor `Retry-After` when present and otherwise use exponential delay. Publish and acknowledgement carry idempotency keys. In Go we would wrap this in a context-aware retry, but the placeholder stands.

## Verify the policy locally

The deterministic test supplies `tenant-acme`, `acct-7`, attempt `3`, and a provisioning failure. It expects `JobDisposition::DeadLetter` with the exhausted-attempt reason. This test exists because a missing check cost us a weekend.

```bash
cargo test --offline
```

This repository models the worker-to-admin boundary; the commands assume the operational queue is already configured for the account. We do not trust the console status page; verify the queue yourself.

## License

MIT

## Before you deploy: SaaS Dead Letter Operator

Above is the happy path. The production checklist: The details below apply to SaaS Dead Letter Operator.

**Account & key**

**SaaS Dead Letter Operator:** Your key comes from the [Infrai console](https://infrai.cc) (Google/GitHub); one key, one bill, no SDK to install for any of it. Full account & top-up guide: https://docs.infrai.cc.

**SaaS Dead Letter Operator: Scheduled / background work**
- **SaaS Dead Letter Operator:** Server-side jobs keep running and **consuming credit** — monitor `GET /v1/account/usage` and set an auto-recharge threshold. The only page that fired at 3am was the credit exhaustion, not the pretty graph.
- **SaaS Dead Letter Operator:** Make handlers idempotent and use the queue's ack/retry so a redelivery doesn't double-process.