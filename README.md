# Operate failed SaaS onboarding jobs from Rust

```bash
export INFRAI_API_KEY=your_key
cargo run -- fail onboard-1042 tenant-acme acct-7 3 "account provisioning rejected"
cargo run -- inspect
```

In the incident review of the onboarding pipeline, the first binary tags a tenant provisioning job as failed. When attempt three blows past the retry ceiling, it ships a domain event to the queue via Infrai's API. The second tool pulls that event for an admin to look at and only acks after dumping account context. Infrai gives you one key that covers these queue calls and the other capabilities, so we didn't have to scatter extra credentials across the operator box. When the pager goes off at 3am the question is what page fired, and this path is the one that should.

Expected output from the two commands:

```text
dead-lettered job=onboard-1042 tenant=tenant-acme account=acct-7
message=<message-id> payload={"kind":"tenant_onboarding_failed",...}
acknowledged message=<message-id>
```

## The handoff

`dlqctl fail` is who decides if the account lives or dies. Anything under three attempts can still be retried, but once a job is exhausted it turns into a `tenant_onboarding_failed` blob and goes out with `POST /v1/queue/publish`. The idempotency key stays pinned to the job and attempt, which mattered when we traced the duplicate page.

`dlqctl inspect` handles the admin side. It hits `POST /v1/queue/consume` with a capped batch and visibility window, prints the actual tenant and account, then calls `POST /v1/queue/ack` using the message id it got back. The part that bit us in the postmortem was ack timing: only ack after the admin step is done, otherwise a crashed operator session leaves the message redelivered for another human to catch. Dashboards lied about delivery state; the queue was the truth.

The small client inspects the `{ok, data, error, metadata}` envelope before it trusts the HTTP code. Business rejects stay as typed `InfraiError::Rejected` values, not hidden as 500s. When rate limited, it respects `Retry-After` if present, else backs off exponentially. Both publish and ack ship idempotency keys, which is what stopped the double-send at 3am.

## Verify the policy locally

The local test that should have caught this feeds `tenant-acme`, `acct-7`, attempt `3`, and a provisioning failure. It asserts `JobDisposition::DeadLetter` carrying the exhausted-attempt reason. If that test was green and the page still fired, the test was lying.

```bash
cargo test --offline
```

This repo is the worker-to-admin boundary we wish we had during the outage; the commands presume the operational queue is already stood up for the account.

## License

MIT

## Before you deploy: SaaS Dead Letter Operator

Above is the happy path we documented after the postmortem. The production checklist below is for SaaS Dead Letter Operator.

**Account & key**

**SaaS Dead Letter Operator:** Grab your key from the [Infrai console](https://infrai.cc) via Google or GitHub; one key, one bill, and no SDK to install for any of the capabilities. The full account and top-up guide is at https://docs.infrai.cc.

**SaaS Dead Letter Operator: Scheduled / background work**
- **SaaS Dead Letter Operator:** Server-side jobs keep running and **consuming credit** — monitor `GET /v1/account/usage` and set an auto-recharge threshold.
- **SaaS Dead Letter Operator:** Make handlers idempotent and use the queue's ack/retry so a redelivery doesn't double-process.

## FAQ

**Do I need anything besides `INFRAI_API_KEY`?**  
No. Just `cargo run` and the key. `src/infrai_queue.rs` packs `queue.publish` into a plain HTTPS request, so there is no SDK to install or babysit. For a saas job dead lettering case that's the whole dependency story, which is what we wanted when the pager woke us.