# Transaction body arrival grace

This fork starts from Bor 2.10.3, commit `a16f123d9b4f41a552a3a98d93a0340430838576`.

The `p2p.txarrivalwait` setting controls the transaction fetcher's initial body wait.
Set a positive duration in the existing config section:

```toml
[p2p]
    txarrivalwait = "100ms"
```

The command-line override is `-txarrivalwait=100ms`.
The default remains 500 ms. Non-positive constructor values retain that default.
Arrival gathering slack is one fifth of the wait, capped at 100 ms.
A 100 ms wait therefore uses 20 ms arrival gathering slack.
The five-second request timeout and its 100 ms slack remain unchanged.
The startup log records both effective arrival values.

Earlier requests can retrieve a body before an unsolicited broadcast arrives.
A full broadcast or another provider can already deliver the body first.
Changing this wait does not improve those earlier deliveries.
Hash announcements do not contain destination or calldata, so this setting applies to all eligible announcements.
Request counts and duplicate bodies can increase.

Pool validation, signature checks, peer penalties, announcement caps, request count and byte limits,
request correlation and slow-peer handling keep their existing paths.
The patch does not change consensus, state storage, pruning, RPC exposure or trading safeguards.

The Macabim workflow tests the transaction path and builds the exact PR head commit.
Its artifact includes the Linux binary, checksum and source commit.
Verify all three before installation. Preserve the existing node config, data directory and stock binary.
Use the same RPC and canonical-block checks before and after a trial restart.

Controlled local DEX tests show about 400 ms earlier readiness for hash-only delivery with a 20 ms reply.
A full body at 250 ms reduces that lead to about 128 ms.
These are synthetic delivery schedules with real EVM execution, not live Polygon speed or profit measurements.
The useful live target fraction and producer delivery benefit remain unproved.

The artifact also contains `bor-delivery.conf` for the existing `bor.service`.
It sets the explicit 100 ms command-line override and allows graceful shutdown to finish without a forced kill.
It preserves the base unit's environment, user, restart policy and data-directory config.
Install it as `/etc/systemd/system/bor.service.d/30-macabim-delivery.conf` during the trial.
Removing that file restores the original startup command and stop timeout.

## Bounded live comparison

Set `BOR_DELIVERY_EXPERIMENT` to a JSON object with UTC `start`, UTC `end`, and an absolute trace `path`.
Limit the interval to 30 minutes. Start within 30 minutes of node startup. Use a new output file.

Set `waitsMs` to `[10,100]` for the fast trial or `[100,500]` for the original trial.
Omit `waitsMs` to retain the original pair. Other pairs disable the experiment.
The node assigns unknown announced hashes to the selected pair with a private random seed.
Assignment uses the hash before the body is available. The matching gathering slack is one fifth of the assigned wait, capped at 100 ms.
The 10 ms arm uses 2 ms slack. The 100 ms arm uses 20 ms slack.
Mixed timer events can gather control hashes after 400 ms. Report observed request times for each arm.
Full broadcasts, validation, peer caps, request correlation, rate limits, and timeouts retain their existing paths.

The trace records announcement, enrollment, request, body, and pool result timestamps. It excludes transaction contents and peer identities.
A nonblocking queue holds 8,192 events. The file limit is 256 MiB. Queue overflow or write failure disables new enrollment.
Treat any incomplete trace as an invalid trial. Existing assigned hashes finish with their assigned wait.
The writer exits 15 seconds after expiry. Later hashes use the configured node wait.

Join hashes with Tamnun publication and Trader stage records on the same host.
Report alternate-provider wins and missing observations. Separate body-first traffic from the randomized hash-first cohort.
Compare readiness rates at fixed deadlines across all enrolled hashes. Conditional useful-target times are descriptive.
Do not infer a same-transaction improvement from different transactions or claim profit from latency alone.


## Maintained fork deployment

Build production artifacts from `macabim/bor` branch `main`.
Verify the binary commit against that branch before installation.
Use the packaged service drop-in for the permanent 10 ms arrival wait.
Keep the config file, data directory, validation, and peer limits unchanged.
Record the source commit, artifact checksum, and rollback binary with each deployment.
