# Plan: leaked quorum queue members

Related: #17137, #17214, #17457 (discussion #17459), rabbitmq/ra#668 (discussion #669)

## Problem

A quorum queue can end up with Ra members that no queue record knows about. A later declare of the
same name then fails, and keeps failing: `ra:start_cluster/3` finds the name in use and
`declare_queue_error/4` deletes the new record again.

Known causes:

 * A force delete cannot reach a member (#17137). The record is removed anyway
 * Two deletes race with a declare (#17457). The second delete removes the new incarnation's record,
   and its Ra cluster keeps running
 * A declare times out while members are still starting (ra#668). The record is removed, and a member
   that finishes starting afterwards stays. Ra `main` now cleans up after a timeout, but only on a best
   effort basis

#17214 retries failed force deletes from a node-local DETS table. That state is lost with the node that
ran the delete, it does not see the second and third causes, and it does not help with existing leaks.
The node that holds an orphan can find it by itself instead, by comparing its Ra members with the queue
records.

## Proposal

Changes 1 to 3 are small and can ship first. Change 4 removes the delete race and needs its own review.

### 1. Ra: report and delete by UID

On the `members-exist-uids` branch of Ra, on top of the unreleased `start_cluster` work:

 * `{error, {members_exist, [{ServerId, UId}]}}` reports the UID of each clashing member
 * `ra:force_delete_server/4` deletes a member only if it is registered with the given UID, and returns
   `{error, not_owner}` otherwise
 * The `start_cluster` pre-check also finds registered members that are stopped, not just running ones

All three are pushed. `rabbitmq-components.mk` points `dep_ra` at that branch until Ra is released.

### 2. Clean up in the declare and delete paths

When `internal_declare/2` returns `created`, no record exists for the name, so any Ra member with that
name is an orphan. On `members_exist`, the declare deletes the clashing members by UID and retries
`ra:start_cluster/3` once. If the declare fails for another reason, it deletes the members it tried to
start, by UID, before removing the record.

`add_member` does the same when `ra:start_server/2` fails because the name is already registered on the
target node. It reads the registered UID on that node and deletes the member by UID only if the record
does not list that node with that UID; otherwise it is the real member and the error is returned as
today. With legacy list-format records, it only deletes if the record does not list the node. Two
concurrent `add_member` calls for the same queue and node could delete each other's new member. The
reconciliation process only acts on queues whose leader is local, so this needs a second caller, such as
`rabbitmq-queues add_member`, or a leader change.

In `delete/4`, `force_delete_queue/1` uses `ra:force_delete_server/4` when the record has UIDs, so a
stale delete cannot remove a newer incarnation's members through that path.

Status: implemented. The UID-guarded force delete has its own 30 second timeout, where the by-name one
waits `infinity`.

### 3. Boot-time orphan sweep

Once at boot, after `recover/2`, delete Ra members that no record lists on this node, comparing UIDs
where the record has them. This builds on `cleanup_data_dir/0`, which nothing calls at boot today. The
sweep:

 * only touches `rabbit_fifo` members, and also removes data directories that are no longer in the Ra
   directory
 * reads the queue records after a `rabbit_khepri:fence/1` call, and deletes nothing if the fence or the
   read fails
 * must run before other nodes can add members to this node, since reconciliation reacts to `node_up`
   with `add_member`

There is no periodic sweep. Orphans that block something are removed when they are hit, by the declare
path or by `recover/2`. A periodic sweep would only reclaim orphans that block nothing, at the risk of
deleting a healthy member.

Status: implemented as a boot step between `recovery` and `pre_flight`, replacing the body of
`cleanup_data_dir/0`. If the metadata store has no quorum at that point, the fence can delay boot by up
to 30 seconds before the sweep is skipped.

### 4. A `deleting` state with an owner (#17457)

A delete marks the queue before it touches Ra, and only the process that marked it may delete the Ra
cluster and then the record:

 1. Set the record's `state` to `deleting` and store the owner pid in `type_state`, with a
    compare-and-swap (`rabbit_db_queue:update/3` already writes conditional on the version it read)
 2. If the record is already `deleting`, check whether the owner is alive. If it is, or it cannot be
    checked, return. If it is dead, take over with the same compare-and-swap
 3. Delete the Ra cluster, with a timeout on every step
 4. Remove the record if this process is still the owner. If some members could not be deleted, clear
    the owner and leave the record in `deleting` for the next delete

This closes the race. No new incarnation can be declared while the record exists, so the owner's Ra
deletes by name can only reach this incarnation. No Ra change is needed. A delete that cannot reach
every member leaves a replicated record of the unfinished delete instead of an orphan, which also covers
#17137.

Details:

 * The queue stays visible. A declare of a `deleting` queue returns an error such as "queue is being
   deleted", not `existing`
 * The owner is a short-lived worker, not the caller. A channel or HTTP handler can outlive its delete
   attempt and would block every later delete
 * The owner check is `erpc:call(Node, erlang, is_process_alive, [Pid], Timeout)`. `false` means dead.
   So do `badarg`, which is expected for a pid from before its node restarted (to be confirmed), and a
   node that has left the cluster. An `erpc` error means the owner may be alive. A monitor is not used
   because confirming a live owner would mean waiting for a timeout
 * `become_leader0/2` sets the state to `live` on every leader repair. It and the other
   `amqqueue:set_state/2` callers must keep `deleting`. `i(state, Q)` must report it
 * `force_delete_queue/1` waits `infinity` today and needs a timeout, since a stuck owner blocks the name
 * It needs a feature flag, since older nodes would ignore the marker

Rejected alternatives: a UID condition on the record delete alone (a stale delete can still kill the new
Ra cluster by name, and the condition then keeps a record with no cluster behind it), and a UID-guarded
`ra:delete_cluster` (a new Ra log command, which older members ignore during a rolling upgrade).

## Known gaps

 * Without change 4, the delete race stays, and changes 2 and 3 clean up after it
 * A stale member on a node that was unreachable while its queue was redeclared has the wrong UID and
   is only removed when that node restarts
 * An orphan whose name is never reused, on a node that never restarts, keeps its disk and memory
 * A record whose Ra cluster is gone is not repaired. `recover/2` starts an empty member, at boot only

## Compatibility

 * Changes 1 to 3 need no feature flag. Change 4 does
 * `ra:start_cluster/3` runs on the leader node, so during a rolling upgrade `members_exist` can come back
   in the old shape. Change 2 then fails the declare as today
 * Change 2 needs a released Ra with change 1, and `dep_ra` back on a hex pin. Change 3 does not

## Tests

 * Ra: UIDs in `members_exist`, `not_owner`, and stopped members found (done), the
   `force_delete_server/4` success path (to do)
 * Declare: succeeds against leftover members, running and stopped (done). Cleans up after a timeout,
   and handles the old `members_exist` shape (to do)
 * `add_member`: succeeds on a node that holds a running leftover member (done), and does not delete a
   member that the record lists with the same UID (covered by `add_member_already_a_member`)
 * Boot sweep: removes a member with no record, one left by `delete_member` while its node was down,
   and an unregistered data directory, and keeps members the record lists (done). Deletes nothing if the
   fence fails (done). A member with a different UID is handled by `recover/2` and not tested separately
 * Change 4: the #17457 reproduction leaves a consistent state. A delete while the owner is alive returns.
   One after the owner died takes over, but two cannot both take over. No takeover when the owner's node
   is unreachable, but a takeover after it restarted. `deleting` survives a leader change. A declare of a
   `deleting` queue fails

## Open questions

 * Keep anything from #17214, such as the `queue_force_deleted` event?
 * Is change 4 part of this work or a follow-up?
 * Should declare failures (ra#668) also mark `deleting` instead of removing the record?
