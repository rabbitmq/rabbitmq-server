%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_backing_queue).

-export([info_keys/0]).

-define(INFO_KEYS, [messages_ram, messages_ready_ram,
                    messages_unacknowledged_ram, messages_persistent,
                    message_bytes, message_bytes_ready,
                    message_bytes_unacknowledged, message_bytes_ram,
                    message_bytes_persistent, head_message_timestamp,
                    disk_reads, disk_writes, backing_queue_status,
                    messages_paged_out, message_bytes_paged_out]).

%% We can't specify a per-queue ack/state with callback signatures
-type ack()   :: any().
-type state() :: any().

-type msg_ids() :: [rabbit_types:msg_id()].
-type delivered_publish() :: {mc:state(),
                              rabbit_types:message_properties()}.
-type fetch_result(Ack) ::
        ('empty' | {mc:state(), boolean(), Ack}).
-type drop_result(Ack) ::
        ('empty' | {rabbit_types:msg_id(), Ack}).
-type recovery_terms() :: [term()] | 'non_clean_shutdown'.
-type recovery_info() :: 'new' | recovery_terms().
-type purged_msg_count() :: non_neg_integer().
-type async_callback() ::
        fun ((atom(), fun ((atom(), state()) -> state())) -> 'ok').

-type msg_fun(A) :: fun ((mc:state(), ack(), A) -> A).
-type msg_pred() :: fun ((rabbit_types:message_properties()) -> boolean()).

-type queue_mode() :: atom().
-type queue_version() :: pos_integer().

%% Called on startup with a vhost and a list of durable queue names on this vhost.
%% The queues aren't being started at this point, but this call allows the
%% backing queue to perform any checking necessary for the consistency
%% of those queues, or initialise any other shared resources.
%%
%% The list of queue recovery terms returned as {ok, Terms} must be given
%% in the same order as the list of queue names supplied.
-callback start(rabbit_types:vhost(), [rabbit_amqqueue:name()]) -> rabbit_types:ok(recovery_terms()).

%% Called to tear down any state/resources for vhost. NB: Implementations should
%% not depend on this function being called on shutdown and instead
%% should hook into the rabbit supervision hierarchy.
-callback stop(rabbit_types:vhost()) -> 'ok'.

%% Initialise the backing queue and its state.
%%
%% Takes
%% 1. the amqqueue record
%% 2. a term indicating whether the queue is an existing queue that
%%    should be recovered or not. When 'new' is given, no recovery is
%%    taking place, otherwise a list of recovery terms is given, or
%%    the atom 'non_clean_shutdown' if no recovery terms are available.
%% 3. an asynchronous callback which accepts a function of type
%%    backing-queue-state to backing-queue-state. This callback
%%    function can be safely invoked from any process, which makes it
%%    useful for passing messages back into the backing queue,
%%    especially as the backing queue does not have control of its own
%%    mailbox.
-callback init(amqqueue:amqqueue(), recovery_info(),
               async_callback()) -> state().

%% Called on queue shutdown when queue isn't being deleted.
-callback terminate(any(), state()) -> state().

%% Called when the queue is terminating and needs to delete all its
%% content.
-callback delete_and_terminate(any(), state()) -> state().

%% Called to clean up after a crashed queue. In this case we don't
%% have a process and thus a state(), we are just removing on-disk data.
-callback delete_crashed(amqqueue:amqqueue()) -> 'ok'.

%% Remove all 'fetchable' messages from the queue, i.e. all messages
%% except those that have been fetched already and are pending acks.
-callback purge(state()) -> {purged_msg_count(), state()}.

%% Remove all messages in the queue which have been fetched and are
%% pending acks.
-callback purge_acks(state()) -> state().

%% Publish a message.
-callback publish(mc:state(),
                  rabbit_types:message_properties(), boolean(), pid(),
                  state()) -> state().

%% Called for messages which have already been passed straight
%% out to a client. The queue will be empty for these calls
%% (i.e. saves the round trip through the backing queue).
-callback publish_delivered(mc:state(),
                            rabbit_types:message_properties(), pid(),
                            state())
                           -> {ack(), state()}.

%% Called to inform the BQ about messages which have reached the
%% queue, but are not going to be further passed to BQ.
-callback discard(rabbit_types:msg_id(), pid(), state()) -> state().

%% Return ids of messages which have been confirmed since the last
%% invocation of this function (or initialisation).
%%
%% Message ids should only appear in the result of drain_confirmed
%% under the following circumstances:
%%
%% 1. The message appears in a call to publish_delivered/4 and the
%%    first argument (ack_required) is false; or
%% 2. The message is fetched from the queue with fetch/2 and the first
%%    argument (ack_required) is false; or
%% 3. The message is acked (ack/2 is called for the message); or
%% 4. The message is fully fsync'd to disk in such a way that the
%%    recovery of the message is guaranteed in the event of a crash of
%%    this rabbit node (excluding hardware failure).
%%
%% In addition to the above conditions, a message id may only appear
%% in the result of drain_confirmed if
%% #message_properties.needs_confirming = true when the msg was
%% published (through whichever means) to the backing queue.
%%
%% It is legal for the same message id to appear in the results of
%% multiple calls to drain_confirmed, which means that the backing
%% queue is not required to keep track of which messages it has
%% already confirmed. The confirm will be issued to the publisher the
%% first time the message id appears in the result of
%% drain_confirmed. All subsequent appearances of that message id will
%% be ignored.
-callback drain_confirmed(state()) -> {msg_ids(), state()}.

%% Drop messages from the head of the queue while the supplied
%% predicate on message properties returns true. Returns the first
%% message properties for which the predicate returned false, or
%% 'undefined' if the whole backing queue was traversed w/o the
%% predicate ever returning false.
-callback dropwhile(msg_pred(), state())
                   -> {rabbit_types:message_properties() | undefined, state()}.

%% Like dropwhile, except messages are fetched in "require
%% acknowledgement" mode and are passed, together with their ack tag,
%% to the supplied function. The function is also fed an
%% accumulator. The result of fetchwhile is as for dropwhile plus the
%% accumulator.
-callback fetchwhile(msg_pred(), msg_fun(A), A, state())
                     -> {rabbit_types:message_properties() | undefined,
                         A, state()}.

%% Produce the next message.
-callback fetch(true,  state()) -> {fetch_result(ack()), state()};
               (false, state()) -> {fetch_result(undefined), state()}.

%% Remove the next message.
-callback drop(true,  state()) -> {drop_result(ack()), state()};
              (false, state()) -> {drop_result(undefined), state()}.

%% Acktags supplied are for messages which can now be forgotten
%% about. Must return 1 msg_id per Ack, in the same order as Acks.
-callback ack([ack()], state()) -> {msg_ids(), state()}.

%% Reinsert messages into the queue which have already been delivered
%% and were pending acknowledgement.
-callback requeue([ack()], state()) -> {msg_ids(), state()}.

%% Fold over messages by ack tag. The supplied function is called with
%% each message, its ack tag, and an accumulator.
-callback ackfold(msg_fun(A), A, state(), [ack()]) -> {A, state()}.

%% Fold over all the messages in a queue and return the accumulated
%% results, leaving the queue undisturbed.
-callback fold(fun((mc:state(),
                    rabbit_types:message_properties(),
                    boolean(), A) -> {('stop' | 'cont'), A}),
               A, state()) -> {A, state()}.

%% How long is my queue?
-callback len(state()) -> non_neg_integer().

%% Is my queue empty?
-callback is_empty(state()) -> boolean().

%% What's the queue depth, where depth = length + number of pending acks
-callback depth(state()) -> non_neg_integer().

%% Update the internal message rates.
-callback update_rates(state()) -> state().

%% Should 'timeout' be called as soon as the queue process can manage
%% (either on an empty mailbox, or when a timer fires)?
-callback needs_timeout(state()) -> 'false' | 'timed' | 'idle'.

%% Called (eventually) after needs_timeout returns 'idle' or 'timed'.
%% Note this may be called more than once for each 'idle' or 'timed'
%% returned from needs_timeout
-callback timeout(state()) -> state().

%% Called immediately before the queue hibernates.
-callback handle_pre_hibernate(state()) -> state().

%% Called when more credit has become available for credit_flow.
-callback resume(state()) -> state().

%% Used to help prioritisation in rabbit_amqqueue_process. The rate of
%% inbound messages and outbound messages at the moment.
-callback msg_rates(state()) -> {float(), float()}.

-callback info(atom(), state()) -> any().

%% Passed a function to be invoked with the relevant backing queue's
%% state. Useful for when the backing queue or other components need
%% to pass functions into the backing queue.
-callback invoke(atom(), fun ((atom(), A) -> A), state()) -> state().

%% Called prior to a publish or publish_delivered call. Allows the BQ
%% to signal that it's already seen this message, (e.g. it was published
%% or discarded previously).
-callback is_duplicate(mc:state(), state()) -> {boolean(), state()}.

-callback set_queue_mode(queue_mode(), state()) -> state().

-callback set_queue_version(queue_version(), state()) -> state().

-callback zip_msgs_and_acks([delivered_publish()],
                            [ack()], Acc, state())
                           -> Acc.

-spec info_keys() -> rabbit_types:info_keys().

info_keys() -> ?INFO_KEYS.
