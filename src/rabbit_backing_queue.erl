%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2013 GoPivotal, Inc.  All rights reserved.
%%

-module(rabbit_backing_queue).

-ifdef(use_specs).

%% We can't specify a per-queue ack/state with callback signatures
-type(ack()   :: any()).
-type(state() :: any()).

-type(msg_ids() :: [rabbit_types:msg_id()]).
-type(fetch_result(Ack) ::
        ('empty' | {rabbit_types:basic_message(), boolean(), Ack})).
-type(drop_result(Ack) ::
        ('empty' | {rabbit_types:msg_id(), Ack})).
-type(attempt_recovery() :: boolean()).
-type(purged_msg_count() :: non_neg_integer()).
-type(async_callback() ::
        fun ((atom(), fun ((atom(), state()) -> state())) -> 'ok')).
-type(duration() :: ('undefined' | 'infinity' | number())).

-type(msg_fun(A) :: fun ((rabbit_types:basic_message(), ack(), A) -> A)).
-type(msg_pred() :: fun ((rabbit_types:message_properties()) -> boolean())).

%% Called on startup with a list of durable queue names. The queues
%% aren't being started at this point, but this call allows the
%% backing queue to perform any checking necessary for the consistency
%% of those queues, or initialise any other shared resources.
-callback start([rabbit_amqqueue:name()]) -> 'ok'.

%% Called to tear down any state/resources. NB: Implementations should
%% not depend on this function being called on shutdown and instead
%% should hook into the rabbit supervision hierarchy.
-callback stop() -> 'ok'.

%% Initialise the backing queue and its state.
%%
%% Takes
%% 1. the amqqueue record
%% 2. a boolean indicating whether the queue is an existing queue that
%%    should be recovered
%% 3. an asynchronous callback which accepts a function of type
%%    backing-queue-state to backing-queue-state. This callback
%%    function can be safely invoked from any process, which makes it
%%    useful for passing messages back into the backing queue,
%%    especially as the backing queue does not have control of its own
%%    mailbox.
-callback init(rabbit_types:amqqueue(), attempt_recovery(),
               async_callback()) -> state().

%% Called on queue shutdown when queue isn't being deleted.
-callback terminate(any(), state()) -> state().

%% Called when the queue is terminating and needs to delete all its
%% content.
-callback delete_and_terminate(any(), state()) -> state().

%% Remove all 'fetchable' messages from the queue, i.e. all messages
%% except those that have been fetched already and are pending acks.
-callback purge(state()) -> {purged_msg_count(), state()}.

%% Remove all messages in the queue which have been fetched and are
%% pending acks.
-callback purge_acks(state()) -> state().

%% Publish a message.
-callback publish(rabbit_types:basic_message(),
                  rabbit_types:message_properties(), boolean(), pid(),
                  state()) -> state().

%% Called for messages which have already been passed straight
%% out to a client. The queue will be empty for these calls
%% (i.e. saves the round trip through the backing queue).
-callback publish_delivered(rabbit_types:basic_message(),
                            rabbit_types:message_properties(), pid(), state())
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
%% message properties for which the predictate returned false, or
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
-callback fold(fun((rabbit_types:basic_message(),
                    rabbit_types:message_properties(),
                    boolean(), A) -> {('stop' | 'cont'), A}),
               A, state()) -> {A, state()}.

%% How long is my queue?
-callback len(state()) -> non_neg_integer().

%% Is my queue empty?
-callback is_empty(state()) -> boolean().

%% What's the queue depth, where depth = length + number of pending acks
-callback depth(state()) -> non_neg_integer().

%% For the next three functions, the assumption is that you're
%% monitoring something like the ingress and egress rates of the
%% queue. The RAM duration is thus the length of time represented by
%% the messages held in RAM given the current rates. If you want to
%% ignore all of this stuff, then do so, and return 0 in
%% ram_duration/1.

%% The target is to have no more messages in RAM than indicated by the
%% duration and the current queue rates.
-callback set_ram_duration_target(duration(), state()) -> state().

%% Optionally recalculate the duration internally (likely to be just
%% update your internal rates), and report how many seconds the
%% messages in RAM represent given the current rates of the queue.
-callback ram_duration(state()) -> {duration(), state()}.

%% Should 'timeout' be called as soon as the queue process can manage
%% (either on an empty mailbox, or when a timer fires)?
-callback needs_timeout(state()) -> 'false' | 'timed' | 'idle'.

%% Called (eventually) after needs_timeout returns 'idle' or 'timed'.
%% Note this may be called more than once for each 'idle' or 'timed'
%% returned from needs_timeout
-callback timeout(state()) -> state().

%% Called immediately before the queue hibernates.
-callback handle_pre_hibernate(state()) -> state().

%% Exists for debugging purposes, to be able to expose state via
%% rabbitmqctl list_queues backing_queue_status
-callback status(state()) -> [{atom(), any()}].

%% Passed a function to be invoked with the relevant backing queue's
%% state. Useful for when the backing queue or other components need
%% to pass functions into the backing queue.
-callback invoke(atom(), fun ((atom(), A) -> A), state()) -> state().

%% Called prior to a publish or publish_delivered call. Allows the BQ
%% to signal that it's already seen this message, (e.g. it was published
%% or discarded previously) and thus the message should be dropped.
-callback is_duplicate(rabbit_types:basic_message(), state())
                      -> {boolean(), state()}.

-else.

-export([behaviour_info/1]).

behaviour_info(callbacks) ->
    [{start, 1}, {stop, 0}, {init, 3}, {terminate, 2},
     {delete_and_terminate, 2}, {purge, 1}, {purge_acks, 1}, {publish, 5},
     {publish_delivered, 4}, {discard, 3}, {drain_confirmed, 1},
     {dropwhile, 2}, {fetchwhile, 4},
     {fetch, 2}, {ack, 2}, {requeue, 2}, {ackfold, 4}, {fold, 3}, {len, 1},
     {is_empty, 1}, {depth, 1}, {set_ram_duration_target, 2},
     {ram_duration, 1}, {needs_timeout, 1}, {timeout, 1},
     {handle_pre_hibernate, 1}, {status, 1}, {invoke, 3}, {is_duplicate, 2}] ;
behaviour_info(_Other) ->
    undefined.

-endif.
