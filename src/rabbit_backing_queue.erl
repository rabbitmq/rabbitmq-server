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
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2007-2011 VMware, Inc.  All rights reserved.
%%

-module(rabbit_backing_queue).

-export([behaviour_info/1]).

behaviour_info(callbacks) ->
    [
     %% Called on startup with a list of durable queue names. The
     %% queues aren't being started at this point, but this call
     %% allows the backing queue to perform any checking necessary for
     %% the consistency of those queues, or initialise any other
     %% shared resources.
     {start, 1},

     %% Called to tear down any state/resources. NB: Implementations
     %% should not depend on this function being called on shutdown
     %% and instead should hook into the rabbit supervision hierarchy.
     {stop, 0},

     %% Initialise the backing queue and its state.
     %%
     %% Takes
     %% 1. the amqqueue record
     %% 2. a boolean indicating whether the queue is an existing queue
     %%    that should be recovered
     %% 3. an asynchronous callback which accepts a function of type
     %%    backing-queue-state to backing-queue-state. This callback
     %%    function can be safely invoked from any process, which
     %%    makes it useful for passing messages back into the backing
     %%    queue, especially as the backing queue does not have
     %%    control of its own mailbox.
     %% 4. a synchronous callback. Same as the asynchronous callback
     %%    but waits for completion and returns 'error' on error.
     {init, 4},

     %% Called on queue shutdown when queue isn't being deleted.
     {terminate, 1},

     %% Called when the queue is terminating and needs to delete all
     %% its content.
     {delete_and_terminate, 1},

     %% Remove all messages in the queue, but not messages which have
     %% been fetched and are pending acks.
     {purge, 1},

     %% Publish a message.
     {publish, 4},

     %% Called for messages which have already been passed straight
     %% out to a client. The queue will be empty for these calls
     %% (i.e. saves the round trip through the backing queue).
     {publish_delivered, 5},

     %% Return ids of messages which have been confirmed since
     %% the last invocation of this function (or initialisation).
     %%
     %% Message ids should only appear in the result of
     %% drain_confirmed under the following circumstances:
     %%
     %% 1. The message appears in a call to publish_delivered/4 and
     %%    the first argument (ack_required) is false; or
     %% 2. The message is fetched from the queue with fetch/2 and the
     %%    first argument (ack_required) is false; or
     %% 3. The message is acked (ack/2 is called for the message); or
     %% 4. The message is fully fsync'd to disk in such a way that the
     %%    recovery of the message is guaranteed in the event of a
     %%    crash of this rabbit node (excluding hardware failure).
     %%
     %% In addition to the above conditions, a message id may only
     %% appear in the result of drain_confirmed if
     %% #message_properties.needs_confirming = true when the msg was
     %% published (through whichever means) to the backing queue.
     %%
     %% It is legal for the same message id to appear in the results
     %% of multiple calls to drain_confirmed, which means that the
     %% backing queue is not required to keep track of which messages
     %% it has already confirmed. The confirm will be issued to the
     %% publisher the first time the message id appears in the result
     %% of drain_confirmed. All subsequent appearances of that message
     %% id will be ignored.
     {drain_confirmed, 1},

     %% Drop messages from the head of the queue while the supplied
     %% predicate returns true.
     {dropwhile, 2},

     %% Produce the next message.
     {fetch, 2},

     %% Acktags supplied are for messages which can now be forgotten
     %% about. Must return 1 msg_id per Ack, in the same order as Acks.
     {ack, 2},

     %% A publish, but in the context of a transaction.
     {tx_publish, 5},

     %% Acks, but in the context of a transaction.
     {tx_ack, 3},

     %% Undo anything which has been done in the context of the
     %% specified transaction.
     {tx_rollback, 2},

     %% Commit a transaction. The Fun passed in must be called once
     %% the messages have really been commited. This CPS permits the
     %% possibility of commit coalescing.
     {tx_commit, 4},

     %% Reinsert messages into the queue which have already been
     %% delivered and were pending acknowledgement.
     {requeue, 3},

     %% How long is my queue?
     {len, 1},

     %% Is my queue empty?
     {is_empty, 1},

     %% For the next three functions, the assumption is that you're
     %% monitoring something like the ingress and egress rates of the
     %% queue. The RAM duration is thus the length of time represented
     %% by the messages held in RAM given the current rates. If you
     %% want to ignore all of this stuff, then do so, and return 0 in
     %% ram_duration/1.

     %% The target is to have no more messages in RAM than indicated
     %% by the duration and the current queue rates.
     {set_ram_duration_target, 2},

     %% Optionally recalculate the duration internally (likely to be
     %% just update your internal rates), and report how many seconds
     %% the messages in RAM represent given the current rates of the
     %% queue.
     {ram_duration, 1},

     %% Should 'idle_timeout' be called as soon as the queue process
     %% can manage (either on an empty mailbox, or when a timer
     %% fires)?
     {needs_idle_timeout, 1},

     %% Called (eventually) after needs_idle_timeout returns
     %% 'true'. Note this may be called more than once for each 'true'
     %% returned from needs_idle_timeout.
     {idle_timeout, 1},

     %% Called immediately before the queue hibernates.
     {handle_pre_hibernate, 1},

     %% Exists for debugging purposes, to be able to expose state via
     %% rabbitmqctl list_queues backing_queue_status
     {status, 1},

     %% Passed a function to be invoked with the relevant backing
     %% queue's state. Useful for when the backing queue or other
     %% components need to pass functions into the backing queue.
     {invoke, 3},

     %% Called prior to a publish or publish_delivered call. Allows
     %% the BQ to signal that it's already seen this message and thus
     %% the message should be dropped.
     {is_duplicate, 2}
    ];
behaviour_info(_Other) ->
    undefined.
