%%   The contents of this file are subject to the Mozilla Public License
%%   Version 1.1 (the "License"); you may not use this file except in
%%   compliance with the License. You may obtain a copy of the License at
%%   http://www.mozilla.org/MPL/
%%
%%   Software distributed under the License is distributed on an "AS IS"
%%   basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%%   License for the specific language governing rights and limitations
%%   under the License.
%%
%%   The Original Code is RabbitMQ.
%%
%%   The Initial Developers of the Original Code are LShift Ltd,
%%   Cohesive Financial Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created before 22-Nov-2008 00:00:00 GMT by LShift Ltd,
%%   Cohesive Financial Technologies LLC, or Rabbit Technologies Ltd
%%   are Copyright (C) 2007-2008 LShift Ltd, Cohesive Financial
%%   Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created by LShift Ltd are Copyright (C) 2007-2010 LShift
%%   Ltd. Portions created by Cohesive Financial Technologies LLC are
%%   Copyright (C) 2007-2010 Cohesive Financial Technologies
%%   LLC. Portions created by Rabbit Technologies Ltd are Copyright
%%   (C) 2007-2010 Rabbit Technologies Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%

-module(rabbit_backing_queue).

-export([behaviour_info/1]).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-spec(behaviour_info/1 ::
	(_) -> 'undefined' | [{atom(),0 | 1 | 2 | 3 | 4},...]).

-endif.

%%----------------------------------------------------------------------------

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
     {init, 3},

     %% Called on queue shutdown when queue isn't being deleted.
     {terminate, 1},

     %% Called when the queue is terminating and needs to delete all
     %% its content.
     {delete_and_terminate, 1},

     %% Remove all messages in the queue, but not messages which have
     %% been fetched and are pending acks.
     {purge, 1},

     %% Publish a message.
     {publish, 3},

     %% Called for messages which have already been passed straight
     %% out to a client. The queue will be empty for these calls
     %% (i.e. saves the round trip through the backing queue).
     {publish_delivered, 4},

     %% Drop messages from the head of the queue while the supplied
     %% predicate returns true.
     {dropwhile, 2},

     %% Produce the next message.
     {fetch, 2},

     %% Acktags supplied are for messages which can now be forgotten
     %% about.
     {ack, 2},

     %% A publish, but in the context of a transaction.
     {tx_publish, 4},

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
     {status, 1}
    ];
behaviour_info(_Other) ->
    undefined.
