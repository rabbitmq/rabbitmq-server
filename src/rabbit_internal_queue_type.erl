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
%%   Portions created by LShift Ltd are Copyright (C) 2007-2009 LShift
%%   Ltd. Portions created by Cohesive Financial Technologies LLC are
%%   Copyright (C) 2007-2009 Cohesive Financial Technologies
%%   LLC. Portions created by Rabbit Technologies Ltd are Copyright
%%   (C) 2007-2009 Rabbit Technologies Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%

-module(rabbit_internal_queue_type).

-export([behaviour_info/1]).

behaviour_info(callbacks) ->
    [
     %% Called with queue name and the persistent msg_store to
     %% use. Transient store is in ?TRANSIENT_MSG_STORE
     {init, 2},

     %% Called on queue shutdown when queue isn't being deleted
     {terminate, 1},

     %% Called when the queue is terminating and needs to delete all
     %% its content.
     {delete_and_terminate, 1},

     %% Remove all messages in the queue, but not messages which have
     %% been fetched and are pending acks.
     {purge, 1},

     %% Publish a message
     {publish, 2},

     %% Called for messages which have already been passed straight
     %% out to a client. The queue will be empty for these calls
     %% (i.e. saves the round trip through the internal queue).
     {publish_delivered, 2},

     {fetch, 1},

     {ack, 2},

     {tx_publish, 2},
     {tx_rollback, 2},
     {tx_commit, 4},

     %% Reinsert messages into the queue which have already been
     %% delivered and were (likely) pending acks.q
     {requeue, 2},

     {len, 1},

     {is_empty, 1},

     {set_queue_duration_target, 2},

     {remeasure_rates, 1},

     {queue_duration, 1},

     %% Can return 'undefined' or a function atom name plus list of
     %% arguments to be invoked in the internal queue module as soon
     %% as the queue process can manage (either on an empty mailbox,
     %% or when a timer fires).
     {needs_sync, 1},

     %% Called immediately before the queue hibernates
     {handle_pre_hibernate, 1},

     %% Exists for debugging purposes, to be able to expose state via
     %% rabbitmqctl list_queues internal_queue_status
     {status, 1}
    ];
behaviour_info(_Other) ->
    undefined.
