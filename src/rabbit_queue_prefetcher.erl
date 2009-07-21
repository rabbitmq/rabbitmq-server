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

-module(rabbit_queue_prefetcher).

-behaviour(gen_server2).

-export([start_link/0]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(HIBERNATE_AFTER_MIN, 1000).

-record(pstate,
        { msg_buf,
          buf_length,
          target_count,
          fetched_count,
          queue
        }).

%% The design of the prefetcher is based on the following:
%%
%% a) It must issue low-priority (-ve) requests to the disk queue for
%%    the next message.
%% b) If the prefetcher is empty and the amqqueue_process
%%    (mixed_queue) asks it for a message, it must exit immediately,
%%    telling the mixed_queue that it is empty so that the mixed_queue
%%    can then take the more efficient path and communicate with the
%%    disk_queue directly
%% c) No message can accidentally be delivered twice, or lost
%% d) The prefetcher must only cause load when the disk_queue is
%%    otherwise idle, and must not worsen performance in a loaded
%%    situation.
%%
%% As such, it's a little tricky. It must never issue a call to the
%% disk_queue - if it did, then that could potentially block, thus
%% causing pain to the mixed_queue that needs fast answers as to
%% whether the prefetcher has prefetched content or not. It behaves as
%% follows:
%%
%% 1) disk_queue:prefetch(Q)
%%    This is a low priority cast
%%
%% 2) The disk_queue may pick up the cast, at which point it'll read
%%    the next message invoke prefetcher:publish(Msg). Normal priority
%%    cast. Note that in the mean time, the mixed_queue could have
%%    come along, found the prefetcher empty, asked it to exit. This
%%    means the effective "reply" from the disk_queue will go no
%%    where. As a result, the disk_queue must perform no modification
%%    to the status of the message *or the queue* - do not mark the
%%    message delivered, and do not advance the queue. If it did
%%    advance the queue and the msg was then lost, then the queue
%%    would have lost a msg that the mixed_queue would not pick up.
%%
%% 3) The prefetcher hopefully receives the cast from
%%    prefetcher:publish(Msg). It then adds to its internal queue and
%%    calls disk_queue:set_delivered_and_advance(Q) which is a normal
%%    priority cast. This cannot be low-priority because if it was,
%%    the mixed_queue could come along, drain the prefetcher, thus
%%    catching the msg just sent by the disk_queue and then call
%%    disk_queue:deliver(Q) which is normal priority call, which could
%%    overtake the low-priority
%%    disk_queue:set_delivered_and_advance(Q) cast and thus result in
%%    the same msg being delivered by the queue twice.
%%
%% 4) The disk_queue receives the set_delivered_and_advance(Q) cast,
%% marks the msg at the head of the queue Q as delivered, and advances
%% the Q to the next msg.
%%
%% 5) If the prefetcher has not met its target then it goes back to
%%    1). Otherwise it just sits and waits for the mixed_queue to
%%    drain it.
%%
%% Now at some point, the mixed_queue will come along and will call
%% prefetcher:drain(). Normal priority call. The prefetcher then
%% replies with its internal queue and the length of that queue. If
%% the prefetch target was reached, the prefetcher stops normally at
%% this point. If it hasn't been reached, then the prefetcher
%% continues to hang around (it almost certainly has issued a
%% disk_queue:prefetch(Q) cast and is waiting for a reply from the
%% disk_queue).
%%
%% If the mixed_queue calls prefetcher:drain() and the prefetcher's
%% internal queue is empty then the prefetcher replies with 'empty',
%% and it exits. This informs the mixed_queue that it should from now
%% on talk directly with the disk_queue and not via the
%% prefetcher. This is more efficient and the mixed_queue will use
%% normal priority blocking calls to the disk_queue and thus get
%% better service that way. When exiting in this way, two situations
%% could occur:
%%
%% 1) The prefetcher has issued a disk_queue:prefetch(Q) which has not
%% yet been picked up by the disk_queue. This msg won't go away and
%% the disk_queue will eventually find it. However, when it does,
%% it'll simply read the next message from the queue (which could now
%% be empty), possibly populate the cache (no harm done) and try and
%% call prefetcher:publish(Msg) which will go no where. However, the
%% state of the queue and the state of the message has not been
%% altered so the mixed_queue will be able to fetch this message as if
%% it had never been prefetched.
%%
%% 2) The disk_queue has already picked up the disk_queue:prefetch(Q)
%% low priority message and has read the message and replied, by
%% calling prefetcher:publish(Msg). In fact, it's possible that
%% message is directly behind the call from mixed_queue to
%% prefetcher:drain(). Same reasoning as in 1) applies - neither the
%% queue's nor the message's state have been altered, so the
%% mixed_queue can absolutely go and fetch the message again.
%%
%% The only point at which the queue is advanced and the message
%% marked as delivered is when the prefetcher calls
%% disk_queue:set_delivered_and_advance(Q). At this point the message
%% has been received by the prefetcher and so we guarantee it will be
%% passed to the mixed_queue when the mixed_queue tries to drain the
%% prefetcher. We must therefore ensure that this msg can't also be
%% delivered to the mixed_queue directly by the disk_queue through the
%% mixed_queue calling disk_queue:deliver(Q) which is why the
%% disk_queue:set_delivered_and_advance(Q) cast must be normal
%% priority (or at least match the priority of disk_queue:deliver(Q)).
%%
%% Finally, the prefetcher is only created when the mixed_queue is
%% operating in mixed mode and it sees that the next N messages are
%% all on disk. During this phase, the mixed_queue can be asked to go
%% back to disk_only mode. When this happens, it calls
%% prefetcher:drain_and_stop() which behaves like two consecutive
%% calls to drain() - i.e. replies with all prefetched messages and
%% causes the prefetcher to exit.
%%
%% Note there is a flaw here in that we end up marking messages which
%% have come through the prefetcher as delivered even if they don't
%% get delivered (e.g. prefetcher fetches them, then broker
%% dies). However, the alternative is that the mixed_queue must do a
%% call to the disk_queue when it effectively passes them out to the
%% rabbit_writer. This would hurt performance, and even at that stage,
%% we have no guarantee that the message will really go out of the
%% socket. What we do still have is that messages which have the
%% redelivered bit set false really are guaranteed to have not been
%% delivered already. Well, almost: if the disk_queue has a large back
%% log of messages then the prefetcher invocation of
%% disk_queue:set_delivered_and_advance(Q) may not be acted upon
%% before a crash. However, given that the prefetching is operating in
%% lock-step with the disk_queue, this means that at most, 1 (one)
%% message can fail to have its delivered flag raised. The alternative
%% is that disk_queue:set_delivered_and_advance(Q) could be made into
%% a call. However, if the disk_queue is heavily loaded, this can
%% block the prefetcher for some time, which in turn can block the
%% mixed_queue when it wants to drain the prefetcher.

start_link(Queue, Count) ->
    gen_server2:start_link(?MODULE, [Queue, Count], []).

init([Q, Count]) ->
    State = #pstate { msg_buf = queue:new(),
                      buf_length = 0,
                      target_count = Count,
                      fetched_count = 0,
                      queue = Q
                     },
    {ok, State, {binary, ?HIBERNATE_AFTER_MIN}}.

handle_call(_Msg, _From, State) ->
    {reply, confused, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(timeout, State) ->
    {noreply, State, hibernate}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
