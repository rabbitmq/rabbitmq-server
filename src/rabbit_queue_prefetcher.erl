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

-export([start_link/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export([publish/2, drain/1, drain_and_stop/1, stop/1]).

-include("rabbit.hrl").

-define(HIBERNATE_AFTER_MIN, 1000).
-define(DESIRED_HIBERNATE, 10000).

-record(pstate,
        { alphas,
          betas,
          queue_mref,
          peruse_cb
        }).

-record(alpha,
        { msg,
          seq_id,
          is_delivered,
          msg_on_disk,
          index_on_disk
        }).

-record(beta,
        { msg_id,
          seq_id,
          is_persistent,
          is_delivered,
          index_on_disk
        }).

%%----------------------------------------------------------------------------
%% Novel
%%----------------------------------------------------------------------------

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
%%    the next message and invoke prefetcher:publish(Msg) - normal
%%    priority cast. Note that in the mean time, the mixed_queue could
%%    have come along, found the prefetcher empty, asked it to
%%    exit. This means the effective "reply" from the disk_queue will
%%    go no where. As a result, the disk_queue should not advance the
%%    queue. However, it does mark the messages as delivered. The
%%    reasoning is that if it didn't, there would be the possibility
%%    that the message was delivered without it being marked as such
%%    on disk. We must maintain the property that a message which is
%%    marked as non-redelivered really hasn't been delivered anywhere
%%    before. The downside is that should the prefetcher not receive
%%    this message, the queue will then fetch the message from the
%%    disk_queue directly, and this message will have its delivered
%%    bit set. The queue will not be advanced though - if it did
%%    advance the queue and the msg was then lost, then the queue
%%    would have lost a msg that the mixed_queue would not pick up.
%%
%% 3) The prefetcher hopefully receives the call from
%%    prefetcher:publish(Msg). It replies immediately, and then adds
%%    to its internal queue. A cast is not sufficient as a pseudo
%%    "reply" here because the mixed_queue could come along, drain the
%%    prefetcher, thus catching the msg just sent by the disk_queue
%%    and then call disk_queue:fetch(Q) which is normal priority call,
%%    which could overtake a reply cast from the prefetcher to the
%%    disk queue, resulting in the same message being delivered
%%    twice. Thus when the disk_queue calls prefetcher:publish(Msg),
%%    it is briefly blocked. However, a) the prefetcher replies
%%    immediately, and b) the prefetcher should never have more than
%%    two items in its mailbox anyway (one from the queue process /
%%    mixed_queue and one from the disk_queue), so this should not
%%    cause a problem to the disk_queue.
%%
%% 4) The disk_queue receives the reply, and advances the Q to the
%%    next msg.
%%
%% 5) If the prefetcher has not met its target then it goes back to
%%    1). Otherwise it just sits and waits for the mixed_queue to
%%    drain it.
%%
%% Now at some point, the mixed_queue will come along and will call
%% prefetcher:drain() - normal priority call. The prefetcher then
%% replies with its internal queue and a flag saying if the prefetcher
%% has finished or is continuing; if the prefetch target was reached,
%% the prefetcher stops normally at this point. If it hasn't been
%% reached, then the prefetcher continues to hang around (it almost
%% certainly has issued a disk_queue:prefetch(Q) cast and is waiting
%% for a reply from the disk_queue).
%%
%% If the mixed_queue calls prefetcher:drain() and the prefetcher's
%% internal queue is empty then the prefetcher replies with 'empty',
%% and it exits. This informs the mixed_queue that it should from now
%% on talk directly with the disk_queue and not via the
%% prefetcher. This is more efficient and the mixed_queue will use
%% normal priority blocking calls to the disk_queue and thus get
%% better service.
%%
%% The prefetcher may at this point have issued a
%% disk_queue:prefetch(Q) cast which has not yet been picked up by the
%% disk_queue. This msg won't go away and the disk_queue will
%% eventually find it. However, when it does, it'll simply read the
%% next message from the queue (which could now be empty), possibly
%% populate the cache (no harm done), mark the message as delivered
%% (oh well, not a spec violation, and better than the alternative)
%% and try and call prefetcher:publish(Msg) which will result in an
%% error, which the disk_queue catches, as the publish call is to a
%% non-existant process. However, the state of the queue has not been
%% altered so the mixed_queue will be able to fetch this message as if
%% it had never been prefetched.
%%
%% The only point at which the queue is advanced is when the
%% prefetcher replies to the publish call. At this point the message
%% has been received by the prefetcher and so we guarantee it will be
%% passed to the mixed_queue when the mixed_queue tries to drain the
%% prefetcher. We must therefore ensure that this msg can't also be
%% delivered to the mixed_queue directly by the disk_queue through the
%% mixed_queue calling disk_queue:fetch(Q) which is why the
%% prefetcher:publish function is a call and not a cast, thus blocking
%% the disk_queue.
%%
%% Finally, the prefetcher is only created when the mixed_queue is
%% operating in mixed mode and it sees that the next N messages are
%% all on disk, and the queue process is about to hibernate. During
%% this phase, the mixed_queue can be asked to go back to disk_only
%% mode. When this happens, it calls prefetcher:drain_and_stop() which
%% behaves like two consecutive calls to drain() - i.e. replies with
%% all prefetched messages and causes the prefetcher to exit.
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
%% delivered already.

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-spec(start_link/1 :: (queue()) ->
             ({'ok', pid()} | 'ignore' | {'error', any()})).
-spec(publish/2 :: (pid(), (message()| 'not_found')) -> 'ok').
-spec(drain/1 :: (pid()) -> ({('finished' | 'continuing' | 'empty'), queue()})).
-spec(drain_and_stop/1 :: (pid()) -> ({('empty' | queue()), queue()})).
-spec(stop/1 :: (pid()) -> 'ok').
             
-endif.

%%----------------------------------------------------------------------------

start_link(Betas) ->
    false = queue:is_empty(Betas), %% ASSERTION
    gen_server2:start_link(?MODULE, [Betas, self()], []).

publish(Prefetcher, Obj = #basic_message {}) ->
    gen_server2:call(Prefetcher, {publish, Obj}, infinity);
publish(Prefetcher, not_found) ->
    gen_server2:call(Prefetcher, publish_empty, infinity).

drain(Prefetcher) ->
    gen_server2:call(Prefetcher, drain, infinity).

drain_and_stop(Prefetcher) ->
    gen_server2:call(Prefetcher, drain_and_stop, infinity).

stop(Prefetcher) ->
    gen_server2:call(Prefetcher, stop, infinity).

%%----------------------------------------------------------------------------

init([Betas, QPid]) when is_pid(QPid) ->
    %% link isn't enough because the signal will not appear if the
    %% queue exits normally. Thus have to use monitor.
    MRef = erlang:monitor(process, QPid),
    Self = self(),
    CB = fun (Result) ->
                 rabbit_misc:with_exit_handler(
                   fun () -> ok end,
                   fun () -> case Result of
                                 {ok, Msg} -> publish(Self, Msg);
                                 not_found -> publish(Self, not_found)
                             end
                   end)
         end,
    State = #pstate { alphas = queue:new(),
                      betas = Betas,
                      queue_mref = MRef,
                      peruse_cb = CB
                    },
    {ok, prefetch(State), infinity, {backoff, ?HIBERNATE_AFTER_MIN,
                                     ?HIBERNATE_AFTER_MIN, ?DESIRED_HIBERNATE}}.

handle_call({publish, Msg = #basic_message { guid = MsgId,
                                             is_persistent = IsPersistent }},
	    DiskQueue, State = #pstate { alphas = Alphas, betas = Betas }) ->
    gen_server2:reply(DiskQueue, ok),
    {{value, #beta { msg_id = MsgId, seq_id = SeqId,
                     is_persistent = IsPersistent,
                     is_delivered = IsDelivered,
                     index_on_disk = IndexOnDisk}}, Betas1} = queue:out(Betas),
    Alphas1 = queue:in(#alpha { msg = Msg, seq_id = SeqId,
                                is_delivered = IsDelivered, msg_on_disk = true,
                                index_on_disk = IndexOnDisk }, Alphas),
    State1 = State #pstate { alphas = Alphas1, betas = Betas1 },
    {Timeout, State2} = case queue:is_empty(Betas1) of
                            true  -> {hibernate, State1};
                            false -> {infinity, prefetch(State1)}
                        end,
    {noreply, State2, Timeout};
handle_call(publish_empty, _From, State) ->
    %% Very odd. This could happen if the queue is deleted or purged
    %% and the mixed queue fails to shut us down.
    {reply, ok, State, hibernate};
handle_call(drain, _From, State = #pstate { alphas = Alphas, betas = Betas }) ->
    case {queue:is_empty(Betas), queue:is_empty(Alphas)} of
        {true , _    } -> {stop, normal, {finished, Alphas}, State};
        {false, true } -> {stop, normal, {empty, Betas}, State};
        {false, false} -> {reply, {continuing, Alphas},
                           State #pstate { alphas = queue:new() }}
    end;
handle_call(drain_and_stop, _From, State = #pstate { alphas = Alphas,
                                                     betas = Betas }) ->
    Res = case queue:is_empty(Alphas) of
              true -> {empty, Betas};
              false -> {Alphas, Betas}
          end,
    {stop, normal, Res, State};
handle_call(stop, _From, State) ->
    {stop, normal, ok, State}.

handle_cast(Msg, State) ->
    exit({unexpected_message_cast_to_prefetcher, Msg, State}).

handle_info({'DOWN', MRef, process, _Pid, _Reason},
            State = #pstate { queue_mref = MRef }) ->
    %% this is the amqqueue_process going down, so we should go down
    %% too
    {stop, normal, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

prefetch(State = #pstate { betas = Betas, peruse_cb = CB }) ->
    {{value, #beta { msg_id = MsgId }}, _Betas1} = queue:out(Betas),
    ok = rabbit_msg_store:peruse(MsgId, CB),
    State.
