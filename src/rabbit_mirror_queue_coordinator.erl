%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2007-2010 VMware, Inc.  All rights reserved.
%%

-module(rabbit_mirror_queue_coordinator).

-export([start_link/2, get_gm/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-export([joined/2, members_changed/3, handle_msg/3]).

-behaviour(gen_server2).
-behaviour(gm).

-include("rabbit.hrl").
-include("gm_specs.hrl").

-record(state, { q,
                 gm
               }).

-define(ONE_SECOND, 1000).

%%----------------------------------------------------------------------------
%%
%% Mirror Queues
%%
%% A queue with mirrors consists of the following:
%%
%%  #amqqueue{ pid, mirror_pids }
%%             |    |
%%  +----------+    +-------+--------------+-----------...etc...
%%  |                       |              |
%%  V                       V              V
%% amqqueue_process---+    slave-----+    slave-----+  ...etc...
%% | BQ = master----+ |    | BQ = vq |    | BQ = vq |
%% |      | BQ = vq | |    +-+-------+    +-+-------+
%% |      +-+-------+ |      |              |
%% +-++-----|---------+      |              |  (some details elided)
%%   ||     |                |              |
%%   ||   coordinator-+      |              |
%%   ||   +-+---------+      |              |
%%   ||     |                |              |
%%   ||     gm-+ -- -- -- -- gm-+- -- -- -- gm-+- -- --...etc...
%%   ||     +--+             +--+           +--+
%%   ||
%%  consumers
%%
%% The master is merely an implementation of bq, and thus is invoked
%% through the normal bq interface by the amqqueue_process. The slaves
%% meanwhile are processes in their own right (as is the
%% coordinator). The coordinator and all slaves belong to the same gm
%% group. Every member of a gm group receives messages sent to the gm
%% group. Because the master is the bq of amqqueue_process, it doesn't
%% have sole control over its mailbox, and as a result, the master
%% itself cannot be passed messages directly (well, it could by via
%% the amqqueue:run_backing_queue_async callback but that would induce
%% additional unnecessary loading on the master queue process), yet it
%% needs to react to gm events, such as the death of slaves. Thus the
%% master creates the coordinator, and it is the coordinator that is
%% the gm callback module and event handler for the master.
%%
%% Consumers are only attached to the master. Thus the master is
%% responsible for informing all slaves when messages are fetched from
%% the bq, when they're acked, and when they're requeued.
%%
%% The basic goal is to ensure that all slaves performs actions on
%% their bqs in the same order as the master. Thus the master
%% intercepts all events going to its bq, and suitably broadcasts
%% these events on the gm. The slaves thus receive two streams of
%% events: one stream is via the gm, and one stream is from channels
%% directly. Whilst the stream via gm is guaranteed to be consistently
%% seen by all slaves, the same is not true of the stream via
%% channels. For example, in the event of an unexpected death of a
%% channel during a publish, only some of the mirrors may receive that
%% publish. As a result of this problem, the messages broadcast over
%% the gm contain published content, and thus slaves can operate
%% successfully on messages that they only receive via the gm. The key
%% purpose of also sending messages directly from the channels to the
%% slaves is that without this, in the event of the death of the
%% master, messages could be lost until a suitable slave is promoted.
%%
%% However, that is not the only reason. For example, if confirms are
%% in use, then there is no guarantee that every slave will see the
%% delivery with the same msg_seq_no. As a result, the slaves have to
%% wait until they've seen both the publish via gm, and the publish
%% via the channel before they have enough information to be able to
%% perform the publish to their own bq, and subsequently issue the
%% confirm, if necessary. Either form of publish can arrive first, and
%% a slave can be upgraded to the master at any point during this
%% process. Confirms continue to be issued correctly, however.
%%
%% Because the slave is a full process, it impersonates parts of the
%% amqqueue API. However, it does not need to implement all parts: for
%% example, no ack or consumer-related message can arrive directly at
%% a slave from a channel: it is only publishes that pass both
%% directly to the slaves and go via gm.
%%
%% Slaves can be added dynamically. When this occurs, there is no
%% attempt made to sync the current contents of the master with the
%% new slave, thus the slave will start empty, regardless of the state
%% of the master. Thus the slave needs to be able to detect and ignore
%% operations which are for messages it has not received: because of
%% the strict FIFO nature of queues in general, this is
%% straightforward - all new publishes that the new slave receives via
%% gm should be processed as normal, but fetches which are for
%% messages the slave has never seen should be ignored. Similarly,
%% acks for messages the slave never fetched should be
%% ignored. Eventually, as the master is consumed from, the messages
%% at the head of the queue which were there before the slave joined
%% will disappear, and the slave will become fully synced with the
%% state of the master. The detection of the sync-status of a slave is
%% done entirely based on length: if the slave and the master both
%% agree on the length of the queue after the fetch of the head of the
%% queue, then the queues must be in sync. The only other possibility
%% is that the slave's queue is shorter, and thus the fetch should be
%% ignored.
%%
%% Because acktags are issued by the bq independently, and because
%% there is no requirement for the master and all slaves to use the
%% same bq, all references to msgs going over gm is by msg_id. Thus
%% upon acking, the master must convert the acktags back to msg_ids
%% (which happens to be what bq:ack returns), then sends the msg_ids
%% over gm, the slaves must convert the msg_ids to acktags (a mapping
%% the slaves themselves must maintain).
%%
%% When the master dies, a slave gets promoted. This will be the
%% eldest slave, and thus the hope is that that slave is most likely
%% to be sync'd with the master. The design of gm is that the
%% notification of the death of the master will only appear once all
%% messages in-flight from the master have been fully delivered to all
%% members of the gm group. Thus at this point, the slave that gets
%% promoted cannot broadcast different events in a different order
%% than the master for the same msgs: there is no possibility for the
%% same msg to be processed by the old master and the new master - if
%% it was processed by the old master then it will have been processed
%% by the slave before the slave was promoted, and vice versa.
%%
%% Upon promotion, all msgs pending acks are requeued as normal, the
%% slave constructs state suitable for use in the master module, and
%% then dynamically changes into an amqqueue_process with the master
%% as the bq, and the slave's bq as the master's bq. Thus the very
%% same process that was the slave is now a full amqqueue_process.
%%
%% In the event of channel failure, there is the possibility that a
%% msg that was being published only makes it to some of the
%% mirrors. If it makes it to the master, then the master will push
%% the entire message onto gm, and all the slaves will publish it to
%% their bq, even though they may not receive it directly from the
%% channel. This currently will create a small memory leak in the
%% slave's msg_id_status mapping as the slaves will expect that
%% eventually they'll receive the msg from the channel. If the message
%% does not make it to the master then the slaves that receive it will
%% hold onto the message, assuming it'll eventually appear via
%% gm. Again, this will currently result in a memory leak, though this
%% time, it's the entire message rather than tracking the status of
%% the message, which is potentially much worse. This may eventually
%% be solved by monitoring publishing channels in some way.
%%
%% We don't support transactions on mirror queues. To do so is
%% challenging. The underlying bq is free to add the contents of the
%% txn to the queue proper at any point after the tx.commit comes in
%% but before the tx.commit-ok goes out. This means that it is not
%% safe for all mirrors to simply issue the bq:tx_commit at the same
%% time, as the addition of the txn's contents to the queue may
%% subsequently be inconsistently interwoven with other actions on the
%% bq. The solution to this is, in the master, wrap the PostCommitFun
%% and do the gm:broadcast in there: at that point, you're in the bq
%% (well, there's actually nothing to stop that function being invoked
%% by some other process, but let's pretend for now: you could always
%% use run_backing_queue to ensure you really are in the queue process
%% (the _async variant would be unsafe from an ordering pov)), the
%% gm:broadcast is safe because you don't have to worry about races
%% with other gm:broadcast calls (same process). Thus this signal
%% would indicate sufficiently to all the slaves that they must insert
%% the complete contents of the txn at precisely this point in the
%% stream of events.
%%
%% However, it's quite difficult for the slaves to make that happen:
%% they would be forced to issue the bq:tx_commit at that point, but
%% then stall processing any further instructions from gm until they
%% receive the notification from their bq that the tx_commit has fully
%% completed (i.e. they need to treat what is an async system as being
%% fully synchronous). This is not too bad (apart from the
%% vomit-inducing notion of it all): just need a queue of instructions
%% from the GM; but then it gets rather worse when you consider what
%% needs to happen if the master dies at this point and the slave in
%% the middle of this tx_commit needs to be promoted.
%%
%% Finally, we can't possibly hope to make transactions atomic across
%% mirror queues, and it's not even clear that that's desirable: if a
%% slave fails whilst there's an open transaction in progress then
%% when the channel comes to commit the txn, it will detect the
%% failure and destroy the channel. However, the txn will have
%% actually committed successfully in all the other mirrors (including
%% master). To do this bit properly would require 2PC and all the
%% baggage that goes with that.
%%
%%----------------------------------------------------------------------------

start_link(Queue, GM) ->
    gen_server2:start_link(?MODULE, [Queue, GM], []).

get_gm(CPid) ->
    gen_server2:call(CPid, get_gm, infinity).

%% ---------------------------------------------------------------------------
%% gen_server
%% ---------------------------------------------------------------------------

init([#amqqueue { name = QueueName } = Q, GM]) ->
    GM1 = case GM of
              undefined ->
                  ok = gm:create_tables(),
                  {ok, GM2} = gm:start_link(QueueName, ?MODULE, [self()]),
                  receive {joined, GM2, _Members} ->
                          ok
                  end,
                  GM2;
              _ ->
                  true = link(GM),
                  GM
          end,
    {ok, _TRef} =
        timer:apply_interval(?ONE_SECOND, gm, broadcast, [GM1, heartbeat]),
    {ok, #state { q = Q, gm = GM1 }, hibernate,
     {backoff, ?HIBERNATE_AFTER_MIN, ?HIBERNATE_AFTER_MIN, ?DESIRED_HIBERNATE}}.

handle_call(get_gm, _From, State = #state { gm = GM }) ->
    reply(GM, State).

handle_cast({gm_deaths, Deaths},
            State = #state { q  = #amqqueue { name = QueueName } }) ->
    rabbit_log:info("Master ~p saw deaths ~p for ~s~n",
                    [self(), Deaths, rabbit_misc:rs(QueueName)]),
    case rabbit_mirror_queue_misc:remove_from_queue(QueueName, Deaths) of
        {ok, Pid} when node(Pid) =:= node() ->
            noreply(State);
        {error, not_found} ->
            {stop, normal, State}
    end.

handle_info(Msg, State) ->
    {stop, {unexpected_info, Msg}, State}.

terminate(_Reason, #state{}) ->
    %% gen_server case
    ok;
terminate([_CPid], _Reason) ->
    %% gm case
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ---------------------------------------------------------------------------
%% GM
%% ---------------------------------------------------------------------------

joined([CPid], Members) ->
    CPid ! {joined, self(), Members},
    ok.

members_changed([_CPid], _Births, []) ->
    ok;
members_changed([CPid], _Births, Deaths) ->
    ok = gen_server2:cast(CPid, {gm_deaths, Deaths}).

handle_msg([_CPid], _From, heartbeat) ->
    ok;
handle_msg([_CPid], _From, _Msg) ->
    ok.

%% ---------------------------------------------------------------------------
%% Others
%% ---------------------------------------------------------------------------

noreply(State) ->
    {noreply, State, hibernate}.

reply(Reply, State) ->
    {reply, Reply, State, hibernate}.
