%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2010-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_mirror_queue_coordinator).

-export([start_link/4, get_gm/1, ensure_monitoring/2]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3, handle_pre_hibernate/1]).

-export([joined/2, members_changed/3, handle_msg/3, handle_terminate/2]).

-behaviour(gen_server2).
-behaviour(gm).

-include_lib("rabbit_common/include/rabbit.hrl").
-include("amqqueue.hrl").
-include("gm_specs.hrl").

-record(state, { q,
                 gm,
                 monitors,
                 death_fun,
                 depth_fun
               }).

%%----------------------------------------------------------------------------
%%
%% Mirror Queues
%%
%% A queue with mirrors consists of the following:
%%
%%  #amqqueue{ pid, slave_pids }
%%             |    |
%%  +----------+    +-------+--------------+-----------...etc...
%%  |                       |              |
%%  V                       V              V
%% amqqueue_process---+    mirror-----+    mirror-----+  ...etc...
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
%% through the normal bq interface by the amqqueue_process. The mirrors
%% meanwhile are processes in their own right (as is the
%% coordinator). The coordinator and all mirrors belong to the same gm
%% group. Every member of a gm group receives messages sent to the gm
%% group. Because the master is the bq of amqqueue_process, it doesn't
%% have sole control over its mailbox, and as a result, the master
%% itself cannot be passed messages directly (well, it could by via
%% the amqqueue:run_backing_queue callback but that would induce
%% additional unnecessary loading on the master queue process), yet it
%% needs to react to gm events, such as the death of mirrors. Thus the
%% master creates the coordinator, and it is the coordinator that is
%% the gm callback module and event handler for the master.
%%
%% Consumers are only attached to the master. Thus the master is
%% responsible for informing all mirrors when messages are fetched from
%% the bq, when they're acked, and when they're requeued.
%%
%% The basic goal is to ensure that all mirrors performs actions on
%% their bqs in the same order as the master. Thus the master
%% intercepts all events going to its bq, and suitably broadcasts
%% these events on the gm. The mirrors thus receive two streams of
%% events: one stream is via the gm, and one stream is from channels
%% directly. Whilst the stream via gm is guaranteed to be consistently
%% seen by all mirrors , the same is not true of the stream via
%% channels. For example, in the event of an unexpected death of a
%% channel during a publish, only some of the mirrors may receive that
%% publish. As a result of this problem, the messages broadcast over
%% the gm contain published content, and thus mirrors can operate
%% successfully on messages that they only receive via the gm.
%%
%% The key purpose of also sending messages directly from the channels
%% to the mirrors is that without this, in the event of the death of
%% the master, messages could be lost until a suitable mirror is
%% promoted. However, that is not the only reason. A mirror cannot send
%% confirms for a message until it has seen it from the
%% channel. Otherwise, it might send a confirm to a channel for a
%% message that it might *never* receive from that channel. This can
%% happen because new mirrors join the gm ring (and thus receive
%% messages from the master) before inserting themselves in the
%% queue's mnesia record (which is what channels look at for routing).
%% As it turns out, channels will simply ignore such bogus confirms,
%% but relying on that would introduce a dangerously tight coupling.
%%
%% Hence the mirrors have to wait until they've seen both the publish
%% via gm, and the publish via the channel before they issue the
%% confirm. Either form of publish can arrive first, and a mirror can
%% be upgraded to the master at any point during this
%% process. Confirms continue to be issued correctly, however.
%%
%% Because the mirror is a full process, it impersonates parts of the
%% amqqueue API. However, it does not need to implement all parts: for
%% example, no ack or consumer-related message can arrive directly at
%% a mirror from a channel: it is only publishes that pass both
%% directly to the mirrors and go via gm.
%%
%% Slaves can be added dynamically. When this occurs, there is no
%% attempt made to sync the current contents of the master with the
%% new mirror, thus the mirror will start empty, regardless of the state
%% of the master. Thus the mirror needs to be able to detect and ignore
%% operations which are for messages it has not received: because of
%% the strict FIFO nature of queues in general, this is
%% straightforward - all new publishes that the new mirror receives via
%% gm should be processed as normal, but fetches which are for
%% messages the mirror has never seen should be ignored. Similarly,
%% acks for messages the mirror never fetched should be
%% ignored. Similarly, we don't republish rejected messages that we
%% haven't seen. Eventually, as the master is consumed from, the
%% messages at the head of the queue which were there before the slave
%% joined will disappear, and the mirror will become fully synced with
%% the state of the master.
%%
%% The detection of the sync-status is based on the depth of the BQs,
%% where the depth is defined as the sum of the length of the BQ (as
%% per BQ:len) and the messages pending an acknowledgement. When the
%% depth of the mirror is equal to the master's, then the mirror is
%% synchronised. We only store the difference between the two for
%% simplicity. Comparing the length is not enough since we need to
%% take into account rejected messages which will make it back into
%% the master queue but can't go back in the mirror, since we don't
%% want "holes" in the mirror queue. Note that the depth, and the
%% length likewise, must always be shorter on the mirror - we assert
%% that in various places. In case mirrors are joined to an empty queue
%% which only goes on to receive publishes, they start by asking the
%% master to broadcast its depth. This is enough for mirrors to always
%% be able to work out when their head does not differ from the master
%% (and is much simpler and cheaper than getting the master to hang on
%% to the guid of the msg at the head of its queue). When a mirror is
%% promoted to a master, it unilaterally broadcasts its depth, in
%% order to solve the problem of depth requests from new mirrors being
%% unanswered by a dead master.
%%
%% Obviously, due to the async nature of communication across gm, the
%% mirrors can fall behind. This does not matter from a sync pov: if
%% they fall behind and the master dies then a) no publishes are lost
%% because all publishes go to all mirrors anyway; b) the worst that
%% happens is that acks get lost and so messages come back to
%% life. This is no worse than normal given you never get confirmation
%% that an ack has been received (not quite true with QoS-prefetch,
%% but close enough for jazz).
%%
%% Because acktags are issued by the bq independently, and because
%% there is no requirement for the master and all mirrors to use the
%% same bq, all references to msgs going over gm is by msg_id. Thus
%% upon acking, the master must convert the acktags back to msg_ids
%% (which happens to be what bq:ack returns), then sends the msg_ids
%% over gm, the mirrors must convert the msg_ids to acktags (a mapping
%% the mirrors themselves must maintain).
%%
%% When the master dies, a mirror gets promoted. This will be the
%% eldest mirror, and thus the hope is that that mirror is most likely
%% to be sync'd with the master. The design of gm is that the
%% notification of the death of the master will only appear once all
%% messages in-flight from the master have been fully delivered to all
%% members of the gm group. Thus at this point, the mirror that gets
%% promoted cannot broadcast different events in a different order
%% than the master for the same msgs: there is no possibility for the
%% same msg to be processed by the old master and the new master - if
%% it was processed by the old master then it will have been processed
%% by the mirror before the mirror was promoted, and vice versa.
%%
%% Upon promotion, all msgs pending acks are requeued as normal, the
%% mirror constructs state suitable for use in the master module, and
%% then dynamically changes into an amqqueue_process with the master
%% as the bq, and the slave's bq as the master's bq. Thus the very
%% same process that was the mirror is now a full amqqueue_process.
%%
%% It is important that we avoid memory leaks due to the death of
%% senders (i.e. channels) and partial publications. A sender
%% publishing a message may fail mid way through the publish and thus
%% only some of the mirrors will receive the message. We need the
%% mirrors to be able to detect this and tidy up as necessary to avoid
%% leaks. If we just had the master monitoring all senders then we
%% would have the possibility that a sender appears and only sends the
%% message to a few of the mirrors before dying. Those mirrors would
%% then hold on to the message, assuming they'll receive some
%% instruction eventually from the master. Thus we have both mirrors
%% and the master monitor all senders they become aware of. But there
%% is a race: if the mirror receives a DOWN of a sender, how does it
%% know whether or not the master is going to send it instructions
%% regarding those messages?
%%
%% Whilst the master monitors senders, it can't access its mailbox
%% directly, so it delegates monitoring to the coordinator. When the
%% coordinator receives a DOWN message from a sender, it informs the
%% master via a callback. This allows the master to do any tidying
%% necessary, but more importantly allows the master to broadcast a
%% sender_death message to all the mirrors , saying the sender has
%% died. Once the mirrors receive the sender_death message, they know
%% that they're not going to receive any more instructions from the gm
%% regarding that sender. However, it is possible that the coordinator
%% receives the DOWN and communicates that to the master before the
%% master has finished receiving and processing publishes from the
%% sender. This turns out not to be a problem: the sender has actually
%% died, and so will not need to receive confirms or other feedback,
%% and should further messages be "received" from the sender, the
%% master will ask the coordinator to set up a new monitor, and
%% will continue to process the messages normally. Slaves may thus
%% receive publishes via gm from previously declared "dead" senders,
%% but again, this is fine: should the mirror have just thrown out the
%% message it had received directly from the sender (due to receiving
%% a sender_death message via gm), it will be able to cope with the
%% publication purely from the master via gm.
%%
%% When a mirror receives a DOWN message for a sender, if it has not
%% received the sender_death message from the master via gm already,
%% then it will wait 20 seconds before broadcasting a request for
%% confirmation from the master that the sender really has died.
%% Should a sender have only sent a publish to mirrors , this allows
%% mirrors to inform the master of the previous existence of the
%% sender. The master will thus monitor the sender, receive the DOWN,
%% and subsequently broadcast the sender_death message, allowing the
%% mirrors to tidy up. This process can repeat for the same sender:
%% consider one mirror receives the publication, then the DOWN, then
%% asks for confirmation of death, then the master broadcasts the
%% sender_death message. Only then does another mirror receive the
%% publication and thus set up its monitoring. Eventually that slave
%% too will receive the DOWN, ask for confirmation and the master will
%% monitor the sender again, receive another DOWN, and send out
%% another sender_death message. Given the 20 second delay before
%% requesting death confirmation, this is highly unlikely, but it is a
%% possibility.
%%
%% When the 20 second timer expires, the mirror first checks to see
%% whether it still needs confirmation of the death before requesting
%% it. This prevents unnecessary traffic on gm as it allows one
%% broadcast of the sender_death message to satisfy many mirrors.
%%
%% If we consider the promotion of a mirror at this point, we have two
%% possibilities: that of the mirror that has received the DOWN and is
%% thus waiting for confirmation from the master that the sender
%% really is down; and that of the mirror that has not received the
%% DOWN. In the first case, in the act of promotion to master, the new
%% master will monitor again the dead sender, and after it has
%% finished promoting itself, it should find another DOWN waiting,
%% which it will then broadcast. This will allow mirrors to tidy up as
%% normal. In the second case, we have the possibility that
%% confirmation-of-sender-death request has been broadcast, but that
%% it was broadcast before the master failed, and that the mirror being
%% promoted does not know anything about that sender, and so will not
%% monitor it on promotion. Thus a mirror that broadcasts such a
%% request, at the point of broadcasting it, recurses, setting another
%% 20 second timer. As before, on expiry of the timer, the mirrors
%% checks to see whether it still has not received a sender_death
%% message for the dead sender, and if not, broadcasts a death
%% confirmation request. Thus this ensures that even when a master
%% dies and the new mirror has no knowledge of the dead sender, it will
%% eventually receive a death confirmation request, shall monitor the
%% dead sender, receive the DOWN and broadcast the sender_death
%% message.
%%
%% The preceding commentary deals with the possibility of mirrors
%% receiving publications from senders which the master does not, and
%% the need to prevent memory leaks in such scenarios. The inverse is
%% also possible: a partial publication may cause only the master to
%% receive a publication. It will then publish the message via gm. The
%% mirrors will receive it via gm, will publish it to their BQ and will
%% set up monitoring on the sender. They will then receive the DOWN
%% message and the master will eventually publish the corresponding
%% sender_death message. The mirror will then be able to tidy up its
%% state as normal.
%%
%% Recovery of mirrored queues is straightforward: as nodes die, the
%% remaining nodes record this, and eventually a situation is reached
%% in which only one node is alive, which is the master. This is the
%% only node which, upon recovery, will resurrect a mirrored queue:
%% nodes which die and then rejoin as a mirror will start off empty as
%% if they have no mirrored content at all. This is not surprising: to
%% achieve anything more sophisticated would require the master and
%% recovering mirror to be able to check to see whether they agree on
%% the last seen state of the queue: checking depth alone is not
%% sufficient in this case.
%%
%% For more documentation see the comments in bug 23554.
%%
%%----------------------------------------------------------------------------

-spec start_link
        (amqqueue:amqqueue(), pid() | 'undefined',
         rabbit_mirror_queue_master:death_fun(),
         rabbit_mirror_queue_master:depth_fun()) ->
            rabbit_types:ok_pid_or_error().

start_link(Queue, GM, DeathFun, DepthFun) ->
    gen_server2:start_link(?MODULE, [Queue, GM, DeathFun, DepthFun], []).

-spec get_gm(pid()) -> pid().

get_gm(CPid) ->
    gen_server2:call(CPid, get_gm, infinity).

-spec ensure_monitoring(pid(), [pid()]) -> 'ok'.

ensure_monitoring(CPid, Pids) ->
    gen_server2:cast(CPid, {ensure_monitoring, Pids}).

%% ---------------------------------------------------------------------------
%% gen_server
%% ---------------------------------------------------------------------------

init([Q, GM, DeathFun, DepthFun]) when ?is_amqqueue(Q) ->
    QueueName = amqqueue:get_name(Q),
    ?store_proc_name(QueueName),
    GM1 = case GM of
              undefined ->
                  {ok, GM2} = gm:start_link(
                                QueueName, ?MODULE, [self()],
                                fun rabbit_misc:execute_mnesia_transaction/1),
                  receive {joined, GM2, _Members} ->
                          ok
                  end,
                  GM2;
              _ ->
                  true = link(GM),
                  GM
          end,
    {ok, #state { q          = Q,
                  gm         = GM1,
                  monitors   = pmon:new(),
                  death_fun  = DeathFun,
                  depth_fun  = DepthFun },
     hibernate,
     {backoff, ?HIBERNATE_AFTER_MIN, ?HIBERNATE_AFTER_MIN, ?DESIRED_HIBERNATE}}.

handle_call(get_gm, _From, State = #state { gm = GM }) ->
    reply(GM, State).

handle_cast({gm_deaths, DeadGMPids}, State = #state{q = Q}) when ?amqqueue_pid_runs_on_local_node(Q) ->
    QueueName = amqqueue:get_name(Q),
    MPid = amqqueue:get_pid(Q),
    case rabbit_mirror_queue_misc:remove_from_queue(
           QueueName, MPid, DeadGMPids) of
        {ok, MPid, DeadPids, ExtraNodes} ->
            rabbit_mirror_queue_misc:report_deaths(MPid, true, QueueName,
                                                   DeadPids),
            rabbit_mirror_queue_misc:add_mirrors(QueueName, ExtraNodes, async),
            noreply(State);
        {ok, _MPid0, DeadPids, _ExtraNodes} ->
            %% see rabbitmq-server#914;
            %% Different mirror is now master, stop current coordinator normally.
            %% Initiating queue is now mirror and the least we could do is report
            %% deaths which we 'think' we saw.
            %% NOTE: Reported deaths here, could be inconsistent.
            rabbit_mirror_queue_misc:report_deaths(MPid, false, QueueName,
                                                   DeadPids),
            {stop, shutdown, State};
        {error, not_found} ->
            {stop, normal, State};
        {error, {not_synced, _}} ->
            rabbit_log:error("Mirror queue ~p in unexpected state."
                             " Promoted to master but already a master.",
                             [QueueName]),
            error(unexpected_mirrored_state)
    end;

handle_cast(request_depth, State = #state{depth_fun = DepthFun, q = QArg}) when ?is_amqqueue(QArg) ->
    QName = amqqueue:get_name(QArg),
    MPid = amqqueue:get_pid(QArg),
    case rabbit_amqqueue:lookup(QName) of
        {ok, QFound} when ?amqqueue_pid_equals(QFound, MPid) ->
            ok = DepthFun(),
            noreply(State);
        _ ->
            {stop, shutdown, State}
    end;

handle_cast({ensure_monitoring, Pids}, State = #state { monitors = Mons }) ->
    noreply(State #state { monitors = pmon:monitor_all(Pids, Mons) });

handle_cast({delete_and_terminate, {shutdown, ring_shutdown}}, State) ->
    {stop, normal, State};
handle_cast({delete_and_terminate, Reason}, State) ->
    {stop, Reason, State}.

handle_info({'DOWN', _MonitorRef, process, Pid, _Reason},
            State = #state { monitors  = Mons,
                             death_fun = DeathFun }) ->
    noreply(case pmon:is_monitored(Pid, Mons) of
                false -> State;
                true  -> ok = DeathFun(Pid),
                         State #state { monitors = pmon:erase(Pid, Mons) }
            end);

handle_info(Msg, State) ->
    {stop, {unexpected_info, Msg}, State}.

terminate(_Reason, #state{}) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

handle_pre_hibernate(State = #state { gm = GM }) ->
    %% Since GM notifications of deaths are lazy we might not get a
    %% timely notification of mirror death if policy changes when
    %% everything is idle. So cause some activity just before we
    %% sleep. This won't cause us to go into perpetual motion as the
    %% heartbeat does not wake up coordinator or mirrors.
    gm:broadcast(GM, hibernate_heartbeat),
    {hibernate, State}.

%% ---------------------------------------------------------------------------
%% GM
%% ---------------------------------------------------------------------------

-spec joined(args(), members()) -> callback_result().

joined([CPid], Members) ->
    CPid ! {joined, self(), Members},
    ok.

-spec members_changed(args(), members(),members()) -> callback_result().

members_changed([_CPid], _Births, []) ->
    ok;
members_changed([CPid],  _Births, Deaths) ->
    ok = gen_server2:cast(CPid, {gm_deaths, Deaths}).

-spec handle_msg(args(), pid(), any()) -> callback_result().

handle_msg([CPid], _From, request_depth = Msg) ->
    ok = gen_server2:cast(CPid, Msg);
handle_msg([CPid], _From, {ensure_monitoring, _Pids} = Msg) ->
    ok = gen_server2:cast(CPid, Msg);
handle_msg([_CPid], _From, {delete_and_terminate, _Reason}) ->
    %% We tell GM to stop, but we don't instruct the coordinator to
    %% stop yet. The GM will first make sure all pending messages were
    %% actually delivered. Then it calls handle_terminate/2 below so the
    %% coordinator is stopped.
    %%
    %% If we stop the coordinator right now, remote mirrors could see the
    %% coordinator DOWN before delete_and_terminate was delivered to all
    %% GMs. One of those GM would be promoted as the master, and this GM
    %% would hang forever, waiting for other GMs to stop.
    {stop, {shutdown, ring_shutdown}};
handle_msg([_CPid], _From, _Msg) ->
    ok.

-spec handle_terminate(args(), term()) -> any().

handle_terminate([CPid], Reason) ->
    ok = gen_server2:cast(CPid, {delete_and_terminate, Reason}),
    ok.

%% ---------------------------------------------------------------------------
%% Others
%% ---------------------------------------------------------------------------

noreply(State) ->
    {noreply, State, hibernate}.

reply(Reply, State) ->
    {reply, Reply, State, hibernate}.
