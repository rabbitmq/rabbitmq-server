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
%% Copyright (c) 2010-2012 VMware, Inc.  All rights reserved.
%%

-module(rabbit_mirror_queue_coordinator).

-export([start_link/4, get_gm/1, ensure_monitoring/2]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-export([joined/2, members_changed/3, handle_msg/3]).

-behaviour(gen_server2).
-behaviour(gm).

-include("rabbit.hrl").
-include("gm_specs.hrl").

-record(state, { q,
                 gm,
                 monitors,
                 death_fun,
                 length_fun
               }).

-define(ONE_SECOND, 1000).

-ifdef(use_specs).

-spec(start_link/4 :: (rabbit_types:amqqueue(), pid() | 'undefined',
                       rabbit_mirror_queue_master:death_fun(),
                       rabbit_mirror_queue_master:length_fun()) ->
                           rabbit_types:ok_pid_or_error()).
-spec(get_gm/1 :: (pid()) -> pid()).
-spec(ensure_monitoring/2 :: (pid(), [pid()]) -> 'ok').

-endif.

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
%% the amqqueue:run_backing_queue callback but that would induce
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
%% queue (or a 'set_length' results in a slave having to drop some
%% messages from the head of its queue), then the queues must be in
%% sync. The only other possibility is that the slave's queue is
%% shorter, and thus the fetch should be ignored. In case slaves are
%% joined to an empty queue which only goes on to receive publishes,
%% they start by asking the master to broadcast its length. This is
%% enough for slaves to always be able to work out when their head
%% does not differ from the master (and is much simpler and cheaper
%% than getting the master to hang on to the guid of the msg at the
%% head of its queue). When a slave is promoted to a master, it
%% unilaterally broadcasts its length, in order to solve the problem
%% of length requests from new slaves being unanswered by a dead
%% master.
%%
%% Obviously, due to the async nature of communication across gm, the
%% slaves can fall behind. This does not matter from a sync pov: if
%% they fall behind and the master dies then a) no publishes are lost
%% because all publishes go to all mirrors anyway; b) the worst that
%% happens is that acks get lost and so messages come back to
%% life. This is no worse than normal given you never get confirmation
%% that an ack has been received (not quite true with QoS-prefetch,
%% but close enough for jazz).
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
%% It is important that we avoid memory leaks due to the death of
%% senders (i.e. channels) and partial publications. A sender
%% publishing a message may fail mid way through the publish and thus
%% only some of the mirrors will receive the message. We need the
%% mirrors to be able to detect this and tidy up as necessary to avoid
%% leaks. If we just had the master monitoring all senders then we
%% would have the possibility that a sender appears and only sends the
%% message to a few of the slaves before dying. Those slaves would
%% then hold on to the message, assuming they'll receive some
%% instruction eventually from the master. Thus we have both slaves
%% and the master monitor all senders they become aware of. But there
%% is a race: if the slave receives a DOWN of a sender, how does it
%% know whether or not the master is going to send it instructions
%% regarding those messages?
%%
%% Whilst the master monitors senders, it can't access its mailbox
%% directly, so it delegates monitoring to the coordinator. When the
%% coordinator receives a DOWN message from a sender, it informs the
%% master via a callback. This allows the master to do any tidying
%% necessary, but more importantly allows the master to broadcast a
%% sender_death message to all the slaves, saying the sender has
%% died. Once the slaves receive the sender_death message, they know
%% that they're not going to receive any more instructions from the gm
%% regarding that sender, thus they throw away any publications from
%% the sender pending publication instructions. However, it is
%% possible that the coordinator receives the DOWN and communicates
%% that to the master before the master has finished receiving and
%% processing publishes from the sender. This turns out not to be a
%% problem: the sender has actually died, and so will not need to
%% receive confirms or other feedback, and should further messages be
%% "received" from the sender, the master will ask the coordinator to
%% set up a new monitor, and will continue to process the messages
%% normally. Slaves may thus receive publishes via gm from previously
%% declared "dead" senders, but again, this is fine: should the slave
%% have just thrown out the message it had received directly from the
%% sender (due to receiving a sender_death message via gm), it will be
%% able to cope with the publication purely from the master via gm.
%%
%% When a slave receives a DOWN message for a sender, if it has not
%% received the sender_death message from the master via gm already,
%% then it will wait 20 seconds before broadcasting a request for
%% confirmation from the master that the sender really has died.
%% Should a sender have only sent a publish to slaves, this allows
%% slaves to inform the master of the previous existence of the
%% sender. The master will thus monitor the sender, receive the DOWN,
%% and subsequently broadcast the sender_death message, allowing the
%% slaves to tidy up. This process can repeat for the same sender:
%% consider one slave receives the publication, then the DOWN, then
%% asks for confirmation of death, then the master broadcasts the
%% sender_death message. Only then does another slave receive the
%% publication and thus set up its monitoring. Eventually that slave
%% too will receive the DOWN, ask for confirmation and the master will
%% monitor the sender again, receive another DOWN, and send out
%% another sender_death message. Given the 20 second delay before
%% requesting death confirmation, this is highly unlikely, but it is a
%% possibility.
%%
%% When the 20 second timer expires, the slave first checks to see
%% whether it still needs confirmation of the death before requesting
%% it. This prevents unnecessary traffic on gm as it allows one
%% broadcast of the sender_death message to satisfy many slaves.
%%
%% If we consider the promotion of a slave at this point, we have two
%% possibilities: that of the slave that has received the DOWN and is
%% thus waiting for confirmation from the master that the sender
%% really is down; and that of the slave that has not received the
%% DOWN. In the first case, in the act of promotion to master, the new
%% master will monitor again the dead sender, and after it has
%% finished promoting itself, it should find another DOWN waiting,
%% which it will then broadcast. This will allow slaves to tidy up as
%% normal. In the second case, we have the possibility that
%% confirmation-of-sender-death request has been broadcast, but that
%% it was broadcast before the master failed, and that the slave being
%% promoted does not know anything about that sender, and so will not
%% monitor it on promotion. Thus a slave that broadcasts such a
%% request, at the point of broadcasting it, recurses, setting another
%% 20 second timer. As before, on expiry of the timer, the slaves
%% checks to see whether it still has not received a sender_death
%% message for the dead sender, and if not, broadcasts a death
%% confirmation request. Thus this ensures that even when a master
%% dies and the new slave has no knowledge of the dead sender, it will
%% eventually receive a death confirmation request, shall monitor the
%% dead sender, receive the DOWN and broadcast the sender_death
%% message.
%%
%% The preceding commentary deals with the possibility of slaves
%% receiving publications from senders which the master does not, and
%% the need to prevent memory leaks in such scenarios. The inverse is
%% also possible: a partial publication may cause only the master to
%% receive a publication. It will then publish the message via gm. The
%% slaves will receive it via gm, will publish it to their BQ and will
%% set up monitoring on the sender. They will then receive the DOWN
%% message and the master will eventually publish the corresponding
%% sender_death message. The slave will then be able to tidy up its
%% state as normal.
%%
%% Recovery of mirrored queues is straightforward: as nodes die, the
%% remaining nodes record this, and eventually a situation is reached
%% in which only one node is alive, which is the master. This is the
%% only node which, upon recovery, will resurrect a mirrored queue:
%% nodes which die and then rejoin as a slave will start off empty as
%% if they have no mirrored content at all. This is not surprising: to
%% achieve anything more sophisticated would require the master and
%% recovering slave to be able to check to see whether they agree on
%% the last seen state of the queue: checking length alone is not
%% sufficient in this case.
%%
%% For more documentation see the comments in bug 23554.
%%
%%----------------------------------------------------------------------------

start_link(Queue, GM, DeathFun, LengthFun) ->
    gen_server2:start_link(?MODULE, [Queue, GM, DeathFun, LengthFun], []).

get_gm(CPid) ->
    gen_server2:call(CPid, get_gm, infinity).

ensure_monitoring(CPid, Pids) ->
    gen_server2:cast(CPid, {ensure_monitoring, Pids}).

%% ---------------------------------------------------------------------------
%% gen_server
%% ---------------------------------------------------------------------------

init([#amqqueue { name = QueueName } = Q, GM, DeathFun, LengthFun]) ->
    GM1 = case GM of
              undefined ->
                  {ok, GM2} = gm:start_link(QueueName, ?MODULE, [self()]),
                  receive {joined, GM2, _Members} ->
                          ok
                  end,
                  GM2;
              _ ->
                  true = link(GM),
                  GM
          end,
    ensure_gm_heartbeat(),
    {ok, #state { q          = Q,
                  gm         = GM1,
                  monitors   = pmon:new(),
                  death_fun  = DeathFun,
                  length_fun = LengthFun },
     hibernate,
     {backoff, ?HIBERNATE_AFTER_MIN, ?HIBERNATE_AFTER_MIN, ?DESIRED_HIBERNATE}}.

handle_call(get_gm, _From, State = #state { gm = GM }) ->
    reply(GM, State).

handle_cast({gm_deaths, Deaths},
            State = #state { q  = #amqqueue { name = QueueName, pid = MPid } })
  when node(MPid) =:= node() ->
    case rabbit_mirror_queue_misc:remove_from_queue(QueueName, Deaths) of
        {ok, MPid, DeadPids} ->
            rabbit_mirror_queue_misc:report_deaths(MPid, true, QueueName,
                                                   DeadPids),
            noreply(State);
        {error, not_found} ->
            {stop, normal, State}
    end;

handle_cast(request_length, State = #state { length_fun = LengthFun }) ->
    ok = LengthFun(),
    noreply(State);

handle_cast({ensure_monitoring, Pids}, State = #state { monitors = Mons }) ->
    noreply(State #state { monitors = pmon:monitor_all(Pids, Mons) }).

handle_info(send_gm_heartbeat, State = #state { gm = GM }) ->
    gm:broadcast(GM, heartbeat),
    ensure_gm_heartbeat(),
    noreply(State);

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
handle_msg([CPid], _From, request_length = Msg) ->
    ok = gen_server2:cast(CPid, Msg);
handle_msg([CPid], _From, {ensure_monitoring, _Pids} = Msg) ->
    ok = gen_server2:cast(CPid, Msg);
handle_msg([_CPid], _From, _Msg) ->
    ok.

%% ---------------------------------------------------------------------------
%% Others
%% ---------------------------------------------------------------------------

noreply(State) ->
    {noreply, State, hibernate}.

reply(Reply, State) ->
    {reply, Reply, State, hibernate}.

ensure_gm_heartbeat() ->
    erlang:send_after(?ONE_SECOND, self(), send_gm_heartbeat).
