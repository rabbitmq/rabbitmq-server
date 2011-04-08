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
%% +-++-----|---------+      |              |
%%   ||     |                |              |
%%   ||   coordinator-+      |              |
%%   ||   +-+---------+      |              |
%%   ||     |                |              |
%%   ||     gm-+ -- -- -- -- gm-+- -- -- -- gm-+- -- --...etc...
%%   ||     +--+             +--+           +--+
%%   ||
%%  consumers
%%
%% The master is merely an implementation of BQ, and thus is invoked
%% through the normal BQ interface by the amqqueue_process. The slaves
%% meanwhile are processes in their own right (as is the
%% coordinator). The coordinator and all slaves belong to the same gm
%% group. Every member of a gm group receives messages sent to the gm
%% group. Because the master is the BQ of amqqueue_process, it doesn't
%% have sole control over its mailbox, and as a result, the master
%% itself cannot be passed messages directly, yet it needs to react to
%% gm events, such as the death of slaves. Thus the master creates the
%% coordinator, and it is the coordinator that is the gm callback
%% module and event handler for the master.
%%
%% Consumers are only attached to the master. Thus the master is
%% responsible for informing all slaves when messages are fetched from
%% the BQ, when they're acked, and when they're requeued.
%%
%% The basic goal is to ensure that all slaves performs actions on
%% their BQ in the same order as the master. Thus the master
%% intercepts all events going to its BQ, and suitably broadcasts
%% these events on the gm. The slaves thus receive two streams of
%% events: one stream is via the gm, and one stream is from channels
%% directly. Note that whilst the stream via gm is guaranteed to be
%% consistently seen by all slaves, the same is not true of the stream
%% via channels. For example, in the event of an unexpected death of a
%% channel during a publish, only some of the mirrors may receive that
%% publish. As a result of this problem, the messages broadcast over
%% the gm contain published content, and thus slaves can operate
%% successfully on messages that they only receive via the gm. The key
%% purpose of also sending messages directly from the channels to the
%% slaves is that without this, in the event of the death of the
%% master, messages can be lost until a suitable slave is promoted.
%%
%% However, there are other reasons as well. For example, if confirms
%% are in use, then there is no guarantee that every slave will see
%% the delivery with the same msg_seq_no. As a result, the slaves have
%% to wait until they've seen both the publish via gm, and the publish
%% via the channel before they have enough information to be able to
%% issue the confirm, if necessary. Either form of publish can arrive
%% first, and a slave can be upgraded to the master at any point
%% during this process. Confirms continue to be issued correctly,
%% however.
%%
%% Because the slave is a full process, it impersonates parts of the
%% amqqueue API. However, it does not need to implement all parts: for
%% example, no ack or consumer-related message can arrive directly at
%% a slave from a channel: it is only publishes that pass both
%% directly to the slaves and go via gm.
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
