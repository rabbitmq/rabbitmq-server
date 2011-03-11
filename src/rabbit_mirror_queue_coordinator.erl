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
