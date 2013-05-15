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
%% The Original Code is RabbitMQ Federation.
%%
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2007-2013 VMware, Inc.  All rights reserved.
%%

-module(rabbit_federation_queue).

-include_lib("amqp_client/include/amqp_client.hrl").
-include("rabbit_federation.hrl").

-behaviour(gen_server2).

-export([start_link/1, go/1, stop/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {queue, links}).

%% TODO the whole supervisor thing
start_link(Q) ->
    {ok, Pid} = gen_server2:start_link(?MODULE, Q, [{timeout, infinity}]),
    put(federated_queue_coordinator, Pid),
    {ok, Pid}.

go(Q)   -> maybe_cast(go).
stop(Q) -> maybe_cast(stop).

maybe_cast(Msg) ->
    %%io:format("~p maybe cast ~p~n", [self(), Msg]),
    case get(federated_queue_coordinator) of
        undefined -> ok;
        Pid       -> gen_server2:cast(Pid, Msg)
    end.

%%----------------------------------------------------------------------------

init(Q) ->
    FakeX = rabbit_exchange:lookup_or_die(
              rabbit_misc:r(<<"/">>, exchange, <<"">>)),
    Upstreams = rabbit_federation_upstream:from_set(<<"all">>, FakeX),
    Links =
        [begin
             {ok, Pid} = rabbit_federation_queue_link:start_link(
                           rabbit_federation_upstream:to_params(U, FakeX),
                           self(), Q),
             Pid
         end || U <- Upstreams],
    {ok, #state{queue = Q, links = Links}}.

handle_call(Msg, _From, State) ->
    {stop, {unexpected_call, Msg}, State}.

handle_cast(go, State = #state{links = Links}) ->
    %%io:format("go ~p~n", [Links]),
    [rabbit_federation_queue_link:go(L) || L <- Links],
    {noreply, State};

handle_cast(stop, State = #state{links = Links}) ->
    [rabbit_federation_queue_link:stop(L) || L <- Links],
    {noreply, State};

handle_cast(Msg, State) ->
    {stop, {unexpected_cast, Msg}, State}.

handle_info(Msg, State) ->
    {stop, {unexpected_info, Msg}, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
