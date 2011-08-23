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
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2011-2011 VMware, Inc.  All rights reserved.
%%

-module(gen_server2_tests).

-compile([export_all]).

-behaviour(gen_server2).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3, prioritise_call/3, prioritise_cast/2,
         prioritise_info/2]).

init([]) ->
    {ok, queue:new()}.

handle_call(await_go, From, Q) ->
    gen_server2:reply(From, ok),
    receive go -> ok end,
    {noreply, queue:in({call, await_go}, Q)};
handle_call(stop, _From, Q) ->
    Q1 = queue:in({call, stop}, Q),
    {stop, normal, queue:to_list(Q1), Q1};
handle_call(Msg, _From, Q) ->
    {reply, ok, queue:in({call, Msg}, Q)}.

handle_cast(Msg, Q) ->
    {noreply, queue:in({cast, Msg}, Q)}.

handle_info(Msg, Q) ->
    {noreply, queue:in({info, Msg}, Q)}.

terminate(_Reason, _Q) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

prioritise_call(await_go, _From, _State)  -> {infinity, infinity};
prioritise_call(stop, _From, _State)      -> {0, 0};
prioritise_call({W, P}, _From, _State)    -> {W, P};
prioritise_call(P, _From, _State)         -> P.

prioritise_cast({W, P}, _State)           -> {W, P};
prioritise_cast(P, _State)                -> P.

prioritise_info({W, P}, _State)           -> {W, P};
prioritise_info(P, _State)                -> P.

test() ->
    {ok, Pid} = gen_server2:start_link(?MODULE, [], []),
    unlink(Pid),
    ok = gen_server2:pcall(Pid, await_go, infinity),
    [gen_server2:pcast(Pid, {W, P})
     || W <- [infinity | lists:seq(0, 10)],
        P <- [infinity | lists:seq(0, 10)]],
    Pid ! go,
    Order = gen_server2:pcall(Pid, stop, infinity),
    Order = [{call, await_go} |
             [{cast, {W, P}} ||
                 P <- [infinity | lists:seq(10, 0, -1)],
                 W <- [infinity | lists:seq(0, 10)]] ++
                 [{call, stop}]],
    passed.
