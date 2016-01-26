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
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2016 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_trust_store).
-export([interface/3]).
-export([start/1, start_link/1,
         stop/1]).
-behaviour(gen_server).
-export([init/1, terminate/2,
         handle_call/3, handle_cast/2,
         handle_info/2,
         code_change/3]).


%% OTP Supervision

start({whitelist, Path}) ->
    gen_server:start(?MODULE, {whitelist, Path}, []).

start_link({whitelist, Path}) ->
    gen_server:start_link({local, trust_store}, ?MODULE, {whitelist, Path}, []).

stop(Id) ->
    gen_server:call(Id, stop).


%% API

interface(_, {bad_cert, _} = Reason, _) ->
    rabbit_log:error("interface/3 clause ~p.~n", [1]),
    {fail, Reason};
interface(_, {extension, _}, _) ->
    rabbit_log:error("interface/3 clause ~p.~n", [2]),
    {unknown, []};
interface(_, valid, _) ->
    rabbit_log:error("interface/3 clause ~p.~n", [3]),
    {valid, []};
interface(_, valid_peer, _) ->
    rabbit_log:error("interface/3 clause ~p.~n", [4]),
    {valid, []}.


%% Generic Server Callback

init({whitelist, _Path}) ->
    {ok, {}}.

handle_call(stop, _, St) ->
    {stop, normal, ok, St}.

handle_cast(stop, St) ->
    {stop, normal, St}. %% OTP 18: Generic Server machinery will call `terminate/2'.

handle_info(_, St) ->
    {noreply, St}.

terminate(_, _St) ->
    ok.

code_change(_,_,_) ->
    {error, no}.
