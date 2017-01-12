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
%% Copyright (c) 2007-2016 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_trust_store).
-behaviour(gen_server).

-export([mode/0, refresh/0, list/0]). %% Console Interface.
-export([whitelisted/3, is_whitelisted/1]). %% Client-side Interface.
-export([start_link/0]).
-export([init/1, terminate/2,
         handle_call/3, handle_cast/2,
         handle_info/2,
         code_change/3]).

-include_lib("kernel/include/file.hrl").
-include_lib("public_key/include/public_key.hrl").

-type certificate() :: #'OTPCertificate'{}.
-type event()       :: valid_peer
                     | valid
                     | {bad_cert, Other :: atom()
                                | unknown_ca
                                | selfsigned_peer}
                     | {extension, #'Extension'{}}.
-type state()       :: confirmed | continue.
-type outcome()     :: {valid, state()}
                     | {fail, Reason :: term()}
                     | {unknown, state()}.

-record(state, {providers_state :: [{module(), term()}], refresh_interval :: integer()}).


%% OTP Supervision

start_link() ->
    gen_server:start_link({local, trust_store}, ?MODULE, [], []).


%% Console Interface

-spec mode() -> 'automatic' | 'manual'.
mode() ->
    gen_server:call(trust_store, mode).

-spec refresh() -> integer().
refresh() ->
    gen_server:call(trust_store, refresh).

-spec list() -> string().
list() ->
    rabbit_trust_store_storage:list().

%% Client (SSL Socket) Interface

-spec whitelisted(certificate(), event(), state()) -> outcome().
whitelisted(_, {bad_cert, unknown_ca}, confirmed) ->
    {valid, confirmed};
whitelisted(#'OTPCertificate'{}=C, {bad_cert, unknown_ca}, continue) ->
    case is_whitelisted(C) of
        true ->
            {valid, confirmed};
        false ->
            {fail, "CA not known AND certificate not whitelisted"}
    end;
whitelisted(#'OTPCertificate'{}=C, {bad_cert, selfsigned_peer}, continue) ->
    case is_whitelisted(C) of
        true ->
            {valid, confirmed};
        false ->
            {fail, "certificate not whitelisted"}
    end;
whitelisted(_, {bad_cert, _} = Reason, _) ->
    {fail, Reason};
whitelisted(_, valid, St) ->
    {valid, St};
whitelisted(#'OTPCertificate'{}=_, valid_peer, St) ->
    {valid, St};
whitelisted(_, {extension, _}, St) ->
    {unknown, St}.

-spec is_whitelisted(certificate()) -> boolean().
is_whitelisted(#'OTPCertificate'{}=C) ->
    rabbit_trust_store_storage:is_whitelisted(C).

%% Generic Server Callbacks

init([]) ->
    erlang:process_flag(trap_exit, true),
    rabbit_trust_store_storage:init(),
    Config = application:get_all_env(rabbitmq_trust_store),
    ProvidersState = rabbit_trust_store_storage:refresh_certs(Config, []),
    Interval = refresh_interval(Config),
    if
        Interval =:= 0 ->
            ok;
        Interval  >  0 ->
            erlang:send_after(Interval, erlang:self(), refresh)
    end,
    {ok,
     #state{
      providers_state = ProvidersState,
      refresh_interval = Interval}}.

handle_call(mode, _, St) ->
    {reply, mode(St), St};
handle_call(refresh, _, #state{providers_state = ProvidersState} = St) ->
    Config = application:get_all_env(rabbitmq_trust_store),
    NewProvidersState = rabbit_trust_store_storage:refresh_certs(Config, ProvidersState),
    {reply, ok, St#state{providers_state = NewProvidersState}};
handle_call(_, _, St) ->
    {noreply, St}.

handle_cast(_, St) ->
    {noreply, St}.

handle_info(refresh, #state{refresh_interval = Interval,
                            providers_state = ProvidersState} = St) ->
    Config = application:get_all_env(rabbitmq_trust_store),
    NewProvidersState = rabbit_trust_store_storage:refresh_certs(Config, ProvidersState),
    erlang:send_after(Interval, erlang:self(), refresh),
    {noreply, St#state{providers_state = NewProvidersState}};
handle_info(_, St) ->
    {noreply, St}.

terminate(shutdown, _St) ->
    rabbit_trust_store_storage:terminate().

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%% Ancillary & Constants

mode(#state{refresh_interval = I}) ->
    if
        I =:= 0 -> 'manual';
        I  >  0 -> 'automatic'
    end.

refresh_interval(Config) ->
    Seconds = case proplists:get_value(refresh_interval, Config) of
        undefined ->
            default_refresh_interval();
        S when is_integer(S), S >= 0 ->
            S;
        {seconds, S} when is_integer(S), S >= 0 ->
            S
    end,
    timer:seconds(Seconds).

default_refresh_interval() ->
    {ok, I} = application:get_env(rabbitmq_trust_store, default_refresh_interval),
    I.


