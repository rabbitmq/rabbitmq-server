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
-export([start/1, start_link/1]).
-export([init/1, terminate/2,
         handle_call/3, handle_cast/2,
         handle_info/2,
         code_change/3]).

-include_lib("kernel/include/file.hrl").
-include_lib("stdlib/include/ms_transform.hrl").
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

start(Settings) ->
    gen_server:start(?MODULE, Settings, []).

start_link(Settings) ->
    gen_server:start_link({local, trust_store}, ?MODULE, Settings, []).


%% Console Interface

-spec mode() -> 'automatic' | 'manual'.
mode() ->
    gen_server:call(trust_store, mode).

-spec refresh() -> integer().
refresh() ->
    gen_server:call(trust_store, refresh).

-spec list() -> string().
list() ->
    gen_server:call(trust_store, list).

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

init(Settings) ->
    erlang:process_flag(trap_exit, true),
    rabbit_trust_store_storage:init(),
    SettingsAndEnv = lists:ukeymerge(1, application:get_all_env(rabbitmq_trust_store), Settings),
    ProvidersState = rabbit_trust_store_storage:refresh_certs(SettingsAndEnv, []),
    Interval = refresh_interval(SettingsAndEnv),
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
% handle_call(list, _, St) ->
%     {reply, list(St), St};
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

% list(#state{whitelist_directory = Path}) ->
%     Formatted =
%         [format_cert(Path, F, S) ||
%          #entry{filename = F, identifier = {_, S}} <- ets:tab2list(table_name())],
%     to_big_string(Formatted).

mode(#state{refresh_interval = I}) ->
    if
        I =:= 0 -> 'manual';
        I  >  0 -> 'automatic'
    end.

refresh_interval(Pairs) ->
    {refresh_interval, S} = lists:keyfind(refresh_interval, 1, Pairs),
    timer:seconds(S).

% scan_then_parse(Filename) when is_list(Filename) ->
%     {ok, Bin} = file:read_file(Filename),
%     [{'Certificate', Data, not_encrypted}] = public_key:pem_decode(Bin),
%     public_key:pkix_decode_cert(Data, otp).

% to_big_string(Formatted) ->
%     string:join([cert_to_string(X) || X <- Formatted], "~n~n").

% cert_to_string({Name, Serial, Subject, Issuer, Validity}) ->
%     Text =
%         io_lib:format("Name: ~s~nSerial: ~p | 0x~.16.0B~nSubject: ~s~nIssuer: ~s~nValidity: ~p~n",
%                      [ Name, Serial, Serial, Subject, Issuer, Validity]),
%     lists:flatten(Text).

% format_cert(Path, Name, Serial) ->
%     {ok, Bin} = file:read_file(filename:join(Path, Name)),
%     [{'Certificate', Data, not_encrypted}] = public_key:pem_decode(Bin),
%     Validity = rabbit_ssl:peer_cert_validity(Data),
%     Subject = rabbit_ssl:peer_cert_subject(Data),
%     Issuer = rabbit_ssl:peer_cert_issuer(Data),
%     {Name, Serial, Subject, Issuer, Validity}.

