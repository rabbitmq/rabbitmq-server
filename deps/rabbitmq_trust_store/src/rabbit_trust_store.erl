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
-export([whitelisted/3]).
-export([start/1, start_link/1]).
-behaviour(gen_server).
-export([init/1, terminate/2,
         handle_call/3, handle_cast/2,
         handle_info/2,
         code_change/3]).

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

-record(entry, {identifier :: tuple()}).


%% OTP Supervision

start(Settings) ->
    gen_server:start(?MODULE, Settings, []).

start_link(Settings) ->
    gen_server:start_link({local, trust_store}, ?MODULE, Settings, []).


%% Client Interface

-spec whitelisted(certificate(), event(), state()) -> outcome().

whitelisted(_, {bad_cert, unknown_ca}, confirmed) ->
    {valid, confirmed};
whitelisted(#'OTPCertificate'{}=C, {bad_cert, unknown_ca}, continue) ->
    E = extract_unique_attributes(C),
    case whitelisted_(E) of
        true ->
            {valid, confirmed};
        false ->
            {fail, "CA not known AND certificate not whitelisted"}
    end;
whitelisted(#'OTPCertificate'{}=C, {bad_cert, selfsigned_peer}, continue) ->
    E = extract_unique_attributes(C),
    case whitelisted_(E) of
        true ->
            {valid, confirmed};
        false ->
            {fail, "certificate not whitelisted"}
    end;
whitelisted(_, {bad_cert, _} = Reason, _) ->
    {fail, Reason};
whitelisted(_, valid, St) ->
    {valid, St};
whitelisted(_, valid_peer, confirmed) ->
    {valid, confirmed};
whitelisted(#'OTPCertificate'{}=C, valid_peer, continue) ->
    E = extract_unique_attributes(C),
    case whitelisted_(E) of
        true ->
            {valid, confirmed};
        false ->
            {valid, continue}
    end;
whitelisted(_, {extension, _}, St) ->
    {unknown, St}.

whitelisted_(#entry{}=E) ->
    gen_server:call(trust_store, {whitelisted, E#entry.identifier}, timeout()).


%% Generic Server Callbacks

init(Settings) ->
    erlang:process_flag(trap_exit, true),
    ets:new(table_name(), table_options()),
    Path = path(Settings),
    Expiry = expiry(Settings),
    tabulate(Path),
    erlang:send_after(Expiry, erlang:self(), {refresh, Path, Expiry}),
    {ok, []}.

handle_call({whitelisted, E}, _Sender, St) ->
    {reply, ets:member(table_name(), E), St}.

handle_cast(_, St) ->
    {noreply, St}.

handle_info({refresh, Path, Expiry}=Notification, St) ->
    true = ets:delete_all_objects(table_name()),
    tabulate(Path),
    erlang:send_after(Expiry, erlang:self(), Notification),
    {noreply, St}.

terminate(shutdown, _St) ->
    true = ets:delete(table_name()).

code_change(_,_,_) ->
    {error, no}.


%% Ancillary & Constants

timeout() ->
    timer:seconds(5).

expiry(Pairs) ->
    {expiry, Time} = lists:keyfind(expiry, 1, Pairs),
    Time.

path(Pairs) ->
    {whitelist, Path} = lists:keyfind(whitelist, 1, Pairs),
    Path.

table_name() ->
    trust_store_whitelist.

table_options() ->
    [private, named_table, set, {keypos, #entry.identifier}, {heir, none}].

tabulate(Path) ->
    {ok, Filenames} = file:list_dir(Path),
    Absolutes = lists:map(fun (Filename) ->
                                  filename:join([Path, Filename])
                          end, Filenames),
    Certificates = lists:map(fun scan_then_parse/1, Absolutes),
    Es = lists:map(fun extract_unique_attributes/1, Certificates),
    ok = insert(Es).

insert([]) ->
    ok;
insert(Es) when is_list(Es) ->
    true = ets:insert_new(table_name(), Es),
    ok.

scan_then_parse(Filename) when is_list(Filename) ->
    {ok, Bin} = file:read_file(Filename),
    [{'Certificate', Data, not_encrypted}] = public_key:pem_decode(Bin),
    public_key:pkix_decode_cert(Data, otp).

extract_unique_attributes(#'OTPCertificate'{}=C) ->
    {Serial, Issuer} = case public_key:pkix_issuer_id(C, other) of
        {error, _Reason} ->
            {ok, Identifier} = public_key:pkix_issuer_id(C, self),
            Identifier;
        {ok, Identifier} ->
            Identifier
    end,
    %% Why change the order of attributes? For the same reason we put
    %% the *most significant figure* first (on the left hand side).
    #entry{identifier = {Issuer, Serial}}.
