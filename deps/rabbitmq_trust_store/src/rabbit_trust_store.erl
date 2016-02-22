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
-behaviour(gen_server).

-export([mode/0, refresh/0]). %% Console Interface.
-export([whitelisted/3]).     %% Client-side Interface (SSL Socket).
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

-record(entry, {filename :: string(), identifier :: tuple()}).
-record(state, {directory_change_time :: integer(), whitelist_directory :: string(), refresh_interval :: integer()}).


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


%% Client (SSL Socket) Interface

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

whitelisted_(#entry{identifier = Id}) ->
    ets:member(table_name(), Id).


%% Generic Server Callbacks

init(Settings) ->
    erlang:process_flag(trap_exit, true),
    ets:new(table_name(), table_options()),
    Path = path(Settings),
    Interval = refresh_interval(Settings),
    Initial = modification_time(Path),
    tabulate(Path),
    if
        Interval =:= 0 ->
            ok;
        Interval  >  0 ->
            erlang:send_after(Interval, erlang:self(), refresh)
    end,
    {ok,
     #state{directory_change_time = Initial,
      whitelist_directory = Path,
      refresh_interval = Interval}}.

handle_call(mode, _, St) ->
    {reply, mode(St), St};
handle_call(refresh, _, St) ->
    {reply, refresh(St), St};
handle_call(_, _, St) ->
    {noreply, St}.

handle_cast(_, St) ->
    {noreply, St}.

handle_info(refresh, #state{refresh_interval=Interval}=St) ->
    New = refresh(St),
    erlang:send_after(Interval, erlang:self(), refresh),
    {noreply, St#state{directory_change_time=New}};
handle_info(_, St) ->
    {noreply, St}.

terminate(shutdown, _St) ->
    true = ets:delete(table_name()).

code_change(_,_,_) ->
    {error, no}.


%% Ancillary & Constants

mode(#state{refresh_interval = I}) ->
    if
        I =:= 0 -> 'manual';
        I  >  0 -> 'automatic'
    end.

refresh(#state{whitelist_directory = Path, directory_change_time = Old}) ->
    New = modification_time(Path),
    case New > Old of
        false ->
            ok;
        true  ->
            tabulate(Path)
    end,
    New.

refresh_interval(Pairs) ->
    {refresh_interval, S} = lists:keyfind(refresh_interval, 1, Pairs),
    timer:seconds(S).

path(Pairs) ->
    {directory, Path} = lists:keyfind(directory, 1, Pairs),
    Path.

table_name() ->
    trust_store_whitelist.

table_options() ->
    [protected,
     named_table,
     set,
     {keypos, #entry.identifier},
     {heir, none}].

modification_time(Path) ->
    {ok, Info} = file:read_file_info(Path, [{time, posix}]),
    Info#file_info.mtime.

already_whitelisted_filenames() ->
    ets:select(table_name(),
        ets:fun2ms(fun (#entry{filename = N}) -> N end)).

one_whitelisted_filename(Name) ->
    ets:fun2ms(fun (#entry{filename = Name}) -> true end).

build_entry(Path, Name) ->
    Absolute    = filename:join(Path, Name),
    Certificate = scan_then_parse(Absolute),
    Unique      = extract_unique_attributes(Certificate),
    _Entry      = Unique#entry{filename = Name}.

do_additions(Before, After, Path) ->
    [ insert(build_entry(Path, Name)) || Name <- After -- Before ].

do_removals(Before, After) ->
    [ delete(Name) || Name <- Before -- After ].

tabulate(Path) ->
    Old = already_whitelisted_filenames(),
    {ok, New} = file:list_dir(Path),
    do_additions(Old, New, Path),
    do_removals(Old, New),
    ok.

delete(Name) ->
    1 = ets:select_delete(table_name(), one_whitelisted_filename(Name)).

insert(Entry) ->
    true = ets:insert_new(table_name(), Entry).

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
