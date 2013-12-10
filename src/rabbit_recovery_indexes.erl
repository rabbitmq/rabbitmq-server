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
%% Copyright (c) 2007-2013 GoPivotal, Inc.  All rights reserved.
%%

%% We use a gen_server simply so that during the terminate/2 call
%% (i.e., during shutdown), we can sync/flush the dets table to disk.

-module(rabbit_recovery_indexes).

-behaviour(gen_server).

-export([recover/0,
         start_link/0,
         store_recovery_terms/2,
         check_clean_shutdown/1,
         read_recovery_terms/1,
         remove_recovery_terms/1,
         flush/0]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-ifdef(use_specs).

-spec(recover() -> 'ok').
-spec(start_link() -> rabbit_types:ok_pid_or_error()).
-spec(store_recovery_terms(
        Name  :: rabbit_misc:resource_name(),
        Terms :: term()) -> rabbit_types:ok_or_error(term())).
-spec(check_clean_shutdown(
        rabbit_misc:resource_name()) ->
             boolean() | rabbit_types:error(term())).
-spec(read_recovery_terms(
        rabbit_misc:resource_name()) ->
             rabbit_types:ok_or_error2(term(), not_found)).

-endif. % use_specs

-include("rabbit.hrl").
-define(SERVER, ?MODULE).
-define(CLEAN_FILENAME, "clean.dot").

recover() ->
    case supervisor:start_child(rabbit_sup,
                                {?SERVER, {?MODULE, start_link, []},
                                 permanent, ?MAX_WAIT, worker,
                                 [?SERVER]}) of
        {ok, _}                       -> ok;
        {error, {already_started, _}} -> ok;
        {error, _}=Err                -> Err
    end.

start_link() ->
    gen_server:start_link(?MODULE, [], []).

store_recovery_terms(Name, Terms) ->
    dets:insert(?MODULE, {Name, Terms}).

check_clean_shutdown(Name) ->
    ok == remove_recovery_terms(Name).

read_recovery_terms(Name) ->
    case dets:lookup(?MODULE, Name) of
        [{_, Terms}] -> {ok, Terms};
        _            -> {error, not_found}
    end.

remove_recovery_terms(Name) ->
    case dets:member(?MODULE, Name) of
        true -> dets:delete(?MODULE, Name);
        _    -> {error, not_found}
    end.

flush() ->
    dets:sync(?MODULE),
    ok.

init(_) ->
    File = filename:join([rabbit_mnesia:dir(), "queues", ?CLEAN_FILENAME]),
    {ok, _} = dets:open_file(?MODULE, [{file, File},
                                       {ram_file, true},
                                       {auto_save, infinity}]),
    {ok, undefined}.

handle_call(Msg, _, State) ->
    {stop, {unexpected_call, Msg}, State}.

handle_cast(Msg, State) ->
    {stop, {unexpected_cast, Msg}, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok = dets:sync(?MODULE).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

