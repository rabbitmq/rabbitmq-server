%% the contents of this file are subject to the mozilla public license
%% version 1.1 (the "license"); you may not use this file except in
%% compliance with the license. you may obtain a copy of the license at
%% http://www.mozilla.org/mpl/
%%
%% software distributed under the license is distributed on an "as is"
%% basis, without warranty of any kind, either express or implied. see the
%% license for the specific language governing rights and limitations
%% under the license.
%%
%% copyright (c) 2016 pivotal software, inc.  all rights reserved.

-module(rabbit_mgmt_db_cache).

-behaviour(gen_server).

%% API functions
-export([start_link/1]).
-export([process_name/1,
         fetch/2,
         fetch/3]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {data :: any() | none,
                multiplier :: integer()}).

-type error_desc() :: key_not_found | timeout | {throw, atom()}.

-define(DEFAULT_MULT, 5).
-define(DEFAULT_TIMEOUT, 60000).
-define(CHILD(Key), {rabbit_mgmt_db_cache:process_name(Key),
                     {rabbit_mgmt_db_cache, start_link, [Key]},
                                     permanent, 5000, worker,
                                     [rabbit_mgmt_db_cache]}).

%% Implements an adaptive cache that times the value generating fun
%% and uses the return value as the cached value for the time it took
%% to produce * some factor (defaults to 5).
%% There is one cache process per key. New processes are started as
%% required.


%%%===================================================================
%%% API functions
%%%===================================================================

-spec fetch(atom(), fun(() -> any()), integer()) -> {ok, any()} | {error, error_desc()}.
fetch(Key, FetchFun, Timeout) ->
    ProcName = process_name(Key),
    Pid = case whereis(ProcName) of
            undefined ->
                {ok, P} = supervisor:start_child(rabbit_mgmt_db_cache_sup, ?CHILD(Key)),
                P;
            P -> P
          end,
    gen_server:call(Pid, {fetch, FetchFun}, Timeout).

-spec fetch(atom(), fun(() -> any())) -> {ok, any()} | {error, error_desc()}.
fetch(Key, FetchFun) ->
    fetch(Key, FetchFun, ?DEFAULT_TIMEOUT).

-spec process_name(atom()) -> atom().
process_name(Key) ->
    list_to_atom(atom_to_list(?MODULE) ++ "_" ++ atom_to_list(Key)).

-spec start_link(atom()) -> {ok, pid()} | ignore | {error, any()}.
start_link(Key) ->
    gen_server:start_link({local, process_name(Key)}, ?MODULE, [], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    Mult = application:get_env(rabbitmg_management,
                               db_cache_multiplier,
                               ?DEFAULT_MULT),
    {ok, #state{data = none,
                multiplier = Mult}}.

handle_call({fetch, FetchFun}, _From, #state{data = none,
                                             multiplier = Mult} = State) ->
    try timer:tc(FetchFun) of
        {Time, Data} ->
            case trunc(Time / 1000 * Mult) of
                0 -> {reply, {ok, Data}, State}; % no need to cache that
                T ->
                    {ok, _} = timer:send_after(T, self(), purge_cache),
                    {reply, {ok, Data}, State#state{data = Data}}
            end
    catch
        Throw -> {reply, {error, {throw, Throw}}, State}
    end;
handle_call({fetch, _FetchFun}, _From, #state{data = Data} = State) ->
    Reply = {ok, Data},
    {reply, Reply, State};
handle_call(purge_cache, _From, State) ->
    {reply, ok, State#state{data = none}}.


handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(purge_cache, State) ->
    {noreply, State#state{data = none}};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
