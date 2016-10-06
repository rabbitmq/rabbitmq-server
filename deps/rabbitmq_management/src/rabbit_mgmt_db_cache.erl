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
%%
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
-define(DEFAULT_TIMEOUT, 30000).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec fetch(atom(), fun(() -> any()), integer()) -> {ok, any()} | {error, error_desc()}.
fetch(Key, FetchFun, Timeout) ->
    ProcName = process_name(Key),
    case whereis(ProcName) of
        undefined -> {error, key_not_found};
        _ -> gen_server:call(ProcName, {fetch, FetchFun}, Timeout)
    end.

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

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init([]) ->
    {ok, #state{data = none,
                multiplier = ?DEFAULT_MULT}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------

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


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info(purge_cache, State) ->
    {noreply, State#state{data = none}};
handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
