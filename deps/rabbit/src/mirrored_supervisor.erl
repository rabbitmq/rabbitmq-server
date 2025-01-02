%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(mirrored_supervisor).

%% Mirrored Supervisor
%% ===================
%%
%% This module implements a new type of supervisor. It acts like a
%% normal supervisor, but at creation time you also provide the name
%% of a process group to join. All the supervisors within the
%% process group act like a single large distributed supervisor:
%%
%% * A process with a given child_id will only exist on one
%%   supervisor within the group.
%%
%% * If one supervisor fails, children may migrate to surviving
%%   supervisors within the group.
%%
%% In almost all cases you will want to use the module name for the
%% process group. Using multiple process groups with the same module
%% name is supported. Having multiple module names for the same
%% process group will lead to undefined behaviour.
%%
%% Motivation
%% ----------
%%
%% Sometimes you have processes which:
%%
%% * Only need to exist once per cluster.
%%
%% * Does not contain much state (or can reconstruct its state easily).
%%
%% * Needs to be restarted elsewhere should it be running on a node
%%   which fails.
%%
%% By creating a mirrored supervisor group with one supervisor on
%% each node, that's what you get.
%%
%%
%% API use
%% -------
%%
%% This is basically the same as for supervisor, except that:
%%
%% 1) start_link(Module, Args) becomes
%%    start_link(Group, Module, Args).
%%
%% 2) start_link({local, Name}, Module, Args) becomes
%%    start_link({local, Name}, Group, Module, Args).
%%
%% 3) start_link({global, Name}, Module, Args) is not available.
%%
%% 4) The restart strategy simple_one_for_one is not available.
%%
%% 5) Mnesia is used to hold global state. At some point your
%%    application should invoke create_tables() (or table_definitions()
%%    if it wants to manage table creation itself).
%%
%% Internals
%% ---------
%%
%% Each mirrored_supervisor consists of three processes - the overall
%% supervisor, the delegate supervisor and the mirroring server. The
%% overall supervisor supervises the other two processes. Its pid is
%% the one returned from start_link; the pids of the other two
%% processes are effectively hidden in the API.
%%
%% The delegate supervisor is in charge of supervising all the child
%% processes that are added to the supervisor as usual.
%%
%% The mirroring server intercepts calls to the supervisor API
%% (directed at the overall supervisor), does any special handling,
%% and forwards everything to the delegate supervisor.
%%
%% This module implements all three, hence init/1 is somewhat overloaded.
%%
%% The mirroring server creates and joins a process group on
%% startup. It monitors all the existing members of this group, and
%% broadcasts a "hello" message to them so that they can monitor it in
%% turn. When it receives a 'DOWN' message, it checks to see if it's
%% the "first" server in the group and restarts all the child
%% processes from the dead supervisor if so.
%%
%% In the future we might load balance this.
%%
%% Startup is slightly fiddly. The mirroring server needs to know the
%% Pid of the overall supervisor, but we don't have that until it has
%% started. Therefore we set this after the fact. We also start any
%% children we found in Module:init() at this point, since starting
%% children requires knowing the overall supervisor pid.

-define(SUPERVISOR, supervisor2).
-define(GEN_SERVER, gen_server2).
-define(SUP_MODULE, mirrored_supervisor_sups).

-export([start_link/3, start_link/4,
         start_child/2, restart_child/2,
         delete_child/2, terminate_child/2,
         which_children/1, count_children/1, check_childspecs/1]).

-behaviour(?GEN_SERVER).

-export([init/1, handle_call/3, handle_info/2, terminate/2, code_change/3,
         handle_cast/2]).

-export([start_internal/2]).
-export([create_tables/0, table_definitions/0]).
-export([supervisor/1, child/2]).

-record(state, {overall,
                delegate,
                group,
                tx_fun,
                initial_childspecs,
                child_order}).

%%--------------------------------------------------------------------------
%% Callback behaviour
%%--------------------------------------------------------------------------

-callback init(Args :: term()) ->
    {ok, {{RestartStrategy :: ?SUPERVISOR:strategy(),
           MaxR :: non_neg_integer(),
           MaxT :: non_neg_integer()},
           [ChildSpec :: ?SUPERVISOR:child_spec()]}}
    | ignore.

%%--------------------------------------------------------------------------
%% Specs
%%--------------------------------------------------------------------------

-type startlink_err() :: {'already_started', pid()} | 'shutdown' | term().
-type startlink_ret() :: {'ok', pid()} | 'ignore' | {'error', startlink_err()}.

-type group_name() :: module().
-type child_id() :: term(). %% supervisor:child_id() is not exported.

-export_type([group_name/0,
              child_id/0]).

-spec start_link(GroupName, Module, Args) -> startlink_ret() when
      GroupName :: group_name(),
      Module :: module(),
      Args :: term().

-spec start_link(SupName, GroupName, Module, Args) ->
                        startlink_ret() when
      SupName :: ?SUPERVISOR:sup_name(),
      GroupName :: group_name(),
      Module :: module(),
      Args :: term().

-spec start_internal(Group, ChildSpecs) -> Result when
      Group :: group_name(),
      ChildSpecs :: [?SUPERVISOR:child_spec()],
      Result :: {'ok', pid()} | {'error', term()}.

-spec create_tables() -> Result when
      Result :: 'ok'.

%%----------------------------------------------------------------------------

start_link(Group, Mod, Args) ->
    start_link0([], Group, init(Mod, Args)).

start_link({local, SupName}, Group, Mod, Args) ->
    start_link0([{local, SupName}], Group, init(Mod, Args));

start_link({global, _SupName}, _Group, _Mod, _Args) ->
    erlang:error(badarg).

start_link0(Prefix, Group, Init) ->
    case apply(?SUPERVISOR, start_link,
               Prefix ++ [?SUP_MODULE, {overall, Group, Init}]) of
        {ok, Pid} -> case catch call(Pid, {init, Pid}) of
                         ok -> {ok, Pid};
                         E  -> E
                     end;
        Other     -> Other
    end.

init(Mod, Args) ->
    _ = pg:start_link(),
    case Mod:init(Args) of
        {ok, {{Bad, _, _}, _ChildSpecs}} when
              Bad =:= simple_one_for_one -> erlang:error(badarg);
        Init                             -> Init
    end.

start_child(Sup, ChildSpec) -> call(Sup, {start_child,  ChildSpec}).
delete_child(Sup, Id)       -> find_call(Sup, Id, {delete_child, Id}).
restart_child(Sup, Id)      -> find_call(Sup, Id, {msg, restart_child, [Id]}).
terminate_child(Sup, Id)    -> find_call(Sup, Id, {msg, terminate_child, [Id]}).
which_children(Sup)         -> fold(which_children, Sup, fun lists:append/2).
count_children(Sup)         -> fold(count_children, Sup, fun add_proplists/2).
check_childspecs(Specs)     -> ?SUPERVISOR:check_childspecs(Specs).

call(Sup, Msg) -> ?GEN_SERVER:call(mirroring(Sup), Msg, infinity).
cast(Sup, Msg) -> with_exit_handler(
                    fun() -> ok end,
                    fun() -> ?GEN_SERVER:cast(mirroring(Sup), Msg) end).

find_call(Sup, Id, Msg) ->
    Group = call(Sup, group),
    case find_mirror(Group, Id) of
        {ok, Mirror} -> call(Mirror, Msg);
        Err -> Err
    end.

find_mirror(Group, Id) ->
    rabbit_db_msup:find_mirror(Group, Id).

fold(FunAtom, Sup, AggFun) ->
    Group = call(Sup, group),
    lists:foldl(AggFun, [],
                [apply(?SUPERVISOR, FunAtom, [D]) ||
                    M <- pg:get_members(Group),
                    D <- [delegate(M)]]).

child(Sup, Id) ->
    [Pid] = [Pid || {Id1, Pid, _, _} <- ?SUPERVISOR:which_children(Sup),
                    Id1 =:= Id],
    Pid.

delegate(Sup) -> child(Sup, delegate).
mirroring(Sup) -> child(Sup, mirroring).

%%----------------------------------------------------------------------------

start_internal(Group, ChildSpecs) ->
    ?GEN_SERVER:start_link(?MODULE, {Group, ChildSpecs},
                           [{timeout, infinity}]).

%%----------------------------------------------------------------------------

init({Group, ChildSpecs}) ->
    {ok, #state{group              = Group,
                initial_childspecs = ChildSpecs,
                child_order = child_order_from(ChildSpecs)}}.

handle_call({init, Overall}, _From,
            State = #state{overall            = undefined,
                           delegate           = undefined,
                           group              = Group,
                           initial_childspecs = ChildSpecs}) ->
    process_flag(trap_exit, true),
    LockId = mirrored_supervisor_locks:lock(Group),
    maybe_log_lock_acquisition_failure(LockId, Group),
    ok = pg:join(Group, Overall),
    rabbit_log:debug("Mirrored supervisor: initializing, overall supervisor ~tp joined group ~tp", [Overall, Group]),
    Rest = pg:get_members(Group) -- [Overall],
    Nodes = [node(M) || M <- Rest],
    rabbit_log:debug("Mirrored supervisor: known group ~tp members: ~tp on nodes ~tp", [Group, Rest, Nodes]),
    case Rest of
        [] ->
            rabbit_log:debug("Mirrored supervisor: no known peer members in group ~tp, will delete all child records for it", [Group]),
            delete_all(Group);
        _  -> ok
    end,
    [begin
         ?GEN_SERVER:cast(mirroring(Pid), {ensure_monitoring, Overall}),
         erlang:monitor(process, Pid)
     end || Pid <- Rest],
    Delegate = delegate(Overall),
    erlang:monitor(process, Delegate),
    State1 = State#state{overall = Overall, delegate = Delegate},
    Results = [maybe_start(Group, Overall, Delegate, S) || S <- ChildSpecs],
    mirrored_supervisor_locks:unlock(LockId),
    case errors(Results) of
        []     -> {reply, ok, State1};
        Errors -> {stop, {shutdown, Errors}, State1}
    end;

handle_call({start_child, ChildSpec}, _From,
            State = #state{overall  = Overall,
                           delegate = Delegate,
                           group    = Group}) ->
    LockId = mirrored_supervisor_locks:lock(Group),
    maybe_log_lock_acquisition_failure(LockId, Group),
    rabbit_log:debug("Mirrored supervisor: asked to consider starting a child, group: ~tp", [Group]),
    Result = case maybe_start(Group, Overall, Delegate, ChildSpec) of
                 already_in_store ->
                     rabbit_log:debug("Mirrored supervisor: maybe_start for group ~tp,"
                                      " overall ~p returned 'record already present'", [Group, Overall]),
                     {error, already_present};
                 {already_in_store, Pid} ->
                     rabbit_log:debug("Mirrored supervisor: maybe_start for group ~tp,"
                                      " overall ~p returned 'already running: ~tp'", [Group, Overall, Pid]),
                     {error, {already_started, Pid}};
                 Else ->
                     rabbit_log:debug("Mirrored supervisor: maybe_start for group ~tp,"
                                      " overall ~tp returned ~tp", [Group, Overall, Else]),
                     Else
             end,
    mirrored_supervisor_locks:unlock(LockId),
    {reply, Result, State};

handle_call({delete_child, Id}, _From, State = #state{delegate = Delegate,
                                                      group    = Group}) ->
    {reply, stop(Group, Delegate, Id), State};

handle_call({msg, F, A}, _From, State = #state{delegate = Delegate}) ->
    {reply, apply(?SUPERVISOR, F, [Delegate | A]), State};

handle_call(group, _From, State = #state{group = Group}) ->
    {reply, Group, State};

handle_call(Msg, _From, State) ->
    {stop, {unexpected_call, Msg}, State}.

handle_cast({ensure_monitoring, Pid}, State) ->
    erlang:monitor(process, Pid),
    {noreply, State};

handle_cast({die, Reason}, State = #state{group = Group}) ->
    _ = tell_all_peers_to_die(Group, Reason),
    {stop, Reason, State};

handle_cast(Msg, State) ->
    {stop, {unexpected_cast, Msg}, State}.

handle_info({'DOWN', _Ref, process, Pid, Reason},
            State = #state{delegate = Pid, group = Group}) ->
    %% Since the delegate is temporary, its death won't cause us to
    %% die. Since the overall supervisor kills processes in reverse
    %% order when shutting down "from above" and we started after the
    %% delegate, if we see the delegate die then that means it died
    %% "from below" i.e. due to the behaviour of its children, not
    %% because the whole app was being torn down.
    %%
    %% Therefore if we get here we know we need to cause the entire
    %% mirrored sup to shut down, not just fail over.
    _ = tell_all_peers_to_die(Group, Reason),
    {stop, Reason, State};

handle_info({'DOWN', _Ref, process, Pid, _Reason},
            State = #state{delegate = Delegate,
                           group    = Group,
                           overall  = O,
                           child_order = ChildOrder}) ->
    %% No guarantee pg will have received the DOWN before us.
    R = case lists:sort(pg:get_members(Group)) -- [Pid] of
            [O | _] -> ChildSpecs = retry_update_all(O, Pid),
                       [start(Delegate, ChildSpec)
                        || ChildSpec <- restore_child_order(ChildSpecs,
                                                            ChildOrder)];
            _       -> []
        end,
    case errors(R) of
        []     -> {noreply, State};
        Errors -> {stop, {shutdown, Errors}, State}
    end;

handle_info(Info, State) ->
    {stop, {unexpected_info, Info}, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%----------------------------------------------------------------------------

tell_all_peers_to_die(Group, Reason) ->
    [cast(P, {die, Reason}) || P <- pg:get_members(Group) -- [self()]].

maybe_start(Group, Overall, Delegate, ChildSpec) ->
    rabbit_log:debug("Mirrored supervisor: asked to consider starting, group: ~tp",
                     [Group]),
    try check_start(Group, Overall, Delegate, ChildSpec) of
        start      ->
            rabbit_log:debug("Mirrored supervisor: check_start for group ~tp,"
                             " overall ~tp returned 'do start'", [Group, Overall]),
            start(Delegate, ChildSpec);
        undefined  ->
            rabbit_log:debug("Mirrored supervisor: check_start for group ~tp,"
                             " overall ~tp returned 'undefined'", [Group, Overall]),
            already_in_store;
        Pid ->
            rabbit_log:debug("Mirrored supervisor: check_start for group ~tp,"
                             " overall ~tp returned 'already running (~tp)'",
                             [Group, Overall, Pid]),
            {already_in_store, Pid}
    catch
        %% If we are torn down while in the transaction...
        {error, E} -> {error, E}
    end.

check_start(Group, Overall, Delegate, ChildSpec) ->
    Id = id(ChildSpec),
    rabbit_log:debug("Mirrored supervisor: check_start for group ~tp, id: ~tp, "
                     "overall: ~tp", [Group, Id, Overall]),
    case rabbit_db_msup:create_or_update(Group, Overall, Delegate, ChildSpec, Id) of
        Delegate0 when is_pid(Delegate0) ->
            child(Delegate0, Id);
        Other ->
            Other
    end.

supervisor(Pid) -> with_exit_handler(fun() -> dead end,
                                     fun() -> delegate(Pid) end).

start(Delegate, ChildSpec) ->
    apply(?SUPERVISOR, start_child, [Delegate, ChildSpec]).

stop(Group, Delegate, Id) ->
    try check_stop(Group, Delegate, Id) of
        deleted    -> apply(?SUPERVISOR, delete_child, [Delegate, Id]);
        running    -> {error, running}
    catch
        {error, E} -> {error, E}
    end.

check_stop(Group, Delegate, Id) ->
    case child(Delegate, Id) of
        undefined ->
            rabbit_db_msup:delete(Group, Id),
            deleted;
        _ ->
            running
    end.

id({Id, _, _, _, _, _}) -> Id.

retry_update_all(O, Pid) ->
    retry_update_all(O, Pid, 10000).

retry_update_all(O, Pid, TimeLeft) when TimeLeft > 0 ->
    case update_all(O, Pid) of
        List when is_list(List) ->
            List;
        {error, timeout} ->
            Sleep = 200,
            TimeLeft1 = TimeLeft - Sleep,
            timer:sleep(Sleep),
            retry_update_all(O, Pid, TimeLeft1)
    end;
retry_update_all(O, Pid, _TimeLeft) ->
    update_all(O, Pid).

update_all(Overall, OldOverall) ->
    rabbit_db_msup:update_all(Overall, OldOverall).

delete_all(Group) ->
    rabbit_db_msup:delete_all(Group).

errors(Results) -> [E || {error, E} <- Results].

%%----------------------------------------------------------------------------

create_tables() ->
    rabbit_db_msup:create_tables().

table_definitions() ->
    rabbit_db_msup:table_definitions().

%%----------------------------------------------------------------------------

with_exit_handler(Handler, Thunk) ->
    try
        Thunk()
    catch
        exit:{R, _} when R =:= noproc; R =:= nodedown;
                         R =:= normal; R =:= shutdown ->
            Handler();
        exit:{{R, _}, _} when R =:= nodedown; R =:= shutdown ->
            Handler()
    end.

add_proplists(P1, P2) ->
    add_proplists(lists:keysort(1, P1), lists:keysort(1, P2), []).
add_proplists([], P2, Acc) -> P2 ++ Acc;
add_proplists(P1, [], Acc) -> P1 ++ Acc;
add_proplists([{K, V1} | P1], [{K, V2} | P2], Acc) ->
    add_proplists(P1, P2, [{K, V1 + V2} | Acc]);
add_proplists([{K1, _} = KV | P1], [{K2, _} | _] = P2, Acc) when K1 < K2 ->
    add_proplists(P1, P2, [KV | Acc]);
add_proplists(P1, [KV | P2], Acc) ->
    add_proplists(P1, P2, [KV | Acc]).

child_order_from(ChildSpecs) ->
    lists:zipwith(fun(C, N) ->
                          {id(C), N}
                  end, ChildSpecs, lists:seq(1, length(ChildSpecs))).

restore_child_order(ChildSpecs, ChildOrder) ->
    lists:sort(fun(A, B) ->
                       proplists:get_value(id(A), ChildOrder)
                           < proplists:get_value(id(B), ChildOrder)
               end, ChildSpecs).

maybe_log_lock_acquisition_failure(undefined = _LockId, Group) ->
    rabbit_log:warning("Mirrored supervisor: could not acquire lock for group ~ts", [Group]);
maybe_log_lock_acquisition_failure(_, _) ->
    ok.
