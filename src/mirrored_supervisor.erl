%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2011 VMware, Inc.  All rights reserved.
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
-define(PG2,        pg2_fixed).

-define(TABLE, mirrored_sup_childspec).
-define(TABLE_DEF,
        {?TABLE,
         [{record_name, mirrored_sup_childspec},
          {type, ordered_set},
          {attributes, record_info(fields, mirrored_sup_childspec)}]}).
-define(TABLE_MATCH, {match, #mirrored_sup_childspec{ _ = '_' }}).

-export([start_link/3, start_link/4,
         start_child/2, restart_child/2,
         delete_child/2, terminate_child/2,
         which_children/1, count_children/1, check_childspecs/1]).

-export([behaviour_info/1]).

-behaviour(?GEN_SERVER).
-behaviour(?SUPERVISOR).

-export([init/1, handle_call/3, handle_info/2, terminate/2, code_change/3,
         handle_cast/2]).

-export([start_internal/2]).
-export([create_tables/0, table_definitions/0]).

-record(mirrored_sup_childspec, {key, mirroring_pid, childspec}).

-record(state, {overall,
                delegate,
                group,
                initial_childspecs}).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-type child()    :: pid() | 'undefined'.
-type child_id() :: term().
-type mfargs()   :: {M :: module(), F :: atom(), A :: [term()] | 'undefined'}.
-type modules()  :: [module()] | 'dynamic'.
-type restart()  :: 'permanent' | 'transient' | 'temporary'.
-type shutdown() :: 'brutal_kill' | timeout().
-type worker()   :: 'worker' | 'supervisor'.
-type sup_name() :: {'local', Name :: atom()} | {'global', Name :: atom()}.
-type sup_ref()  :: (Name :: atom())
                  | {Name :: atom(), Node :: node()}
                  | {'global', Name :: atom()}
                  | pid().
-type child_spec() :: {Id :: child_id(),
                       StartFunc :: mfargs(),
                       Restart :: restart(),
                       Shutdown :: shutdown(),
                       Type :: worker(),
                       Modules :: modules()}.

-type startlink_err() :: {'already_started', pid()} | 'shutdown' | term().
-type startlink_ret() :: {'ok', pid()} | 'ignore' | {'error', startlink_err()}.

-type startchild_err() :: 'already_present'
                        | {'already_started', Child :: child()} | term().
-type startchild_ret() :: {'ok', Child :: child()}
                        | {'ok', Child :: child(), Info :: term()}
                        | {'error', startchild_err()}.

-type group_name() :: any().

-spec start_link(GroupName, Module, Args) -> startlink_ret() when
      GroupName :: group_name(),
      Module :: module(),
      Args :: term().

-spec start_link(SupName, GroupName, Module, Args) -> startlink_ret() when
      SupName :: sup_name(),
      GroupName :: group_name(),
      Module :: module(),
      Args :: term().

-spec start_child(SupRef, ChildSpec) -> startchild_ret() when
      SupRef :: sup_ref(),
      ChildSpec :: child_spec() | (List :: [term()]).

-spec restart_child(SupRef, Id) -> Result when
      SupRef :: sup_ref(),
      Id :: child_id(),
      Result :: {'ok', Child :: child()}
              | {'ok', Child :: child(), Info :: term()}
              | {'error', Error},
      Error :: 'running' | 'not_found' | 'simple_one_for_one' | term().

-spec delete_child(SupRef, Id) -> Result when
      SupRef :: sup_ref(),
      Id :: child_id(),
      Result :: 'ok' | {'error', Error},
      Error :: 'running' | 'not_found' | 'simple_one_for_one'.

-spec terminate_child(SupRef, Id) -> Result when
      SupRef :: sup_ref(),
      Id :: pid() | child_id(),
      Result :: 'ok' | {'error', Error},
      Error :: 'not_found' | 'simple_one_for_one'.

-spec which_children(SupRef) -> [{Id,Child,Type,Modules}] when
      SupRef :: sup_ref(),
      Id :: child_id() | 'undefined',
      Child :: child(),
      Type :: worker(),
      Modules :: modules().

-spec check_childspecs(ChildSpecs) -> Result when
      ChildSpecs :: [child_spec()],
      Result :: 'ok' | {'error', Error :: term()}.

-spec start_internal(Group, ChildSpecs) -> Result when
      Group :: group_name(),
      ChildSpecs :: [child_spec()],
      Result :: startlink_ret().

-spec create_tables() -> Result when
      Result :: 'ok'.

-endif.

%%----------------------------------------------------------------------------

start_link(Group, Mod, Args) ->
    start_link0([], Group, init(Mod, Args)).

start_link({local, SupName}, Group, Mod, Args) ->
    start_link0([{local, SupName}], Group, init(Mod, Args));

start_link({global, _SupName}, _Group, _Mod, _Args) ->
    error(badarg).

start_link0(Prefix, Group, Init) ->
    case apply(?SUPERVISOR, start_link,
               Prefix ++ [?MODULE, {overall, Group, Init}]) of
        {ok, Pid} -> call(Pid, {init, Pid}),
                     {ok, Pid};
        Other     -> Other
    end.

init(Mod, Args) ->
    case Mod:init(Args) of
        {ok, {{Bad, _, _}, _ChildSpecs}} when
              Bad =:= simple_one_for_one orelse
              Bad =:= simple_one_for_one_terminate -> error(badarg);
        Init                                       -> Init
    end.

start_child(Sup, ChildSpec) -> call(Sup, {start_child,  ChildSpec}).
delete_child(Sup, Id)       -> find_call(Sup, Id, {delete_child, Id}).
restart_child(Sup, Id)      -> find_call(Sup, Id, {msg, restart_child, [Id]}).
terminate_child(Sup, Id)    -> find_call(Sup, Id, {msg, terminate_child, [Id]}).
which_children(Sup)         -> fold(which_children, Sup, fun lists:append/2).
count_children(Sup)         -> fold(count_children, Sup, fun add_proplists/2).
check_childspecs(Specs)     -> ?SUPERVISOR:check_childspecs(Specs).

behaviour_info(callbacks) -> [{init,1}];
behaviour_info(_Other)    -> undefined.

call(Sup, Msg) ->
    ?GEN_SERVER:call(child(Sup, mirroring), Msg, infinity).

find_call(Sup, Id, Msg) ->
    Group = call(Sup, group),
    MatchHead = #mirrored_sup_childspec{mirroring_pid = '$1',
                                        key           = {Group, Id},
                                        _             = '_'},
    %% If we did this inside a tx we could still have failover
    %% immediately after the tx - we can't be 100% here. So we may as
    %% well dirty_select.
    case mnesia:dirty_select(?TABLE, [{MatchHead, [], ['$1']}]) of
        [Mirror] -> ?GEN_SERVER:call(Mirror, Msg, infinity);
        []       -> {error, not_found}
    end.

fold(FunAtom, Sup, AggFun) ->
    Group = call(Sup, group),
    lists:foldl(AggFun, [],
                [apply(?SUPERVISOR, FunAtom, [D]) ||
                    M <- ?PG2:get_members(Group),
                    D <- [?GEN_SERVER:call(M, delegate_supervisor, infinity)]]).

child(Sup, Id) ->
    [Pid] = [Pid || {Id1, Pid, _, _} <- ?SUPERVISOR:which_children(Sup),
                    Id1 =:= Id],
    Pid.

%%----------------------------------------------------------------------------

start_internal(Group, ChildSpecs) ->
    ?GEN_SERVER:start_link(?MODULE, {mirroring, Group, ChildSpecs},
                           [{timeout, infinity}]).

%%----------------------------------------------------------------------------

init({overall, Group, Init}) ->
    case Init of
        {ok, {Restart, ChildSpecs}} ->
            Delegate = {delegate, {?SUPERVISOR, start_link,
                                   [?MODULE, {delegate, Restart}]},
                        temporary, 16#ffffffff, supervisor, [?SUPERVISOR]},
            Mirroring = {mirroring, {?MODULE, start_internal,
                                     [Group, ChildSpecs]},
                         permanent, 16#ffffffff, worker, [?MODULE]},
            %% Important: Delegate MUST start after Mirroring, see comment in
            %% handle_info('DOWN', ...) below
            {ok, {{one_for_all, 0, 1}, [Mirroring, Delegate]}};
        ignore ->
            ignore
    end;

init({delegate, Restart}) ->
    {ok, {Restart, []}};

init({mirroring, Group, ChildSpecs}) ->
    {ok, #state{group = Group, initial_childspecs = ChildSpecs}}.

handle_call({init, Overall}, _From,
            State = #state{overall            = undefined,
                           delegate           = undefined,
                           group              = Group,
                           initial_childspecs = ChildSpecs}) ->
    process_flag(trap_exit, true),
    ?PG2:create(Group),
    ok = ?PG2:join(Group, self()),
    Rest = ?PG2:get_members(Group) -- [self()],
    case Rest of
        [] -> {atomic, _} = mnesia:transaction(fun() -> delete_all(Group) end);
        _  -> ok
    end,
    [begin
         ?GEN_SERVER:cast(Pid, {ensure_monitoring, self()}),
         erlang:monitor(process, Pid)
     end || Pid <- Rest],
    Delegate = child(Overall, delegate),
    erlang:monitor(process, Delegate),
    [maybe_start(Group, Delegate, S) || S <- ChildSpecs],
    {reply, ok, State#state{overall = Overall, delegate = Delegate}};

handle_call({start_child, ChildSpec}, _From,
            State = #state{delegate = Delegate,
                           group    = Group}) ->
    {reply, maybe_start(Group, Delegate, ChildSpec), State};

handle_call({delete_child, Id}, _From, State = #state{delegate = Delegate,
                                                      group    = Group}) ->
    {reply, stop(Group, Delegate, Id), State};

handle_call({msg, F, A}, _From, State = #state{delegate = Delegate}) ->
    {reply, apply(?SUPERVISOR, F, [Delegate | A]), State};

handle_call(delegate_supervisor, _From, State = #state{delegate = Delegate}) ->
    {reply, Delegate, State};

handle_call(group, _From, State = #state{group = Group}) ->
    {reply, Group, State};

handle_call(Msg, _From, State) ->
    {stop, {unexpected_call, Msg}, State}.

handle_cast({ensure_monitoring, Pid}, State) ->
    erlang:monitor(process, Pid),
    {noreply, State};

handle_cast({die, Reason}, State = #state{group = Group}) ->
    tell_all_peers_to_die(Group, Reason),
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
    tell_all_peers_to_die(Group, Reason),
    {stop, Reason, State};

handle_info({'DOWN', _Ref, process, Pid, _Reason},
            State = #state{delegate = Delegate, group = Group}) ->
    %% TODO load balance this
    %% No guarantee pg2 will have received the DOWN before us.
    Self = self(),
    case lists:sort(?PG2:get_members(Group)) -- [Pid] of
        [Self | _] -> {atomic, ChildSpecs} =
                          mnesia:transaction(fun() -> update_all(Pid) end),
                      [start(Delegate, ChildSpec) || ChildSpec <- ChildSpecs];
        _          -> ok
    end,
    {noreply, State};

handle_info(Info, State) ->
    {stop, {unexpected_info, Info}, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%----------------------------------------------------------------------------

tell_all_peers_to_die(Group, Reason) ->
    [?GEN_SERVER:cast(P, {die, Reason}) ||
        P <- ?PG2:get_members(Group) -- [self()]].

maybe_start(Group, Delegate, ChildSpec) ->
    case mnesia:transaction(fun() ->
                                    check_start(Group, Delegate, ChildSpec)
                            end) of
        {atomic, start} -> start(Delegate, ChildSpec);
        {atomic, Pid}   -> {error, {already_started, Pid}};
        %% If we are torn down while in the transaction...
        {aborted, E}    -> {error, E}
    end.

check_start(Group, Delegate, ChildSpec) ->
    case mnesia:wread({?TABLE, {Group, id(ChildSpec)}}) of
        []  -> write(Group, ChildSpec),
               start;
        [S] -> #mirrored_sup_childspec{key           = {Group, Id},
                                       mirroring_pid = Pid} = S,
               case self() of
                   Pid -> child(Delegate, Id);
                   _   -> case supervisor(Pid) of
                              dead      -> write(Group, ChildSpec),
                                           start;
                              Delegate0 -> child(Delegate0, Id)
                          end
               end
    end.

supervisor(Pid) ->
    with_exit_handler(
      fun() -> dead end,
      fun() -> gen_server:call(Pid, delegate_supervisor, infinity) end).

write(Group, ChildSpec) ->
    ok = mnesia:write(
           #mirrored_sup_childspec{key           = {Group, id(ChildSpec)},
                                   mirroring_pid = self(),
                                   childspec     = ChildSpec}),
    ChildSpec.

delete(Group, Id) ->
    ok = mnesia:delete({?TABLE, {Group, Id}}).

start(Delegate, ChildSpec) ->
    apply(?SUPERVISOR, start_child, [Delegate, ChildSpec]).

stop(Group, Delegate, Id) ->
    case mnesia:transaction(fun() -> check_stop(Group, Delegate, Id) end) of
        {atomic, deleted} -> apply(?SUPERVISOR, delete_child, [Delegate, Id]);
        {atomic, running} -> {error, running};
        {aborted, E}      -> {error, E}
    end.

check_stop(Group, Delegate, Id) ->
    case child(Delegate, Id) of
        undefined -> delete(Group, Id),
                     deleted;
        _         -> running
    end.

id({Id, _, _, _, _, _}) -> Id.

update_all(OldPid) ->
    MatchHead = #mirrored_sup_childspec{mirroring_pid = OldPid,
                                        key           = '$1',
                                        childspec     = '$2',
                                        _             = '_'},
    [write(Group, C) ||
        [{Group, _Id}, C] <- mnesia:select(?TABLE, [{MatchHead, [], ['$$']}])].

delete_all(Group) ->
    MatchHead = #mirrored_sup_childspec{key       = {Group, '_'},
                                        childspec = '$1',
                                        _         = '_'},
    [delete(Group, id(C)) ||
        C <- mnesia:select(?TABLE, [{MatchHead, [], ['$1']}])].

%%----------------------------------------------------------------------------

create_tables() ->
    create_tables([?TABLE_DEF]).

create_tables([]) ->
    ok;
create_tables([{Table, Attributes} | Ts]) ->
    case mnesia:create_table(Table, Attributes) of
        {atomic, ok}                        -> create_tables(Ts);
        {aborted, {already_exists, ?TABLE}} -> create_tables(Ts);
        Err                                 -> Err
    end.

table_definitions() ->
    {Name, Attributes} = ?TABLE_DEF,
    [{Name, [?TABLE_MATCH | Attributes]}].

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

add_proplists(P1, []) -> P1;
add_proplists(P1, [{K, V2} | P2]) ->
    add_proplists(case proplists:get_value(K, P1) of
                      undefined -> [{K, V2} | P1];
                      V1        -> [{K, V1 + V2} | proplists:delete(K, P1)]
                  end, P2).
