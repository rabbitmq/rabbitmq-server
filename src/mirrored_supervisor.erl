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
%% Copyright (c) 2007-2011 VMware, Inc.  All rights reserved.
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
%% 4) Mnesia is used to hold global state. At some point your
%%    application should invoke create_tables() (or table_definitions()
%%    if it wants to manage table creation itself).
%%
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
          {attributes, record_info(fields, mirrored_sup_childspec)}]}).
-define(TABLE_MATCH, {match, #mirrored_sup_childspec{ _ = '_' }}).

-export([start_link/3, start_link/4,
         start_child/2, restart_child/2,
         delete_child/2, terminate_child/2,
         which_children/1, check_childspecs/1]).

-export([behaviour_info/1]).

-behaviour(?GEN_SERVER).
-behaviour(?SUPERVISOR).

-export([init/1, handle_call/3, handle_info/2, terminate/2, code_change/3,
         handle_cast/2]).

-export([start_internal/2]).
-export([create_tables/0, table_definitions/0]).

-record(mirrored_sup_childspec, {id, mirroring_pid, childspec}).

-record(state, {overall, group, initial_childspecs}).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-type child()    :: pid() | 'undefined'.
-type child_id() :: term().
-type mfargs()   :: {M :: module(), F :: atom(), A :: [term()] | undefined}.
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

-type strategy() :: 'one_for_all' | 'one_for_one'
                  | 'rest_for_one' | 'simple_one_for_one'.

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
      Id :: child_id() | undefined,
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
    start_link0([], Group, Mod, Args).

start_link({local, SupName}, Group, Mod, Args) ->
    start_link0([{local, SupName}], Group, Mod, Args);

start_link({global, _SupName}, _Group, _Mod, _Args) ->
    exit(mirrored_supervisors_must_not_be_globally_named).

start_link0(Prefix, Group, Mod, Args) ->
    case apply(?SUPERVISOR, start_link,
               Prefix ++ [?MODULE, {overall, Group, Mod, Args}]) of
        {ok, Pid} -> call(Pid, {finish_startup, Pid}),
                     {ok, Pid};
        Other     -> Other
    end.

start_child(Sup, ChildSpec)  -> call(Sup, {start_child,  ChildSpec}).
delete_child(Sup, Name)      -> call(Sup, {delete_child, Name}).
restart_child(Sup, Name)     -> call(Sup, {msg, restart_child,   [Name]}).
terminate_child(Sup, Name)   -> call(Sup, {msg, terminate_child, [Name]}).
which_children(Sup)          -> ?SUPERVISOR:which_children(Sup).
check_childspecs(ChildSpecs) -> ?SUPERVISOR:check_childspecs(ChildSpecs).

behaviour_info(callbacks) -> [{init,1}];
behaviour_info(_Other)    -> undefined.

call(Sup, Msg) ->
    ?GEN_SERVER:call(child(Sup, mirroring), Msg, infinity).

child(Sup, Name) ->
    [Pid] = [Pid || {Name1, Pid, _, _} <- which_children(Sup), Name1 =:= Name],
    Pid.

%%----------------------------------------------------------------------------

start_internal(Group, ChildSpecs) ->
    ?GEN_SERVER:start_link(?MODULE, {mirroring, Group, ChildSpecs},
                           [{timeout, infinity}]).

%%----------------------------------------------------------------------------

init({overall, Group, Mod, Args}) ->
    {ok, {Restart, ChildSpecs}} = Mod:init(Args),
    Delegate = {delegate, {?SUPERVISOR, start_link,
                           [?MODULE, {delegate, Restart}]},
                transient, 16#ffffffff, supervisor, [?SUPERVISOR]},
    Mirroring = {mirroring, {?MODULE, start_internal, [Group, ChildSpecs]},
                 transient, 16#ffffffff, worker, [?MODULE]},
    {ok, {{one_for_all, 0, 1}, [Delegate, Mirroring]}};

init({delegate, Restart}) ->
    {ok, {Restart, []}};

init({mirroring, Group, ChildSpecs}) ->
    ?PG2:create(Group),
    [begin
         gen_server2:call(Pid, {hello, self()}, infinity),
         erlang:monitor(process, Pid)
     end
     || Pid <- ?PG2:get_members(Group)],
    ok = ?PG2:join(Group, self()),
    {ok, #state{group = Group, initial_childspecs = ChildSpecs}}.

handle_call({finish_startup, Overall}, _From,
            State = #state{overall            = undefined,
                           initial_childspecs = ChildSpecs}) ->
    [maybe_start(Overall, S) || S <- ChildSpecs],
    {reply, ok, State#state{overall = Overall}};

handle_call({start_child, ChildSpec}, _From,
            State = #state{overall = Overall}) ->
    {reply, maybe_start(Overall, ChildSpec), State};

handle_call({delete_child, Id}, _From,
            State = #state{overall = Overall}) ->
    {atomic, ok} = mnesia:transaction(fun() -> delete(Id) end),
    {reply, stop(Overall, Id), State};

handle_call({msg, F, A}, _From, State = #state{overall = Overall}) ->
    {reply, apply(?SUPERVISOR, F, [child(Overall, delegate) | A]), State};

handle_call({hello, Pid}, _From, State) ->
    erlang:monitor(process, Pid),
    {reply, ok, State};

handle_call(overall_supervisor, _From, State = #state{overall = Overall}) ->
    {reply, Overall, State};

handle_call(Msg, _From, State) ->
    {stop, {unexpected_call, Msg}, State}.

handle_cast(Msg, State) ->
    {stop, {unexpected_cast, Msg}, State}.

handle_info({'DOWN', _Ref, process, Pid, _Reason},
            State = #state{overall = Overall, group = Group}) ->
    %% TODO load balance this
    %% We remove the dead pid here because pg2 is slightly racy,
    %% most of the time it will be gone before we get here but not
    %% always.
    Self = self(),
    case lists:sort(?PG2:get_members(Group)) -- [Pid] of
        [Self | _] -> {atomic, ChildSpecs} =
                          mnesia:transaction(fun() -> update_all(Pid) end),
                      [start(Overall, ChildSpec) || ChildSpec <- ChildSpecs];
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

maybe_start(Overall, ChildSpec) ->
    case mnesia:transaction(fun() -> check_start(ChildSpec) end) of
        {atomic, start} -> start(Overall, ChildSpec);
        {atomic, Pid}   -> {ok, Pid}
    end.

check_start(ChildSpec) ->
    case mnesia:wread({?TABLE, id(ChildSpec)}) of
        []  -> write(ChildSpec),
               start;
        [S] -> #mirrored_sup_childspec{id            = Id,
                                       mirroring_pid = Pid} = S,
               case supervisor(Pid) of
                   dead -> delete(ChildSpec),
                           write(ChildSpec),
                           start;
                   Sup  -> child(child(Sup, delegate), Id)
               end
    end.

supervisor(Pid) ->
    try
        gen_server:call(Pid, overall_supervisor, infinity)
    catch
        exit:{noproc, _} -> dead
    end.

write(ChildSpec) ->
    ok = mnesia:write(#mirrored_sup_childspec{id              = id(ChildSpec),
                                              mirroring_pid   = self(),
                                              childspec       = ChildSpec}).

delete(Id) ->
    ok = mnesia:delete({?TABLE, Id}).

start(Overall, ChildSpec) ->
    apply(?SUPERVISOR, start_child, [child(Overall, delegate), ChildSpec]).

stop(Overall, Id) ->
    apply(?SUPERVISOR, delete_child, [child(Overall, delegate), Id]).

id({Id, _, _, _, _, _}) -> Id.

update(ChildSpec) ->
    delete(ChildSpec),
    write(ChildSpec),
    ChildSpec.

update_all(OldPid) ->
    MatchHead = #mirrored_sup_childspec{mirroring_pid   = OldPid,
                                        childspec       = '$1',
                                        _               = '_'},
    [update(C) || C <- mnesia:select(?TABLE, [{MatchHead, [], ['$1']}])].

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
