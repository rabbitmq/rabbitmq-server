%% This is the version of pg2 from R14B02, which contains the fix
%% described at
%% http://erlang.2086793.n4.nabble.com/pg2-still-busted-in-R13B04-td2230601.html.
%% The changes are a search-and-replace to rename the module and avoid
%% clashes with other versions of pg2, and also a simple rewrite of
%% "andalso" and "orelse" expressions to case statements where the second
%% operand is not a boolean since R12B does not allow this.

%%
%% %CopyrightBegin%
%%
%% Copyright Ericsson AB 1997-2010. All Rights Reserved.
%%
%% The contents of this file are subject to the Erlang Public License,
%% Version 1.1, (the "License"); you may not use this file except in
%% compliance with the License. You should have received a copy of the
%% Erlang Public License along with this software. If not, it can be
%% retrieved online at http://www.erlang.org/.
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and limitations
%% under the License.
%%
%% %CopyrightEnd%
%%
-module(pg2_fixed).

-export([create/1, delete/1, join/2, leave/2]).
-export([get_members/1, get_local_members/1]).
-export([get_closest_pid/1, which_groups/0]).
-export([start/0,start_link/0,init/1,handle_call/3,handle_cast/2,handle_info/2,
         terminate/2]).

%%% As of R13B03 monitors are used instead of links.

%%%
%%% Exported functions
%%%

-spec start_link() -> {'ok', pid()} | {'error', term()}.

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec start() -> {'ok', pid()} | {'error', term()}.

start() ->
    ensure_started().

-spec create(term()) -> 'ok'.

create(Name) ->
    ensure_started(),
    case ets:member(pg2_fixed_table, {group, Name}) of
        false ->
            global:trans({{?MODULE, Name}, self()},
                         fun() ->
                                 gen_server:multi_call(?MODULE, {create, Name})
                         end),
            ok;
        true ->
            ok
    end.

-type name() :: term().

-spec delete(name()) -> 'ok'.

delete(Name) ->
    ensure_started(),
    global:trans({{?MODULE, Name}, self()},
                 fun() ->
                         gen_server:multi_call(?MODULE, {delete, Name})
                 end),
    ok.

-spec join(name(), pid()) -> 'ok' | {'error', {'no_such_group', term()}}.

join(Name, Pid) when is_pid(Pid) ->
    ensure_started(),
    case ets:member(pg2_fixed_table, {group, Name}) of
        false ->
            {error, {no_such_group, Name}};
        true ->
            global:trans({{?MODULE, Name}, self()},
                         fun() ->
                                 gen_server:multi_call(?MODULE,
                                                       {join, Name, Pid})
                         end),
            ok
    end.

-spec leave(name(), pid()) -> 'ok' | {'error', {'no_such_group', name()}}.

leave(Name, Pid) when is_pid(Pid) ->
    ensure_started(),
    case ets:member(pg2_fixed_table, {group, Name}) of
        false ->
            {error, {no_such_group, Name}};
        true ->
            global:trans({{?MODULE, Name}, self()},
                         fun() ->
                                 gen_server:multi_call(?MODULE,
                                                       {leave, Name, Pid})
                         end),
            ok
    end.

-type get_members_ret() :: [pid()] | {'error', {'no_such_group', name()}}.

-spec get_members(name()) -> get_members_ret().
   
get_members(Name) ->
    ensure_started(),
    case ets:member(pg2_fixed_table, {group, Name}) of
        true ->
            group_members(Name);
        false ->
            {error, {no_such_group, Name}}
    end.

-spec get_local_members(name()) -> get_members_ret().

get_local_members(Name) ->
    ensure_started(),
    case ets:member(pg2_fixed_table, {group, Name}) of
        true ->
            local_group_members(Name);
        false ->
            {error, {no_such_group, Name}}
    end.

-spec which_groups() -> [name()].

which_groups() ->
    ensure_started(),
    all_groups().

-type gcp_error_reason() :: {'no_process', term()} | {'no_such_group', term()}.

-spec get_closest_pid(term()) -> pid() | {'error', gcp_error_reason()}.

get_closest_pid(Name) ->
    case get_local_members(Name) of
        [Pid] ->
            Pid;
        [] ->
            {_,_,X} = erlang:now(),
            case get_members(Name) of
                [] -> {error, {no_process, Name}};
                Members ->
                    lists:nth((X rem length(Members))+1, Members)
            end;
        Members when is_list(Members) ->
            {_,_,X} = erlang:now(),
            lists:nth((X rem length(Members))+1, Members);
        Else ->
            Else
    end.

%%%
%%% Callback functions from gen_server
%%%

-record(state, {}).

-spec init([]) -> {'ok', #state{}}.

init([]) ->
    Ns = nodes(),
    net_kernel:monitor_nodes(true),
    lists:foreach(fun(N) ->
                          {?MODULE, N} ! {new_pg2_fixed, node()},
                          self() ! {nodeup, N}
                  end, Ns),
    pg2_fixed_table = ets:new(pg2_fixed_table, [ordered_set, protected, named_table]),
    {ok, #state{}}.

-type call() :: {'create', name()}
              | {'delete', name()}
              | {'join', name(), pid()}
              | {'leave', name(), pid()}.

-spec handle_call(call(), _, #state{}) -> 
        {'reply', 'ok', #state{}}.

handle_call({create, Name}, _From, S) ->
    assure_group(Name),
    {reply, ok, S};
handle_call({join, Name, Pid}, _From, S) ->
    case ets:member(pg2_fixed_table, {group, Name}) of
        true -> join_group(Name, Pid);
        _    -> ok
    end,
    {reply, ok, S};
handle_call({leave, Name, Pid}, _From, S) ->
    case ets:member(pg2_fixed_table, {group, Name}) of
        true -> leave_group(Name, Pid);
        _    -> ok
    end,
    {reply, ok, S};
handle_call({delete, Name}, _From, S) ->
    delete_group(Name),
    {reply, ok, S};
handle_call(Request, From, S) ->
    error_logger:warning_msg("The pg2_fixed server received an unexpected message:\n"
                             "handle_call(~p, ~p, _)\n", 
                             [Request, From]),
    {noreply, S}.

-type all_members() :: [[name(),...]].
-type cast() :: {'exchange', node(), all_members()}
              | {'del_member', name(), pid()}.

-spec handle_cast(cast(), #state{}) -> {'noreply', #state{}}.

handle_cast({exchange, _Node, List}, S) ->
    store(List),
    {noreply, S};
handle_cast(_, S) ->
    %% Ignore {del_member, Name, Pid}.
    {noreply, S}.

-spec handle_info(tuple(), #state{}) -> {'noreply', #state{}}.

handle_info({'DOWN', MonitorRef, process, _Pid, _Info}, S) ->
    member_died(MonitorRef),
    {noreply, S};
handle_info({nodeup, Node}, S) ->
    gen_server:cast({?MODULE, Node}, {exchange, node(), all_members()}),
    {noreply, S};
handle_info({new_pg2_fixed, Node}, S) ->
    gen_server:cast({?MODULE, Node}, {exchange, node(), all_members()}),
    {noreply, S};
handle_info(_, S) ->
    {noreply, S}.

-spec terminate(term(), #state{}) -> 'ok'.

terminate(_Reason, _S) ->
    true = ets:delete(pg2_fixed_table),
    ok.

%%%
%%% Local functions
%%%

%%% One ETS table, pg2_fixed_table, is used for bookkeeping. The type of the
%%% table is ordered_set, and the fast matching of partially
%%% instantiated keys is used extensively.
%%%
%%% {{group, Name}}
%%%    Process group Name.
%%% {{ref, Pid}, RPid, MonitorRef, Counter}
%%% {{ref, MonitorRef}, Pid}
%%%    Each process has one monitor. Sometimes a process is spawned to
%%%    monitor the pid (RPid). Counter is incremented when the Pid joins
%%%    some group.
%%% {{member, Name, Pid}, GroupCounter}
%%% {{local_member, Name, Pid}}
%%%    Pid is a member of group Name, GroupCounter is incremented when the
%%%    Pid joins the group Name.
%%% {{pid, Pid, Name}}
%%%    Pid is a member of group Name.

store(List) ->
    _ = [case assure_group(Name) of
             true ->
                 [join_group(Name, P) || P <- Members -- group_members(Name)];
             _ ->
                 ok
         end || [Name, Members] <- List],
    ok.

assure_group(Name) ->
    Key = {group, Name},
    ets:member(pg2_fixed_table, Key) orelse true =:= ets:insert(pg2_fixed_table, {Key}).

delete_group(Name) ->
    _ = [leave_group(Name, Pid) || Pid <- group_members(Name)],
    true = ets:delete(pg2_fixed_table, {group, Name}),
    ok.

member_died(Ref) ->
    [{{ref, Ref}, Pid}] = ets:lookup(pg2_fixed_table, {ref, Ref}),
    Names = member_groups(Pid),
    _ = [leave_group(Name, P) || 
            Name <- Names,
            P <- member_in_group(Pid, Name)],
    %% Kept for backward compatibility with links. Can be removed, eventually.
    _ = [gen_server:abcast(nodes(), ?MODULE, {del_member, Name, Pid}) ||
            Name <- Names],
    ok.

join_group(Name, Pid) ->
    Ref_Pid = {ref, Pid}, 
    try _ = ets:update_counter(pg2_fixed_table, Ref_Pid, {4, +1})
    catch _:_ ->
            {RPid, Ref} = do_monitor(Pid),
            true = ets:insert(pg2_fixed_table, {Ref_Pid, RPid, Ref, 1}),
            true = ets:insert(pg2_fixed_table, {{ref, Ref}, Pid})
    end,
    Member_Name_Pid = {member, Name, Pid},
    try _ = ets:update_counter(pg2_fixed_table, Member_Name_Pid, {2, +1, 1, 1})
    catch _:_ ->
            true = ets:insert(pg2_fixed_table, {Member_Name_Pid, 1}),
            _ = [ets:insert(pg2_fixed_table, {{local_member, Name, Pid}}) ||
                    node(Pid) =:= node()],
            true = ets:insert(pg2_fixed_table, {{pid, Pid, Name}})
    end.

leave_group(Name, Pid) ->
    Member_Name_Pid = {member, Name, Pid},
    try ets:update_counter(pg2_fixed_table, Member_Name_Pid, {2, -1, 0, 0}) of
        N ->
            if 
                N =:= 0 ->
                    true = ets:delete(pg2_fixed_table, {pid, Pid, Name}),
                    _ = [ets:delete(pg2_fixed_table, {local_member, Name, Pid}) ||
                            node(Pid) =:= node()],
                    true = ets:delete(pg2_fixed_table, Member_Name_Pid);
                true ->
                    ok
            end,
            Ref_Pid = {ref, Pid}, 
            case ets:update_counter(pg2_fixed_table, Ref_Pid, {4, -1}) of
                0 ->
                    [{Ref_Pid,RPid,Ref,0}] = ets:lookup(pg2_fixed_table, Ref_Pid),
                    true = ets:delete(pg2_fixed_table, {ref, Ref}),
                    true = ets:delete(pg2_fixed_table, Ref_Pid),
                    true = erlang:demonitor(Ref, [flush]),
                    kill_monitor_proc(RPid, Pid);
                _ ->
                    ok
            end
    catch _:_ ->
            ok
    end.

all_members() ->
    [[G, group_members(G)] || G <- all_groups()].

group_members(Name) ->
    [P || 
        [P, N] <- ets:match(pg2_fixed_table, {{member, Name, '$1'},'$2'}),
        _ <- lists:seq(1, N)].

local_group_members(Name) ->
    [P || 
        [Pid] <- ets:match(pg2_fixed_table, {{local_member, Name, '$1'}}),
        P <- member_in_group(Pid, Name)].

member_in_group(Pid, Name) ->
    case ets:lookup(pg2_fixed_table, {member, Name, Pid}) of
        [] -> [];
        [{{member, Name, Pid}, N}] ->
            lists:duplicate(N, Pid)
    end.

member_groups(Pid) ->
    [Name || [Name] <- ets:match(pg2_fixed_table, {{pid, Pid, '$1'}})].

all_groups() ->
    [N || [N] <- ets:match(pg2_fixed_table, {{group,'$1'}})].

ensure_started() ->
    case whereis(?MODULE) of
        undefined ->
            C = {pg2_fixed, {?MODULE, start_link, []}, permanent,
                 1000, worker, [?MODULE]},
            supervisor:start_child(kernel_safe_sup, C);
        Pg2_FixedPid ->
            {ok, Pg2_FixedPid}
    end.


kill_monitor_proc(RPid, Pid) ->
    case RPid of
        Pid -> ok;
        _   -> exit(RPid, kill)
    end.

%% When/if erlang:monitor() returns before trying to connect to the
%% other node this function can be removed.
do_monitor(Pid) ->
    case (node(Pid) =:= node()) orelse lists:member(node(Pid), nodes()) of
        true ->
            %% Assume the node is still up
            {Pid, erlang:monitor(process, Pid)};
        false ->
            F = fun() -> 
                        Ref = erlang:monitor(process, Pid),
                        receive 
                            {'DOWN', Ref, process, Pid, _Info} ->
                                exit(normal)
                        end
                end,
            erlang:spawn_monitor(F)
    end.
