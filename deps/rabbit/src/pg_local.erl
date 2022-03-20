%% This file is a copy of pg2.erl from the R13B-3 Erlang/OTP
%% distribution, with the following modifications:
%%
%% 1) Process groups are node-local only.
%%
%% 2) Groups are created/deleted implicitly.
%%
%% 3) 'join' and 'leave' are asynchronous.
%%
%% 4) the type specs of the exported non-callback functions have been
%%    extracted into a separate, guarded section, and rewritten in
%%    old-style spec syntax, for better compatibility with older
%%    versions of Erlang/OTP. The remaining type specs have been
%%    removed.

%% All modifications are (C) 2010-2022 VMware, Inc. or its affiliates.

%% %CopyrightBegin%
%%
%% Copyright Ericsson AB 1997-2009. All Rights Reserved.
%%
%% The contents of this file are subject to the Erlang Public License,
%% Version 1.1, (the "License"); you may not use this file except in
%% compliance with the License. You should have received a copy of the
%% Erlang Public License along with this software. If not, it can be
%% retrieved online at https://www.erlang.org/.
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and limitations
%% under the License.
%%
%% %CopyrightEnd%
%%
-module(pg_local).

-export([join/2, leave/2, get_members/1, in_group/2]).
%% intended for testing only; not part of official API
-export([sync/0, clear/0]).
-export([start/0, start_link/0, init/1, handle_call/3, handle_cast/2,
         handle_info/2, terminate/2]).

%%----------------------------------------------------------------------------

-type name() :: term().

%%----------------------------------------------------------------------------

-define(TABLE, pg_local_table).

%%%
%%% Exported functions
%%%

-spec start_link() -> {'ok', pid()} | {'error', any()}.

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec start() -> {'ok', pid()} | {'error', any()}.

start() ->
    ensure_started().

-spec join(name(), pid()) -> 'ok'.

join(Name, Pid) when is_pid(Pid) ->
    _ = ensure_started(),
    gen_server:cast(?MODULE, {join, Name, Pid}).

-spec leave(name(), pid()) -> 'ok'.

leave(Name, Pid) when is_pid(Pid) ->
    _ = ensure_started(),
    gen_server:cast(?MODULE, {leave, Name, Pid}).

-spec get_members(name()) -> [pid()].

get_members(Name) ->
    _ = ensure_started(),
    group_members(Name).

-spec in_group(name(), pid()) -> boolean().

in_group(Name, Pid) ->
    _ = ensure_started(),
    %% The join message is a cast and thus can race, but we want to
    %% keep it that way to be fast in the common case.
    case member_present(Name, Pid) of
        true  -> true;
        false -> sync(),
                 member_present(Name, Pid)
    end.

-spec sync() -> 'ok'.

sync() ->
    _ = ensure_started(),
    gen_server:call(?MODULE, sync, infinity).

clear() ->
    _ = ensure_started(),
    gen_server:call(?MODULE, clear, infinity).

%%%
%%% Callback functions from gen_server
%%%

-record(state, {}).

init([]) ->
    ?TABLE = ets:new(?TABLE, [ordered_set, protected, named_table]),
    {ok, #state{}}.

handle_call(sync, _From, S) ->
    {reply, ok, S};

handle_call(clear, _From, S) ->
    ets:delete_all_objects(?TABLE),
    {reply, ok, S};

handle_call(Request, From, S) ->
    error_logger:warning_msg("The pg_local server received an unexpected message:\n"
                             "handle_call(~p, ~p, _)\n",
                             [Request, From]),
    {noreply, S}.

handle_cast({join, Name, Pid}, S) ->
    _ = join_group(Name, Pid),
    {noreply, S};
handle_cast({leave, Name, Pid}, S) ->
    leave_group(Name, Pid),
    {noreply, S};
handle_cast(_, S) ->
    {noreply, S}.

handle_info({'DOWN', MonitorRef, process, Pid, _Info}, S) ->
    member_died(MonitorRef, Pid),
    {noreply, S};
handle_info(_, S) ->
    {noreply, S}.

terminate(_Reason, _S) ->
    true = ets:delete(?TABLE),
    ok.

%%%
%%% Local functions
%%%

%%% One ETS table, pg_local_table, is used for bookkeeping. The type of the
%%% table is ordered_set, and the fast matching of partially
%%% instantiated keys is used extensively.
%%%
%%% {{ref, Pid}, MonitorRef, Counter}
%%% {{ref, MonitorRef}, Pid}
%%%    Each process has one monitor. Counter is incremented when the
%%%    Pid joins some group.
%%% {{member, Name, Pid}, _}
%%%    Pid is a member of group Name, GroupCounter is incremented when the
%%%    Pid joins the group Name.
%%% {{pid, Pid, Name}}
%%%    Pid is a member of group Name.

member_died(Ref, Pid) ->
    case ets:lookup(?TABLE, {ref, Ref}) of
        [{{ref, Ref}, Pid}] ->
            leave_all_groups(Pid);
        %% in case the key has already been removed
        %% we can clean up using the value from the DOWN message
        _  ->
            leave_all_groups(Pid)
    end,
    ok.

leave_all_groups(Pid) ->
    Names = member_groups(Pid),
    _ = [leave_group(Name, P) ||
            Name <- Names,
            P <- member_in_group(Pid, Name)].

join_group(Name, Pid) ->
    Ref_Pid = {ref, Pid},
    try _ = ets:update_counter(?TABLE, Ref_Pid, {3, +1})
    catch _:_ ->
            Ref = erlang:monitor(process, Pid),
            true = ets:insert(?TABLE, {Ref_Pid, Ref, 1}),
            true = ets:insert(?TABLE, {{ref, Ref}, Pid})
    end,
    Member_Name_Pid = {member, Name, Pid},
    try _ = ets:update_counter(?TABLE, Member_Name_Pid, {2, +1})
    catch _:_ ->
            true = ets:insert(?TABLE, {Member_Name_Pid, 1}),
            true = ets:insert(?TABLE, {{pid, Pid, Name}})
    end.

leave_group(Name, Pid) ->
    Member_Name_Pid = {member, Name, Pid},
    try ets:update_counter(?TABLE, Member_Name_Pid, {2, -1}) of
        N ->
            if
                N =:= 0 ->
                    true = ets:delete(?TABLE, {pid, Pid, Name}),
                    true = ets:delete(?TABLE, Member_Name_Pid);
                true ->
                    ok
            end,
            Ref_Pid = {ref, Pid},
            case ets:update_counter(?TABLE, Ref_Pid, {3, -1}) of
                0 ->
                    [{Ref_Pid,Ref,0}] = ets:lookup(?TABLE, Ref_Pid),
                    true = ets:delete(?TABLE, {ref, Ref}),
                    true = ets:delete(?TABLE, Ref_Pid),
                    true = erlang:demonitor(Ref, [flush]),
                    ok;
                _ ->
                    ok
            end
    catch _:_ ->
            ok
    end.

group_members(Name) ->
    [P ||
        [P, N] <- ets:match(?TABLE, {{member, Name, '$1'},'$2'}),
        _ <- lists:seq(1, N)].

member_in_group(Pid, Name) ->
    [{{member, Name, Pid}, N}] = ets:lookup(?TABLE, {member, Name, Pid}),
    lists:duplicate(N, Pid).

member_present(Name, Pid) ->
    case ets:lookup(?TABLE, {member, Name, Pid}) of
        [_] -> true;
        []  -> false
    end.

member_groups(Pid) ->
    [Name || [Name] <- ets:match(?TABLE, {{pid, Pid, '$1'}})].

ensure_started() ->
    case whereis(?MODULE) of
        undefined ->
            C = {pg_local, {?MODULE, start_link, []}, permanent,
                 16#ffffffff, worker, [?MODULE]},
            supervisor:start_child(kernel_safe_sup, C);
        PgLocalPid ->
            {ok, PgLocalPid}
    end.
