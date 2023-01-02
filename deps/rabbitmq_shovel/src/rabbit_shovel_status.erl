%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_shovel_status).
-behaviour(gen_server).

-export([start_link/0]).

-export([report/3,
         report_blocked_status/2,
         remove/1,
         status/0,
         lookup/1,
         cluster_status/0,
         cluster_status_with_nodes/0]).
-export([inject_node_info/2, find_matching_shovel/3]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).
-define(ETS_NAME, ?MODULE).
-define(CHECK_FREQUENCY, 60000).

%% rabbit_shovel_mgmt_util:format_info as well as CLI shovel commands
%% rely on this strict type as of 3.11.2
%% (would be good to allow any atom as status name)
-type info() :: starting
              | {running, proplists:proplist()}
              | {terminated, term()}.
-type blocked_status() :: running | flow | blocked.

-type name() :: binary() | {rabbit_types:vhost(), binary()}.
-type type() :: static | dynamic.
-type status_tuple() :: {name(), type(), info(), calendar:datetime()}.

-export_type([info/0, blocked_status/0]).

-record(state, {timer}).
-record(entry, {name :: name(),
                type :: type(),
                info :: info(),
                blocked_status = running :: blocked_status(),
                blocked_at :: integer() | undefined,
                timestamp :: calendar:datetime()}).

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec report(name(), type(), info()) -> ok.
report(Name, Type, Info) ->
    gen_server:cast(?SERVER, {report, Name, Type, Info, calendar:local_time()}).

-spec report_blocked_status(name(), blocked_status()) -> ok.
report_blocked_status(Name, Status) ->
    gen_server:cast(?SERVER, {report_blocked_status, Name, Status, erlang:monotonic_time()}).

-spec remove(name()) -> ok.
remove(Name) ->
    gen_server:cast(?SERVER, {remove, Name}).

%% Warning: this function could be called from other nodes in the
%% cluster with different RabbitMQ version. Don't change the returned
%% format without a feature flag.
-spec status() -> [status_tuple()].
status() ->
    gen_server:call(?SERVER, status, infinity).

-spec cluster_status() -> [status_tuple()].
cluster_status() ->
    Nodes = rabbit_nodes:all_running(),
    lists:usort(rabbit_misc:append_rpc_all_nodes(Nodes, ?MODULE, status, [])).

-spec cluster_status_with_nodes() -> [status_tuple()].
cluster_status_with_nodes() ->
    Nodes = rabbit_nodes:all_running(),
    lists:foldl(
        fun(Node, Acc) ->
            case rabbit_misc:rpc_call(Node, ?MODULE, status, []) of
                {badrpc, _} ->
                    Acc;
                Xs0 when is_list(Xs0) ->
                    Xs = inject_node_info(Node, Xs0),
                    Acc ++ Xs
            end
        end, [], Nodes).

-spec lookup(name()) -> proplists:proplist() | not_found.
lookup(Name) ->
    gen_server:call(?SERVER, {lookup, Name}, infinity).

init([]) ->
    ?ETS_NAME = ets:new(?ETS_NAME,
                        [named_table, {keypos, #entry.name}, private]),
    {ok, ensure_timer(#state{})}.

handle_call(status, _From, State) ->
    Entries = ets:tab2list(?ETS_NAME),
    {reply, [{Entry#entry.name,
              Entry#entry.type,
              blocked_status_to_info(Entry),
              Entry#entry.timestamp}
             || Entry <- Entries], State};

handle_call({lookup, Name}, _From, State) ->
    Link = case ets:lookup(?ETS_NAME, Name) of
               [Entry] -> [{name, Name},
                           {type, Entry#entry.type},
                           {info, blocked_status_to_info(Entry)},
                           {timestamp, Entry#entry.timestamp}];
               [] -> not_found
           end,
    {reply, Link, State}.

handle_cast({report, Name, Type, Info, Timestamp}, State) ->
    true = ets:insert(?ETS_NAME, #entry{name = Name, type = Type, info = Info,
                                        timestamp = Timestamp}),
    rabbit_event:notify(shovel_worker_status,
                        split_name(Name) ++ split_status(Info)),
    {noreply, State};

handle_cast({report_blocked_status, Name, Status, Timestamp}, State) ->
    case Status of
        flow ->
            true = ets:update_element(?ETS_NAME, Name, [{#entry.blocked_status, flow},
                                                        {#entry.blocked_at, Timestamp}]);
        _ ->
            true = ets:update_element(?ETS_NAME, Name, [{#entry.blocked_status, Status}])
    end,
    {noreply, State};

handle_cast({remove, Name}, State) ->
    true = ets:delete(?ETS_NAME, Name),
    rabbit_event:notify(shovel_worker_removed, split_name(Name)),
    {noreply, State}.

handle_info(check, State) ->
    try
        rabbit_shovel_dyn_worker_sup_sup:cleanup_specs()
    catch
        C:E ->
            rabbit_log_shovel:warning("Recurring shovel spec clean up failed with ~p:~p", [C, E])
    end,
    {noreply, ensure_timer(State)};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, State) ->
    _ = rabbit_misc:stop_timer(State, #state.timer),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

-spec inject_node_info(node(), [status_tuple()]) -> [status_tuple()].
inject_node_info(Node, Shovels) ->
    lists:map(
        fun({Name, Type, {State, Opts}, Timestamp}) ->
            Opts1 = Opts ++ [{node, Node}],
            {Name, Type, {State, Opts1}, Timestamp}
        end, Shovels).

-spec find_matching_shovel(rabbit_types:vhost(), binary(), [status_tuple()]) -> status_tuple() | undefined.
find_matching_shovel(VHost, Name, Shovels) ->
    case lists:filter(
        fun ({{V, S}, _Kind, _Status, _}) ->
            VHost =:= V andalso Name =:= S
        end, Shovels) of
            []  -> undefined;
        [S | _] -> S
    end.

%%
%% Implementation
%%

-spec split_status(info()) -> proplists:proplist().
split_status({running, MoreInfo})         -> [{status, running} | MoreInfo];
split_status({terminated, Reason})        -> [{status, terminated},
                                              {reason, Reason}];
split_status(Status) when is_atom(Status) -> [{status, Status}].

split_name({VHost, Name})           -> [{name,  Name},
                                        {vhost, VHost}];
split_name(Name) when is_atom(Name) -> [{name, Name}].

ensure_timer(State0) ->
    State1 = rabbit_misc:stop_timer(State0, #state.timer),
    rabbit_misc:ensure_timer(State1, #state.timer, ?CHECK_FREQUENCY, check).

-spec blocked_status_to_info(#entry{}) -> info().
blocked_status_to_info(#entry{info = {running, Info},
                              blocked_status = BlockedStatus0,
                              blocked_at = BlockedAt}) ->
    BlockedStatus =
        case BlockedStatus0 of
            running ->
                credit_flow:state_delayed(BlockedAt);
            _ ->
                BlockedStatus0
        end,
    {running, Info ++ [{blocked_status, BlockedStatus}]};
blocked_status_to_info(#entry{info = Info}) ->
    Info.

