%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_stream_coordinator).

-behaviour(ra_machine).

-export([format_ra_event/2]).

-export([init/1,
         apply/3,
         state_enter/2,
         init_aux/1,
         handle_aux/6,
         tick/2]).

-export([recover/0,
         add_replica/2,
         delete_replica/2,
         register_listener/1]).

-export([new_stream/2,
         delete_stream/2]).

-export([policy_changed/1]).

-export([local_pid/1,
         members/1]).
-export([query_local_pid/3,
         query_members/2]).


-export([log_overview/1]).
-export([replay/1]).

-rabbit_boot_step({?MODULE,
                   [{description, "Restart stream coordinator"},
                    {mfa,         {?MODULE, recover, []}},
                    {requires,    core_initialized},
                    {enables,     recovery}]}).

%% exported for unit tests only
-ifdef(TEST).
-export([update_stream/3,
         evaluate_stream/3]).
-endif.

-include("rabbit_stream_coordinator.hrl").
-include("amqqueue.hrl").

-define(REPLICA_FRESHNESS_LIMIT_MS, 10 * 1000). %% 10s

-type state() :: #?MODULE{}.
-type args() :: #{index := ra:index(),
                  node := node(),
                  epoch := osiris:epoch()}.

-type command() :: {new_stream, stream_id(), #{leader_node := node(),
                                               queue := amqqueue:amqqueue()}} |
                   {delete_stream, stream_id(), #{}} |
                   {add_replica, stream_id(), #{node := node()}} |
                   {delete_replica, stream_id(), #{node := node()}} |
                   {policy_changed, stream_id(), #{queue := amqqueue:amqqueue()}} |
                   {register_listener, #{pid := pid(),
                                         stream_id := stream_id(),
                                         queue_ref := queue_ref()}} |
                   {action_failed, stream_id(), #{index := ra:index(),
                                                  node := node(),
                                                  epoch := osiris:epoch(),
                                                  action := atom(), %% TODO: refine
                                                  term() => term()}} |
                   {member_started, stream_id(), #{index := ra:index(),
                                                   node := node(),
                                                   epoch := osiris:epoch(),
                                                   pid := pid()}} |
                   {member_stopped, stream_id(), args()} |
                   {retention_updated, stream_id(), args()} |
                   {mnesia_updated, stream_id(), args()} |
                   ra_machine:effect().

-export_type([command/0]).

recover() ->
    case erlang:whereis(?MODULE) of
        undefined ->
            case ra:restart_server(?RA_SYSTEM, {?MODULE, node()}) of
                {error, Reason} when Reason == not_started;
                                     Reason == name_not_registered ->
                    %% First boot, do nothing and wait until the first `declare`
                    ok;
                _ ->
                    ok
            end;
        _ ->
            ok
    end.

%% new api

new_stream(Q, LeaderNode)
  when ?is_amqqueue(Q) andalso is_atom(LeaderNode) ->
    #{name := StreamId,
      nodes := Nodes} = amqqueue:get_type_state(Q),
    %% assertion leader is in nodes configuration
    true = lists:member(LeaderNode, Nodes),
    process_command({new_stream, StreamId,
                     #{leader_node => LeaderNode,
                       queue => Q}}).

delete_stream(Q, ActingUser)
  when ?is_amqqueue(Q) ->
    #{name := StreamId} = amqqueue:get_type_state(Q),
    case process_command({delete_stream, StreamId, #{}}) of
        {ok, ok, _} ->
            QName = amqqueue:get_name(Q),
              _ = rabbit_amqqueue:internal_delete(QName, ActingUser),
            {ok, {ok, 0}};
        Err ->
            Err
    end.

-spec add_replica(amqqueue:amqqueue(), node()) ->
    ok | {error, term()}.
add_replica(Q, Node) when ?is_amqqueue(Q) ->
    %% performing safety check
    %% if any replica is stale then we should not allow
    %% further replicas to be added
    Pid = amqqueue:get_pid(Q),
    try
        ReplState0 = osiris_writer:query_replication_state(Pid),
        {{_, InitTs}, ReplState} = maps:take(node(Pid), ReplState0),
        {MaxTs, MinTs} = maps:fold(fun (_, {_, Ts}, {Max, Min}) ->
                                           {max(Ts, Max), min(Ts, Min)}
                                   end, {InitTs, InitTs}, ReplState),
        case (MaxTs - MinTs) > ?REPLICA_FRESHNESS_LIMIT_MS of
            true ->
                {error, {disallowed, out_of_sync_replica}};
            false ->
                Name = rabbit_misc:rs(amqqueue:get_name(Q)),
                rabbit_log:info("~s : adding replica ~s to ~s Replication State: ~w",
                                [?MODULE, Node, Name, ReplState0]),
                StreamId = maps:get(name, amqqueue:get_type_state(Q)),
                case process_command({add_replica, StreamId, #{node => Node}}) of
                    {ok, Result, _} ->
                        Result;
                    Err ->
                        Err
                end
        end
    catch
        _:Error ->
            {error, Error}
    end.

delete_replica(StreamId, Node) ->
    process_command({delete_replica, StreamId, #{node => Node}}).

policy_changed(Q) when ?is_amqqueue(Q) ->
    StreamId = maps:get(name, amqqueue:get_type_state(Q)),
    process_command({policy_changed, StreamId, #{queue => Q}}).

local_pid(StreamId) when is_list(StreamId) ->
    MFA = {?MODULE, query_local_pid, [StreamId, node()]},
    case ra:local_query({?MODULE, node()}, MFA) of
        {ok, {_, {ok, Pid}}, _} ->
            case is_process_alive(Pid) of
                true ->
                    {ok, Pid};
                false ->
                    case ra:consistent_query({?MODULE, node()}, MFA) of
                        {ok, Result, _} ->
                            Result;
                        {error, _} = Err ->
                            Err;
                        {timeout, _} ->
                            {error, timeout}
                    end
            end;
        {ok, {_, Result}, _} ->
            Result;
        {error, _} = Err ->
            Err;
        {timeout, _} ->
            {error, timeout}
    end.

-spec members(stream_id()) ->
    {ok, #{node() := {pid() | undefined, writer | replica}}} |
    {error, not_found}.
members(StreamId) when is_list(StreamId) ->
    MFA = {?MODULE, query_members, [StreamId]},
    case ra:local_query({?MODULE, node()}, MFA) of
        {ok, {_, {ok, _} = Result}, _} ->
            Result;
        {ok, {_, {error, not_found}}, _} ->
            %% fall back to consistent query
            case ra:consistent_query({?MODULE, node()}, MFA) of
                {ok, Result, _} ->
                    Result;
                {error, _} = Err ->
                    Err;
                {timeout, _} ->
                    {error, timeout}
            end;
        {ok, {_, Result}, _} ->
            Result;
        {error, _} = Err ->
            Err;
        {timeout, _} ->
            {error, timeout}
    end.

query_members(StreamId, #?MODULE{streams = Streams}) ->
    case Streams of
        #{StreamId := #stream{members = Members}} ->
            {ok, maps:map(
                   fun (_, #member{state = {running, _, Pid},
                                   role = {Role, _}}) ->
                           {Pid, Role};
                       (_, #member{role = {Role, _}}) ->
                           {undefined, Role}
                   end, Members)};
        _ ->
            {error, not_found}
    end.

query_local_pid(StreamId, Node, #?MODULE{streams = Streams}) ->
    case Streams of
        #{StreamId := #stream{members =
                              #{Node := #member{state =
                                                {running, _, Pid}}}}} ->
            {ok, Pid};
        _ ->
            {error, not_found}
    end.

-spec register_listener(amqqueue:amqqueue()) ->
    {error, term()} | {ok, ok, atom() | {atom(), atom()}}.
register_listener(Q) when ?is_amqqueue(Q)->
    #{name := StreamId} = amqqueue:get_type_state(Q),
    process_command({register_listener,
                     #{pid => self(),
                       stream_id => StreamId}}).

process_command(Cmd) ->
    Servers = ensure_coordinator_started(),
    process_command(Servers, Cmd).

process_command([], _Cmd) ->
    {error, coordinator_unavailable};
process_command([Server | Servers], Cmd) ->
    case ra:process_command(Server, Cmd, ?CMD_TIMEOUT) of
        {timeout, _} ->
            rabbit_log:warning("Coordinator timeout on server ~s when processing command ~W",
                               [Server, element(1, Cmd), 10]),
            process_command(Servers, Cmd);
        {error, noproc} ->
            process_command(Servers, Cmd);
        Reply ->
            Reply
    end.

ensure_coordinator_started() ->
    Local = {?MODULE, node()},
    AllNodes = all_coord_members(),
    case whereis(?MODULE) of
        undefined ->
            global:set_lock(?STREAM_COORDINATOR_STARTUP),
            Nodes = case ra:restart_server(?RA_SYSTEM, Local) of
                {error, Reason} when Reason == not_started orelse
                                     Reason == name_not_registered ->
                    OtherNodes = all_coord_members() -- [Local],
                    %% We can't use find_members/0 here as a process that timeouts means the cluster is up
                    case lists:filter(fun(N) -> global:whereis_name(N) =/= undefined end, OtherNodes) of
                        [] ->
                            start_coordinator_cluster();
                        _ ->
                            OtherNodes
                    end;
                ok ->
                    AllNodes;
                {error, {already_started, _}} ->
                            AllNodes;
                _ ->
                    AllNodes
            end,
            global:del_lock(?STREAM_COORDINATOR_STARTUP),
            Nodes;
        _ ->
            AllNodes
    end.

start_coordinator_cluster() ->
    Nodes = rabbit_mnesia:cluster_nodes(running),
    rabbit_log:debug("Starting stream coordinator on nodes: ~w", [Nodes]),
    case ra:start_cluster(?RA_SYSTEM, [make_ra_conf(Node, Nodes) || Node <-  Nodes]) of
        {ok, Started, _} ->
            rabbit_log:debug("Started stream coordinator on ~w", [Started]),
            Started;
        {error, cluster_not_formed} ->
            rabbit_log:warning("Stream coordinator could not be started on nodes ~w",
                               [Nodes]),
            []
    end.

all_coord_members() ->
    Nodes = rabbit_mnesia:cluster_nodes(running) -- [node()],
    [{?MODULE, Node} || Node <- [node() | Nodes]].

init(_Conf) ->
    #?MODULE{}.

-spec apply(map(), command(), state()) ->
    {state(), term(), ra_machine:effects()}.
apply(#{index := _Idx} = Meta0, {_CmdTag, StreamId, #{}} = Cmd,
      #?MODULE{streams = Streams0,
               monitors = Monitors0} = State0) ->
    Stream0 = maps:get(StreamId, Streams0, undefined),
    Meta = maps:without([term, machine_version], Meta0),
    case filter_command(Meta, Cmd, Stream0) of
        ok ->
            Stream1 = update_stream(Meta, Cmd, Stream0),
            Reply = case Stream1 of
                        #stream{reply_to = undefined} ->
                            ok;
                        _ ->
                            %% reply_to is set so we'll reply later
                            '$ra_no_reply'
                    end,
            case Stream1 of
                undefined ->
                    return(Meta, State0#?MODULE{streams = maps:remove(StreamId, Streams0)},
                           Reply, []);
                _ ->
                    {Stream2, Effects0} = evaluate_stream(Meta, Stream1, []),
                    {Stream3, Effects1} = eval_listeners(Stream2, Effects0),
                    {Stream, Effects2} = eval_retention(Meta, Stream3, Effects1),
                    {Monitors, Effects} = ensure_monitors(Stream, Monitors0, Effects2),
                    return(Meta,
                           State0#?MODULE{streams = Streams0#{StreamId => Stream},
                                          monitors = Monitors}, Reply, Effects)
            end;
        Reply ->
            return(Meta, State0, Reply, [])
    end;
apply(Meta, {down, Pid, Reason} = Cmd,
      #?MODULE{streams = Streams0,
               listeners = Listeners0,
               monitors = Monitors0} = State) ->

    Effects0 = case Reason of
                   noconnection ->
                       [{monitor, node, node(Pid)}];
                   _ ->
                       []
               end,
    case maps:take(Pid, Monitors0) of
        {{StreamId, listener}, Monitors} ->
            Listeners = case maps:take(StreamId, Listeners0) of
                            error ->
                                Listeners0;
                            {Pids0, Listeners1} ->
                                case maps:remove(Pid, Pids0) of
                                    Pids when map_size(Pids) == 0 ->
                                        Listeners1;
                                    Pids ->
                                        Listeners1#{StreamId => Pids}
                                end
                        end,
            return(Meta, State#?MODULE{listeners = Listeners,
                                       monitors = Monitors}, ok, Effects0);
        {{StreamId, member}, Monitors1} ->
            case Streams0 of
                #{StreamId := Stream0} ->
                    Stream1 = update_stream(Meta, Cmd, Stream0),
                    {Stream, Effects} = evaluate_stream(Meta, Stream1, Effects0),
                    Streams = Streams0#{StreamId => Stream},
                    return(Meta, State#?MODULE{streams = Streams,
                                               monitors = Monitors1}, ok,
                           Effects);
                _ ->
                    %% stream not found, can happen if "late" downs are
                    %% received
                    return(Meta, State#?MODULE{streams = Streams0,
                                               monitors = Monitors1}, ok, Effects0)
            end;
        error ->
            return(Meta, State, ok, Effects0)
    end;
apply(Meta, {register_listener, #{pid := Pid,
                                  stream_id := StreamId}},
      #?MODULE{streams = Streams,
               monitors = Monitors0} = State0) ->
    case Streams of
        #{StreamId := #stream{listeners = Listeners0} = Stream0} ->
            Stream1 = Stream0#stream{listeners = maps:put(Pid, undefined, Listeners0)},
            {Stream, Effects} = eval_listeners(Stream1, []),
            Monitors = maps:put(Pid, {StreamId, listener}, Monitors0),
            return(Meta,
                   State0#?MODULE{streams = maps:put(StreamId, Stream, Streams),
                                  monitors = Monitors}, ok,
                   [{monitor, process, Pid} | Effects]);
        _ ->
            return(Meta, State0, stream_not_found, [])
    end;
apply(Meta, {nodeup, Node} = Cmd,
      #?MODULE{monitors = Monitors0,
               streams = Streams0} = State)  ->
    %% reissue monitors for all disconnected members
    {Effects0, Monitors} =
        maps:fold(
          fun(_, #stream{id = Id,
                         members = M}, {Acc, Mon}) ->
                  case M of
                      #{Node := #member{state = {disconnected, _, P}}} ->
                          {[{monitor, process, P} | Acc],
                           Mon#{P => {Id, member}}};
                      _ ->
                          {Acc, Mon}
                  end
          end, {[], Monitors0}, Streams0),
    {Streams, Effects} =
        maps:fold(fun (Id, S0, {Ss, E0}) ->
                          S1 = update_stream(Meta, Cmd, S0),
                          {S, E} = evaluate_stream(Meta, S1, E0),
                          {Ss#{Id => S}, E}
                  end, {Streams0, Effects0}, Streams0),
    return(Meta, State#?MODULE{monitors = Monitors,
                               streams = Streams}, ok, Effects);
apply(Meta, UnkCmd, State) ->
    rabbit_log:debug("~s: unknown command ~W",
                     [?MODULE, UnkCmd, 10]),
    return(Meta, State, {error, unknown_command}, []).

return(#{index := Idx}, State, Reply, Effects) ->
    case Idx rem 4096 == 0 of
        true ->
            %% add release cursor effect
            {State, Reply, [{release_cursor, Idx, State} | Effects]};
        false ->
            {State, Reply, Effects}
    end.

state_enter(recover, _) ->
    put('$rabbit_vm_category', ?MODULE),
    [];
state_enter(leader, #?MODULE{streams = Streams,
                             monitors = Monitors}) ->
    Pids = maps:keys(Monitors),
    %% monitor all the known nodes
    Nodes = all_member_nodes(Streams),
    NodeMons = [{monitor, node, N} || N <- Nodes],
    NodeMons ++ [{aux, fail_active_actions} |
                 [{monitor, process, P} || P <- Pids]];
state_enter(_S, _) ->
    [].

all_member_nodes(Streams) ->
    maps:keys(
      maps:fold(
        fun (_, #stream{members = M}, Acc) ->
                maps:merge(Acc, M)
        end, #{}, Streams)).

tick(_Ts, _State) ->
    [{aux, maybe_resize_coordinator_cluster}].

maybe_resize_coordinator_cluster() ->
    spawn(fun() ->
                  case ra:members({?MODULE, node()}) of
                      {_, Members, _} ->
                          MemberNodes = [Node || {_, Node} <- Members],
                          Running = rabbit_mnesia:cluster_nodes(running),
                          All = rabbit_mnesia:cluster_nodes(all),
                          case Running -- MemberNodes of
                              [] ->
                                  ok;
                              New ->
                                  rabbit_log:info("~s: New rabbit node(s) detected, "
                                                  "adding : ~w",
                                                  [?MODULE, New]),
                                  add_members(Members, New)
                          end,
                          case MemberNodes -- All of
                              [] ->
                                  ok;
                              Old ->
                                  rabbit_log:info("~s: Rabbit node(s) removed from the cluster, "
                                                  "deleting: ~w", [?MODULE, Old]),
                                  remove_members(Members, Old)
                          end;
                      _ ->
                          ok
                  end
          end).

add_members(_, []) ->
    ok;
add_members(Members, [Node | Nodes]) ->
    Conf = make_ra_conf(Node, [N || {_, N} <- Members]),
    case ra:start_server(?RA_SYSTEM, Conf) of
        ok ->
            case ra:add_member(Members, {?MODULE, Node}) of
                {ok, NewMembers, _} ->
                    add_members(NewMembers, Nodes);
                _ ->
                    add_members(Members, Nodes)
            end;
        Error ->
            rabbit_log:warning("Stream coordinator failed to start on node ~s : ~W",
                               [Node, Error, 10]),
            add_members(Members, Nodes)
    end.

remove_members(_, []) ->
    ok;
remove_members(Members, [Node | Nodes]) ->
    case ra:remove_member(Members, {?MODULE, Node}) of
        {ok, NewMembers, _} ->
            remove_members(NewMembers, Nodes);
        _ ->
            remove_members(Members, Nodes)
    end.

-record(aux, {actions = #{} ::
              #{pid() := {stream_id(), #{node := node(),
                                         index := non_neg_integer(),
                                         epoch := osiris:epoch()}}},
              resizer :: undefined | pid()}).

init_aux(_Name) ->
    #aux{}.

%% TODO ensure the dead writer is restarted as a replica at some point in time, increasing timeout?
handle_aux(leader, _, maybe_resize_coordinator_cluster,
           #aux{resizer = undefined} = Aux, LogState, _) ->
    Pid = maybe_resize_coordinator_cluster(),
    {no_reply, Aux#aux{resizer = Pid}, LogState, [{monitor, process, aux, Pid}]};
handle_aux(leader, _, maybe_resize_coordinator_cluster,
           AuxState, LogState, _) ->
    %% Coordinator resizing is still happening, let's ignore this tick event
    {no_reply, AuxState, LogState};
handle_aux(leader, _, {down, Pid, _},
           #aux{resizer = Pid} = Aux, LogState, _) ->
    %% Coordinator resizing has finished
    {no_reply, Aux#aux{resizer = undefined}, LogState};
handle_aux(leader, _, {start_writer, StreamId,
                       #{epoch := Epoch, node := Node} = Args, Conf},
           Aux, LogState, _) ->
    rabbit_log:debug("~s: running action: 'start_writer'"
                     " for ~s on node ~w in epoch ~b",
                     [?MODULE, StreamId, Node, Epoch]),
    ActionFun = phase_start_writer(StreamId, Args, Conf),
    run_action(starting, StreamId, Args, ActionFun, Aux, LogState);
handle_aux(leader, _, {start_replica, StreamId,
                       #{epoch := Epoch, node := Node} = Args, Conf},
           Aux, LogState, _) ->
    rabbit_log:debug("~s: running action: 'start_replica'"
                     " for ~s on node ~w in epoch ~b",
                     [?MODULE, StreamId, Node, Epoch]),
    ActionFun = phase_start_replica(StreamId, Args, Conf),
    run_action(starting, StreamId, Args, ActionFun, Aux, LogState);
handle_aux(leader, _, {stop, StreamId, #{node := Node,
                                         epoch := Epoch} = Args, Conf},
           Aux, LogState, _) ->
    rabbit_log:debug("~s: running action: 'stop'"
                     " for ~s on node ~w in epoch ~b",
                     [?MODULE, StreamId, Node, Epoch]),
    ActionFun = phase_stop_member(StreamId, Args, Conf),
    run_action(stopping, StreamId, Args, ActionFun, Aux, LogState);
handle_aux(leader, _, {update_mnesia, StreamId, Args, Conf},
           #aux{actions = _Monitors} = Aux, LogState,
           #?MODULE{streams = _Streams}) ->
    rabbit_log:debug("~s: running action: 'update_mnesia'"
                     " for ~s", [?MODULE, StreamId]),
    ActionFun = phase_update_mnesia(StreamId, Args, Conf),
    run_action(updating_mnesia, StreamId, Args, ActionFun, Aux, LogState);
handle_aux(leader, _, {update_retention, StreamId, Args, _Conf},
           #aux{actions = _Monitors} = Aux, LogState,
           #?MODULE{streams = _Streams}) ->
    rabbit_log:debug("~s: running action: 'update_retention'"
                     " for ~s", [?MODULE, StreamId]),
    ActionFun = phase_update_retention(StreamId, Args),
    run_action(update_retention, StreamId, Args, ActionFun, Aux, LogState);
handle_aux(leader, _, {delete_member, StreamId, #{node := Node} = Args, Conf},
           #aux{actions = _Monitors} = Aux, LogState,
           #?MODULE{streams = _Streams}) ->
    rabbit_log:debug("~s: running action: 'delete_member'"
                     " for ~s ~s", [?MODULE, StreamId, Node]),
    ActionFun = phase_delete_member(StreamId, Args, Conf),
    run_action(delete_member, StreamId, Args, ActionFun, Aux, LogState);
handle_aux(leader, _, fail_active_actions,
           #aux{actions = Monitors} = Aux, LogState,
           #?MODULE{streams = Streams}) ->
    Exclude = maps:from_list([{S, ok}
                              || {P, {S, _, _}} <- maps:to_list(Monitors),
                             not is_process_alive(P)]),
    rabbit_log:debug("~s: failing actions: ~w", [?MODULE, Exclude]),
    fail_active_actions(Streams, Exclude),
    {no_reply, Aux, LogState, []};
handle_aux(leader, _, {down, Pid, normal},
           #aux{actions = Monitors} = Aux, LogState, _) ->
    %% action process finished normally, just remove from actions map
    {no_reply, Aux#aux{actions = maps:remove(Pid, Monitors)}, LogState, []};
handle_aux(leader, _, {down, Pid, Reason},
           #aux{actions = Monitors0} = Aux, LogState, _) ->
    %% An action has failed - report back to the state machine
    case maps:get(Pid, Monitors0, undefined) of
        {StreamId, Action, #{node := Node, epoch := Epoch} = Args} ->
            rabbit_log:warning("~s: error while executing action for stream queue ~s, "
                               " node ~s, epoch ~b Err: ~w",
                               [?MODULE, StreamId, Node, Epoch, Reason]),
            Monitors = maps:remove(Pid, Monitors0),
            Cmd = {action_failed, StreamId, Args#{action => Action}},
            send_self_command(Cmd),
            {no_reply, Aux#aux{actions = maps:remove(Pid, Monitors)},
             LogState, []};
        undefined ->
            %% should this ever happen?
            {no_reply, Aux, LogState, []}
    end;
handle_aux(_, _, _, AuxState, LogState, _) ->
    {no_reply, AuxState, LogState}.

run_action(Action, StreamId, #{node := _Node,
                               epoch := _Epoch} = Args,
           ActionFun, #aux{actions = Actions0} = Aux, Log) ->
    Coordinator = self(),
    Pid = spawn_link(fun() ->
                             ActionFun(),
                             unlink(Coordinator)
                     end),
    Effects = [],
    Actions = Actions0#{Pid => {StreamId, Action, Args}},
    {no_reply, Aux#aux{actions = Actions}, Log, Effects}.

wrap_reply(From, Reply) ->
    [{reply, From, {wrap_reply, Reply}}].

phase_start_replica(StreamId, #{epoch := Epoch,
                                node := Node} = Args, Conf0) ->
    fun() ->
            try osiris_replica:start(Node, Conf0) of
                {ok, Pid} ->
                    rabbit_log:info("~s: ~s: replica started on ~s in ~b pid ~w",
                                    [?MODULE, StreamId, Node, Epoch, Pid]),
                    send_self_command({member_started, StreamId,
                                       Args#{pid => Pid}});
                {error, already_present} ->
                    %% need to remove child record if this is the case
                    %% can it ever happen?
                    _ = osiris_replica:stop(Node, Conf0),
                    send_action_failed(StreamId, starting, Args);
                {error, {already_started, Pid}} ->
                    %% TODO: we need to check that the current epoch is the same
                    %% before we can be 100% sure it is started in the correct
                    %% epoch, can this happen? who knows...
                    send_self_command({member_started, StreamId,
                                       Args#{pid => Pid}});
                {error, Reason} ->
                    rabbit_log:warning("~s: Error while starting replica for ~s on node ~s in ~b : ~W",
                                       [?MODULE, maps:get(name, Conf0), Node, Epoch, Reason, 10]),
                    maybe_sleep(Reason),
                    send_action_failed(StreamId, starting, Args)
            catch _:Error ->
                    rabbit_log:warning("~s: Error while starting replica for ~s on node ~s in ~b : ~W",
                                       [?MODULE, maps:get(name, Conf0), Node, Epoch, Error, 10]),
                    maybe_sleep(Error),
                    send_action_failed(StreamId, starting, Args)
            end
    end.

send_action_failed(StreamId, Action, Arg) ->
  send_self_command({action_failed, StreamId, Arg#{action => Action}}).

send_self_command(Cmd) ->
    ra:pipeline_command({?MODULE, node()}, Cmd),
    ok.


phase_delete_member(StreamId, #{node := Node} = Arg, Conf) ->
    fun() ->
            try osiris_server_sup:delete_child(Node, Conf) of
                ok ->
                    send_self_command({member_deleted, StreamId, Arg});
                _ ->
                    send_action_failed(StreamId, deleting, Arg)
            catch _:E ->
                    rabbit_log:warning("~s: Error while deleting member for ~s : on node ~s ~W",
                                       [?MODULE, StreamId, Node, E, 10]),
                    maybe_sleep(E),
                    send_action_failed(StreamId, deleting, Arg)
            end
    end.

phase_stop_member(StreamId, #{node := Node,
                              epoch := Epoch} = Arg0, Conf) ->
    fun() ->
            try osiris_server_sup:stop_child(Node, StreamId) of
                ok ->
                    %% get tail
                    try get_replica_tail(Node, Conf) of
                        {ok, Tail} ->
                            Arg = Arg0#{tail => Tail},
                            rabbit_log:debug("~s: ~s: member stopped on ~s in ~b Tail ~w",
                                             [?MODULE, StreamId, Node, Epoch, Tail]),
                            send_self_command({member_stopped, StreamId, Arg});
                        Err ->
                            rabbit_log:warning("~s: failed to get tail of member ~s on ~s in ~b Error: ~w",
                                               [?MODULE, StreamId, Node, Epoch, Err]),
                            maybe_sleep(Err),
                            send_action_failed(StreamId, stopping, Arg0)
                    catch _:Err ->
                            rabbit_log:warning("~s: failed to get tail of member ~s on ~s in ~b Error: ~w",
                                               [?MODULE, StreamId, Node, Epoch, Err]),
                            maybe_sleep(Err),
                            send_action_failed(StreamId, stopping, Arg0)
                    end;
                Err ->
                    rabbit_log:warning("~s: failed to stop "
                                       "member ~s ~w Error: ~w",
                                       [?MODULE, StreamId, Node, Err]),
                    maybe_sleep(Err),
                    send_action_failed(StreamId, stopping, Arg0)
            catch _:Err ->
                      rabbit_log:warning("~s: failed to stop member ~s ~w Error: ~w",
                                         [?MODULE, StreamId, Node, Err]),
                      maybe_sleep(Err),
                      send_action_failed(StreamId, stopping, Arg0)
            end
    end.

phase_start_writer(StreamId, #{epoch := Epoch,
                               node := Node} = Args0, Conf) ->
    fun() ->
            try osiris_writer:start(Conf) of
                {ok, Pid} ->
                    Args = Args0#{epoch => Epoch, pid => Pid},
                    rabbit_log:info("~s: started writer ~s on ~w in ~b",
                                    [?MODULE, StreamId, Node, Epoch]),
                    send_self_command({member_started, StreamId, Args});
                Err ->
                    %% no sleep for writer failures as we want to trigger a new
                    %% election asap
                    rabbit_log:warning("~s: failed to start writer ~s on ~s in ~b Error: ~w",
                                       [?MODULE, StreamId, Node, Epoch, Err]),
                    send_action_failed(StreamId, starting, Args0)
            catch _:Err ->
                    rabbit_log:warning("~s: failed to start writer ~s on ~s in ~b Error: ~w",
                                       [?MODULE, StreamId, Node, Epoch, Err]),
                    send_action_failed(StreamId, starting, Args0)
            end
    end.

phase_update_retention(StreamId, #{pid := Pid,
                                   retention := Retention} = Args) ->
    fun() ->
            try osiris:update_retention(Pid, Retention) of
                ok ->
                    send_self_command({retention_updated, StreamId, Args});
                {error, Reason} = Err ->
                    rabbit_log:warning("~s: failed to update retention for ~s ~w Reason: ~w",
                                       [?MODULE, StreamId, node(Pid), Reason]),
                    maybe_sleep(Err),
                    send_action_failed(StreamId, update_retention, Args)
            catch _:Err ->
                    rabbit_log:warning("~s: failed to update retention for ~s ~w Error: ~w",
                                       [?MODULE, StreamId, node(Pid), Err]),
                    maybe_sleep(Err),
                    send_action_failed(StreamId, update_retention, Args)
            end
    end.

get_replica_tail(Node, Conf) ->
    case rpc:call(Node, ?MODULE, log_overview, [Conf]) of
        {badrpc, nodedown} ->
            {error, nodedown};
        {error, _} = Err ->
            Err;
        {_Range, Offsets} ->
            {ok, select_highest_offset(Offsets)}
    end.

select_highest_offset([]) ->
    empty;
select_highest_offset(Offsets) ->
    lists:last(Offsets).

log_overview(Config) ->
    case whereis(osiris_sup) of
        undefined ->
            {error, app_not_running};
        _ ->
            Dir = osiris_log:directory(Config),
            osiris_log:overview(Dir)
    end.


replay(L) when is_list(L) ->
    lists:foldl(
      fun ({M, E}, Acc) ->
              element(1, ?MODULE:apply(M, E, Acc))
      end, init(#{}), L).

is_quorum(1, 1) ->
    true;
is_quorum(NumReplicas, NumAlive) ->
    NumAlive >= ((NumReplicas div 2) + 1).

phase_update_mnesia(StreamId, Args, #{reference := QName,
                                      leader_pid := LeaderPid} = Conf) ->
    fun() ->
            rabbit_log:debug("~s: running mnesia update for ~s: ~W",
                             [?MODULE, StreamId, Conf, 10]),
            Fun = fun (Q) ->
                          amqqueue:set_type_state(amqqueue:set_pid(Q, LeaderPid), Conf)
                  end,
            try rabbit_misc:execute_mnesia_transaction(
                  fun() ->
                          rabbit_amqqueue:update(QName, Fun)
                  end) of
                not_found ->
                    %% This can happen during recovery
                    [Q] = mnesia:dirty_read(rabbit_durable_queue, QName),
                    %% TODO: what is the possible return type here?
                    _ = rabbit_amqqueue:ensure_rabbit_queue_record_is_initialized(Fun(Q)),
                    send_self_command({mnesia_updated, StreamId, Args});
                _ ->
                    send_self_command({mnesia_updated, StreamId, Args})
            catch _:E ->
                    rabbit_log:debug("~s: failed to update mnesia for ~s: ~W",
                                     [?MODULE, StreamId, E, 10]),
                    send_action_failed(StreamId, updating_mnesia, Args)
            end
    end.

format_ra_event(ServerId, Evt) ->
    {stream_coordinator_event, ServerId, Evt}.

make_ra_conf(Node, Nodes) ->
    UId = ra:new_uid(ra_lib:to_binary(?MODULE)),
    Formatter = {?MODULE, format_ra_event, []},
    Members = [{?MODULE, N} || N <- Nodes],
    TickTimeout = application:get_env(rabbit, stream_tick_interval,
                                      ?TICK_TIMEOUT),
    #{cluster_name => ?MODULE,
      id => {?MODULE, Node},
      uid => UId,
      friendly_name => atom_to_list(?MODULE),
      metrics_key => ?MODULE,
      initial_members => Members,
      log_init_args => #{uid => UId},
      tick_timeout => TickTimeout,
      machine => {module, ?MODULE, #{}},
      ra_event_formatter => Formatter}.

filter_command(_Meta, {delete_replica, _, #{node := Node}}, #stream{id = StreamId,
                                                                    members = Members0}) ->
    Members = maps:filter(fun(_, #member{target = S}) when S =/= deleted ->
                                  true;
                             (_, _) ->
                                  false
                          end, Members0),
    case maps:size(Members) =< 1 of
        true ->
            rabbit_log:warning(
              "~s failed to delete replica on node ~s for stream ~s: refusing to delete the only replica",
              [?MODULE, Node, StreamId]),
            {error, last_stream_member};
        false ->
            ok
    end;
filter_command(_, _, _) ->
    ok.

update_stream(Meta, Cmd, Stream) ->
    try
        update_stream0(Meta, Cmd, Stream)
    catch
        _:E:Stacktrace ->
            rabbit_log:warning(
              "~s failed to update stream:~n~W~n~W",
              [?MODULE, E, 10, Stacktrace, 10]),
            Stream
    end.

update_stream0(#{system_time := _} = Meta,
               {new_stream, StreamId, #{leader_node := LeaderNode,
                                        queue := Q}}, undefined) ->
    #{nodes := Nodes} = Conf = amqqueue:get_type_state(Q),
    %% this jumps straight to the state where all members
    %% have been stopped and a new writer has been chosen
    E = 1,
    QueueRef = amqqueue:get_name(Q),
    Members = maps:from_list(
                [{N, #member{role = case LeaderNode of
                                        N -> {writer, E};
                                        _ -> {replica, E}
                                    end,
                             node = N,
                             state = {ready, E},
                             %% no members are running actions
                             current = undefined}
                 } || N <- Nodes]),
    #stream{id = StreamId,
            epoch = E,
            nodes = Nodes,
            queue_ref = QueueRef,
            conf = Conf,
            members = Members,
            reply_to = maps:get(from, Meta, undefined)};
update_stream0(#{system_time := _Ts} = _Meta,
               {delete_stream, _StreamId, #{}},
               #stream{members = Members0,
                       target = _} = Stream0) ->
    Members = maps:map(
                fun (_, M) ->
                        M#member{target = deleted}
                end, Members0),
    Stream0#stream{members = Members,
                   % reply_to = maps:get(from, Meta, undefined),
                   target = deleted};
update_stream0(#{system_time := _Ts} = _Meta,
               {add_replica, _StreamId, #{node := Node}},
               #stream{members = Members0,
                       epoch = Epoch,
                       nodes = Nodes,
                       target = _} = Stream0) ->
    case maps:is_key(Node, Members0) of
        true ->
            Stream0;
        false ->
            Members1 = Members0#{Node => #member{role = {replica, Epoch},
                                                 node = Node,
                                                 target = stopped}},
            Members = set_running_to_stopped(Members1),
            Stream0#stream{members = Members,
                           nodes = lists:sort([Node | Nodes])}
    end;
update_stream0(#{system_time := _Ts} = _Meta,
               {delete_replica, _StreamId, #{node := Node}},
               #stream{members = Members0,
                       epoch = _Epoch,
                       nodes = Nodes,
                       target = _} = Stream0) ->
    case maps:is_key(Node, Members0) of
        true ->
            %% TODO: check of duplicate
            Members = maps:map(
                        fun (K, M) when K == Node ->
                                M#member{target = deleted};
                            (_, #member{target = running} = M) ->
                                M#member{target = stopped};
                            (_, M) ->
                                M
                        end, Members0),
            Stream0#stream{members = Members,
                           nodes = lists:delete(Node, Nodes)};
        false ->
            Stream0
    end;
update_stream0(#{system_time := _Ts},
               {member_started, _StreamId,
                #{epoch := E,
                  index := Idx,
                  pid := Pid} = Args}, #stream{epoch = E,
                                               members = Members} = Stream0) ->
    Node = node(Pid),
    case maps:get(Node, Members, undefined) of
        #member{role = {_, E},
                current = {starting, Idx},
                state = _} = Member0 ->
            %% this is what we expect, leader epoch should match overall
            %% epoch
            Member = Member0#member{state = {running, E, Pid},
                                    current = undefined},
            %% TODO: we need to tell the machine to monitor the leader
            Stream0#stream{members =
                           Members#{Node => Member}};
        Member ->
            %% do we just ignore any members started events from unexpected
            %% epochs?
            rabbit_log:warning("~s: member started unexpected ~w ~w",
                               [?MODULE, Args, Member]),
            Stream0
    end;
update_stream0(#{system_time := _Ts},
               {member_deleted, _StreamId, #{node := Node}},
               #stream{nodes = Nodes,
                       members = Members0} = Stream0) ->
    case maps:take(Node, Members0) of
        {_, Members} when map_size(Members) == 0 ->
            undefined;
        {#member{state = _}, Members} ->
            %% this is what we expect, leader epoch should match overall
            %% epoch
            Stream0#stream{nodes = lists:delete(Node, Nodes),
                           members = Members};
        _ ->
            %% do we just ignore any writer_started events from unexpected
            %% epochs?
            Stream0
    end;
update_stream0(#{system_time := _Ts},
               {member_stopped, _StreamId,
                #{node := Node,
                  index := Idx,
                  epoch := StoppedEpoch,
                  tail := Tail}}, #stream{epoch = Epoch,
                                          target = Target,
                                          nodes = Nodes,
                                          members = Members0} = Stream0) ->
    IsLeaderInCurrent = case find_leader(Members0) of
                            {#member{role = {writer, Epoch},
                                     target = running,
                                     state = {ready, Epoch}}, _} ->
                                true;
                            {#member{role = {writer, Epoch},
                                     target = running,
                                     state = {running, Epoch, _}}, _} ->
                                true;
                            _ ->
                                false
                        end,
    case maps:get(Node, Members0) of
        #member{role = {replica, Epoch},
                current = {stopping, Idx},
                state = _} = Member0
          when IsLeaderInCurrent ->
            %% A leader has already been selected so skip straight to ready state
            Member = update_target(Member0#member{state = {ready, Epoch},
                                                  current = undefined}, Target),
            Members1 = Members0#{Node => Member},
            Stream0#stream{members = Members1};
        #member{role = {_, Epoch},
                current = {stopping, Idx},
                state = _} = Member0 ->
            %% this is what we expect, member epoch should match overall
            %% epoch
            Member = case StoppedEpoch of
                         Epoch ->
                             update_target(Member0#member{state = {stopped, StoppedEpoch, Tail},
                                                          current = undefined}, Target);
                         _ ->
                             %% if stopped epoch is from another epoch
                             %% leave target as is to retry stop in current term
                             Member0#member{state = {stopped, StoppedEpoch, Tail},
                                            current = undefined}
                     end,

            Members1 = Members0#{Node => Member},

            Offsets = [{N, T}
                       || #member{state = {stopped, E, T},
                                  target = running,
                                  node = N} <- maps:values(Members1),
                          E == Epoch],
            case is_quorum(length(Nodes), length(Offsets)) of
                true ->
                    %% select leader
                    NewWriterNode = select_leader(Offsets),
                    NextEpoch = Epoch + 1,
                    Members = maps:map(
                                fun (N, #member{state = {stopped, E, _}} = M)
                                      when E == Epoch ->
                                        case NewWriterNode of
                                            N ->
                                                %% new leader
                                                M#member{role = {writer, NextEpoch},
                                                         state = {ready, NextEpoch}};
                                            _ ->
                                                M#member{role = {replica, NextEpoch},
                                                         state = {ready, NextEpoch}}
                                        end;
                                    (_N, #member{target = deleted} = M) ->
                                        M;
                                    (_N, M) ->
                                        M#member{role = {replica, NextEpoch}}
                                end, Members1),
                    Stream0#stream{epoch = NextEpoch,
                                   members = Members};
                false ->
                    Stream0#stream{members = Members1}
            end;
        _Member ->
            Stream0
    end;
update_stream0(#{system_time := _Ts},
               {mnesia_updated, _StreamId, #{epoch := E}},
               Stream0) ->
    %% reset mnesia state
    case Stream0 of
        undefined ->
            undefined;
        _ ->
            Stream0#stream{mnesia = {updated, E}}
    end;
update_stream0(#{system_time := _Ts},
               {retention_updated, _StreamId, #{node := Node}},
               #stream{members = Members0,
                       conf = Conf} = Stream0) ->
    Members = maps:update_with(Node, fun (M) ->
                                             M#member{current = undefined,
                                                      conf = Conf}
                                     end, Members0),
    Stream0#stream{members = Members};
update_stream0(#{system_time := _Ts},
               {action_failed, _StreamId, #{action := updating_mnesia}},
               #stream{mnesia = {_, E}} = Stream0) ->
    Stream0#stream{mnesia = {updated, E}};
update_stream0(#{system_time := _Ts},
               {action_failed, _StreamId,
                #{node := Node,
                  index := Idx,
                  action := Action,
                  epoch := _Epoch}}, #stream{members = Members0} = Stream0) ->
    Members1 = maps:update_with(Node,
                                fun (#member{current = {C, I}} = M)
                                      when C == Action andalso I == Idx ->
                                        M#member{current = undefined};
                                    (M) ->
                                        M
                                end, Members0),
    case Members0 of
        #{Node := #member{role = {writer, E},
                          state = {ready, E},
                          current = {starting, Idx}}}
          when Action == starting ->
            %% the leader failed to start = we need a new election
            %% stop all members
            Members = set_running_to_stopped(Members1),
            Stream0#stream{members = Members};
        _ ->
            Stream0#stream{members = Members1}
    end;
update_stream0(#{system_time := _Ts},
               {down, Pid, Reason},
               #stream{epoch = E,
                       members = Members0} = Stream0) ->
    DownNode = node(Pid),
    case Members0 of
        #{DownNode := #member{role = {writer, E},
                              state = {running, E, Pid}} = Member} ->
            Members1 = Members0#{DownNode => Member#member{state = {down, E}}},
            %% leader is down, set all members that should be running to stopped
            Members = maps:map(fun (_, #member{target = running} = M) ->
                                       M#member{target = stopped};
                                   (_, M) ->
                                       M
                               end, Members1),
            Stream0#stream{members = Members};
        #{DownNode := #member{role = {replica, _},
                              state = {running, _, Pid}} = Member}
          when Reason == noconnection ->
            %% mark process as disconnected such that we don't set it to down until
            %% the node is back and we can re-monitor
            Members = Members0#{DownNode =>
                                Member#member{state = {disconnected, E, Pid}}},
            Stream0#stream{members = Members};
        #{DownNode := #member{role = {replica, _},
                              state = {S, _, Pid}} = Member}
          when S == running orelse S == disconnected ->
            %% the down process is currently running with the correct Pid
            %% set state to down
            Members = Members0#{DownNode => Member#member{state = {down, E}}},
            Stream0#stream{members = Members};
        _ ->
            Stream0
    end;
update_stream0(#{system_time := _Ts},
               {down, _Pid, _Reason}, undefined) ->
    undefined;
update_stream0(#{system_time := _Ts} = _Meta,
               {nodeup, Node},
               #stream{members = Members0} = Stream0) ->
    Members = maps:map(
                fun (_, #member{node = N,
                                current = {sleeping, nodeup}} = M)
                      when N == Node ->
                        M#member{current = undefined};
                    (_, M) ->
                        M
                end, Members0),
    Stream0#stream{members = Members};
update_stream0(#{system_time := _Ts},
               {policy_changed, _StreamId, #{queue := Q}},
               #stream{conf = Conf0,
                       members = _Members0} = Stream0) ->
    Conf = rabbit_stream_queue:update_stream_conf(Q, Conf0),
    Stream0#stream{conf = Conf};
update_stream0(_Meta, _Cmd, undefined) ->
    undefined.

eval_listeners(#stream{listeners = Listeners0,
                       queue_ref = QRef,
                       members = Members} = Stream, Effects0) ->
    case find_leader(Members) of
        {#member{state = {running, _, LeaderPid}}, _} ->
            %% a leader is running, check all listeners to see if any of them
            %% has not been notified of the current leader pid
            {Listeners, Effects} =
                maps:fold(
                  fun(_, P, Acc) when P == LeaderPid ->
                          Acc;
                     (LPid, _, {L, Acc}) ->
                          {L#{LPid => LeaderPid},
                           [{send_msg, LPid,
                             {queue_event, QRef,
                              {stream_leader_change, LeaderPid}},
                             cast} | Acc]}
                  end, {Listeners0, Effects0}, Listeners0),
            {Stream#stream{listeners = Listeners}, Effects};
        _ ->
            {Stream, Effects0}
    end.

eval_retention(#{index := Idx} = Meta,
               #stream{conf = #{retention := Ret} = Conf,
                       id = StreamId,
                       epoch = Epoch,
                       members = Members} = Stream, Effects0) ->
    NeedUpdate = maps:filter(
                   fun (_, #member{state = {running, _, _},
                                   current = undefined,
                                   conf = C}) ->
                           Ret =/= maps:get(retention, C, undefined);
                       (_, _) ->
                           false
                   end, Members),
    Args = Meta#{epoch => Epoch},
    Effs = [{aux, {update_retention, StreamId,
                   Args#{pid => Pid,
                         node => node(Pid),
                         retention => Ret}, Conf}}
            || #member{state = {running, _, Pid}} <- maps:values(NeedUpdate)],
    Updated = maps:map(fun (_, M) -> M#member{current = {updating, Idx}} end,
                       NeedUpdate),
    {Stream#stream{members = maps:merge(Members, Updated)}, Effs ++ Effects0}.


%% this function should be idempotent,
%% it should modify the state such that it won't issue duplicate
%% actions when called again
evaluate_stream(#{index := Idx} = Meta,
                #stream{id = StreamId,
                        reply_to = From,
                        epoch = Epoch,
                        mnesia = {MnesiaTag, MnesiaEpoch},
                        members = Members0} = Stream0, Effs0) ->
     case find_leader(Members0) of
         {#member{state = LState,
                  node = LeaderNode,
                  target = deleted,
                  current = undefined} = Writer0, Replicas}
           when LState =/= deleted ->
             Action = {aux, {delete_member, StreamId, LeaderNode,
                             make_writer_conf(Writer0, Stream0)}},
             Writer = Writer0#member{current = {deleting, Idx}},
             Effs = case From of
                        undefined ->
                            [Action | Effs0];
                        _ ->
                            wrap_reply(From, {ok, 0}) ++ [Action | Effs0]
                    end,
             Stream = Stream0#stream{reply_to = undefined},
             eval_replicas(Meta, Writer, Replicas, Stream, Effs);
         {#member{state = {down, Epoch},
                  target = stopped,
                  node = LeaderNode,
                  current = undefined} = Writer0, Replicas} ->
             %% leader is down - all replicas need to be stopped
             %% and tail infos retrieved
             %% some replicas may already be in stopping or ready state
             Args = Meta#{epoch => Epoch,
                          node => LeaderNode},
             Conf = make_writer_conf(Writer0, Stream0),
             Action = {aux, {stop, StreamId, Args, Conf}},
             Writer = Writer0#member{current = {stopping, Idx}},
             eval_replicas(Meta, Writer, Replicas, Stream0, [Action | Effs0]);
         {#member{state = {ready, Epoch}, %% writer ready in current epoch
                  target = running,
                  node = LeaderNode,
                  current = undefined} = Writer0, _Replicas} ->
             %% ready check has been completed and a new leader has been chosen
             %% time to start writer,
             %% if leader start fails, revert back to down state for all and re-run
             WConf = make_writer_conf(Writer0, Stream0),
             Members = Members0#{LeaderNode =>
                                 Writer0#member{current = {starting, Idx},
                                                conf = WConf}},
             Args = Meta#{node => LeaderNode, epoch => Epoch},
             Actions = [{aux, {start_writer, StreamId, Args, WConf}} | Effs0],
             {Stream0#stream{members = Members}, Actions};
         {#member{state = {running, Epoch, LeaderPid},
                  target = running} = Writer, Replicas} ->
             Effs1 = case From of
                         undefined ->
                             Effs0;
                         _ ->
                             %% we need a reply effect here
                             wrap_reply(From, {ok, LeaderPid}) ++ Effs0
                     end,
             Stream1 = Stream0#stream{reply_to = undefined},
             case MnesiaTag == updated andalso MnesiaEpoch < Epoch of
                 true ->
                     Args = Meta#{node => node(LeaderPid), epoch => Epoch},
                     Effs = [{aux,
                              {update_mnesia, StreamId, Args,
                               make_replica_conf(LeaderPid, Stream1)}} | Effs1],
                     Stream = Stream1#stream{mnesia = {updating, MnesiaEpoch}},
                     eval_replicas(Meta, Writer, Replicas, Stream, Effs);
                 false ->
                     eval_replicas(Meta, Writer, Replicas, Stream1, Effs1)
             end;
         {#member{state = S,
                  target = stopped,
                  node = LeaderNode,
                  current = undefined} = Writer0, Replicas}
           when element(1, S) =/= stopped ->
             %% leader should be stopped
             Args = Meta#{node => LeaderNode, epoch => Epoch},
             Action = {aux, {stop, StreamId, Args,
                             make_writer_conf(Writer0, Stream0)}},
             Writer = Writer0#member{current = {stopping, Idx}},
             eval_replicas(Meta, Writer, Replicas, Stream0, [Action | Effs0]);
         {Writer, Replicas} ->
             eval_replicas(Meta, Writer, Replicas, Stream0, Effs0)
     end.

eval_replicas(Meta, undefined, Replicas, Stream, Actions0) ->
    {Members, Actions} = lists:foldl(
                           fun (R, Acc) ->
                                   eval_replica(Meta, R, deleted, Stream, Acc)
                           end, {#{}, Actions0},
                           Replicas),
    {Stream#stream{members = Members}, Actions};
eval_replicas(Meta, #member{state = LeaderState,
                            node = WriterNode} = Writer, Replicas,
              Stream, Actions0) ->
    {Members, Actions} = lists:foldl(
                           fun (R, Acc) ->
                                   eval_replica(Meta, R, LeaderState,
                                                Stream, Acc)
                           end, {#{WriterNode => Writer}, Actions0},
                           Replicas),
    {Stream#stream{members = Members}, Actions}.

eval_replica(#{index := Idx} = Meta,
             #member{state = _State,
                     target = stopped,
                     node = Node,
                     current = undefined} = Replica,
             _LeaderState,
             #stream{id = StreamId,
                     epoch = Epoch,
                     conf = Conf0},
             {Replicas, Actions}) ->
    %% if we're not running anything and we aren't stopped and not caught
    %% by previous clauses we probably should stop
    Args = Meta#{node => Node, epoch => Epoch},

    Conf = Conf0#{epoch => Epoch},
    {Replicas#{Node => Replica#member{current = {stopping, Idx}}},
     [{aux, {stop, StreamId, Args, Conf}} | Actions]};
eval_replica(#{index := Idx} = Meta, #member{state = _,
                                             node = Node,
                                             current = Current,
                                             target = deleted} = Replica,
             _LeaderState, #stream{id = StreamId,
                                   epoch = Epoch,
                                   conf = Conf}, {Replicas, Actions0}) ->

    case Current of
        undefined ->
            Args = Meta#{epoch => Epoch, node => Node},
            Actions = [{aux, {delete_member, StreamId, Args, Conf}} |
                       Actions0],
            {Replicas#{Node => Replica#member{current = {deleting, Idx}}},
             Actions};
        _ ->
            {Replicas#{Node => Replica}, Actions0}
    end;
eval_replica(#{index := Idx} = Meta, #member{state = {State, Epoch},
                                             node = Node,
                                             target = running,
                                             current = undefined} = Replica,
             {running, Epoch, Pid},
             #stream{id = StreamId,
                     epoch = Epoch} = Stream,
             {Replicas, Actions})
  when State == ready; State == down ->
    %% replica is down or ready and the leader is running
    %% time to start it
    Conf = make_replica_conf(Pid, Stream),
    Args = Meta#{node => Node, epoch => Epoch},
    {Replicas#{Node => Replica#member{current = {starting, Idx},
                                      conf = Conf}},
     [{aux, {start_replica, StreamId, Args, Conf}} | Actions]};
eval_replica(_Meta, #member{state = {running, Epoch, _},
                            target = running,
                            node = Node} = Replica,
             {running, Epoch, _}, _Stream, {Replicas, Actions}) ->
    {Replicas#{Node => Replica}, Actions};
eval_replica(_Meta, #member{state = {stopped, _E, _},
                            node = Node,
                            current = undefined} = Replica,
             _LeaderState, _Stream,
             {Replicas, Actions}) ->
    %%  if stopped we should just wait for a quorum to reach stopped and
    %%  update_stream will move to ready state
    {Replicas#{Node => Replica}, Actions};
eval_replica(_Meta, #member{state = {ready, E},
                            target = running,
                            node = Node,
                            current = undefined} = Replica,
             {ready, E}, _Stream,
             {Replicas, Actions}) ->
    %% if we're ready and so is the leader we just wait a swell
    {Replicas#{Node => Replica}, Actions};
eval_replica(_Meta, #member{node = Node} = Replica, _LeaderState, _Stream,
             {Replicas, Actions}) ->
    {Replicas#{Node => Replica}, Actions}.

fail_active_actions(Streams, Exclude) ->
    maps:map(
      fun (_,  #stream{id = Id, members = Members})
            when not is_map_key(Id, Exclude)  ->
              _ = maps:map(fun(_, M) ->
                                   fail_action(Id, M)
                           end, Members)
      end, Streams),

    ok.

fail_action(_StreamId, #member{current = undefined}) ->
    ok;
fail_action(StreamId, #member{role = {_, E},
                              current = {Action, Idx},
                              node = Node}) ->
    rabbit_log:debug("~s: failing active action for ~s node ~w Action ~w",
                     [?MODULE, StreamId, Node, Action]),
    %% if we have an action send failure message
    send_self_command({action_failed, StreamId,
                       #{action => Action,
                         index => Idx,
                         node => Node,
                         epoch => E}}).

ensure_monitors(#stream{id = StreamId,
                        members = Members}, Monitors, Effects) ->
    maps:fold(
      fun
          (_, #member{state = {running, _, Pid}}, {M, E})
            when not is_map_key(Pid, M) ->
              {M#{Pid => {StreamId, member}},
               [{monitor, process, Pid},
                %% ensure we're always monitoring the node as well
                {monitor, node, node(Pid)} | E]};
          (_, _, Acc) ->
              Acc
      end, {Monitors, Effects}, Members).

make_replica_conf(LeaderPid,
                  #stream{epoch = Epoch,
                          nodes = Nodes,
                          conf = Conf}) ->
    LeaderNode = node(LeaderPid),
    Conf#{leader_node => LeaderNode,
          nodes => Nodes,
          leader_pid => LeaderPid,
          replica_nodes => lists:delete(LeaderNode, Nodes),
          epoch => Epoch}.

make_writer_conf(#member{node = Node}, #stream{epoch = Epoch,
                                               nodes = Nodes,
                                               conf = Conf}) ->
    Conf#{leader_node => Node,
          nodes => Nodes,
          replica_nodes => lists:delete(Node, Nodes),
          epoch => Epoch}.


find_leader(Members) ->
    case lists:partition(
           fun (#member{target = deleted}) ->
                   false;
               (#member{role = {Role, _}}) ->
                   Role == writer
           end, maps:values(Members)) of
        {[Writer], Replicas} ->
            {Writer, Replicas};
        {[], Replicas} ->
            {undefined, Replicas}
    end.

select_leader(Offsets) ->
    [{Node, _} | _] = lists:sort(fun({_, {Ao, E}}, {_, {Bo, E}}) ->
                                         Ao >= Bo;
                                    ({_, {_, Ae}}, {_, {_, Be}}) ->
                                         Ae >= Be;
                                    ({_, empty}, _) ->
                                         false;
                                    (_, {_, empty}) ->
                                         true
                                 end, Offsets),
    Node.

maybe_sleep({{nodedown, _}, _}) ->
    timer:sleep(10000);
maybe_sleep({noproc, _}) ->
    timer:sleep(5000);
maybe_sleep({error, nodedown}) ->
    timer:sleep(5000);
maybe_sleep({error, _}) ->
    timer:sleep(5000);
maybe_sleep(_) ->
    ok.

set_running_to_stopped(Members) ->
    maps:map(fun (_, #member{target = running} = M) ->
                     M#member{target = stopped};
                 (_, M) ->
                     M
             end, Members).

update_target(#member{target = deleted} = Member, _) ->
    %% A deleted member can never transition to another state
    Member;
update_target(Member, Target) ->
    Member#member{target = Target}.
