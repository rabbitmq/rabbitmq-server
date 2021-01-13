%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at https://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% Copyright (c) 2012-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%
-module(rabbit_stream_coordinator).

-behaviour(ra_machine).

-export([start/0]).
-export([format_ra_event/2]).

-export([init/1,
         apply/3,
         state_enter/2,
         init_aux/1,
         handle_aux/6,
         tick/2]).

-export([recover/0,
         start_cluster/1,
         add_replica/2,
         delete_replica/2,
         register_listener/1]).

-export([new_stream/2,
         delete_stream/2]).

-export([policy_changed/1]).

-export([local_pid/1]).
-export([query_local_pid/3]).


-export([log_overview/1]).
-export([replay/1]).

-include_lib("rabbit_common/include/rabbit.hrl").
-include("amqqueue.hrl").

-define(STREAM_COORDINATOR_STARTUP, {stream_coordinator_startup, self()}).
-define(TICK_TIMEOUT, 60000).
-define(RESTART_TIMEOUT, 1000).
-define(PHASE_RETRY_TIMEOUT, 10000).
-define(CMD_TIMEOUT, 30000).

-type stream_id() :: string().
-type stream() :: #{conf := osiris:config(),
                    atom() => term()}.
-type stream_role() :: leader | follower | listener.
-type queue_ref() :: rabbit_types:r(queue).
-type tail() :: {osiris:offset(), osiris:epoch()} | empty.

-record(member,
        {state = {down, 0} :: {down, osiris:epoch()}
                              | {stopped, osiris:epoch(), tail()}
                              | {ready, osiris:epoch()}
                              %% when a replica disconnects
                              | {running | disconnected, osiris:epoch(), pid()}
                              | deleted,
         role :: {writer | replica, osiris:epoch()},
         node :: node(),
         %% the currently running action, if any
         current :: undefined |
                    {updating |
                     stopping |
                     starting |
                     deleting, ra:index()} |
                    {sleeping, nodeup | non_neg_integer()},
         % current_ts :: integer(),
         %% record the "current" config used
         conf :: undefined | osiris:config(),
         target = running :: running | stopped | deleted}).

%% member lifecycle
%% down -> stopped(tail) -> running | disconnected -> deleted
%%
%% split the handling of incoming events (down, success | fail of operations)
%% and the actioning of current state (i.e. member A is down but the cluster target
%% is `up` - start a current action to turn member A -> running

-type from() :: {pid(), reference()}.

-record(stream, {id :: stream_id(),
                 epoch = 0 :: osiris:epoch(),
                 queue_ref :: queue_ref(),
                 conf :: osiris:config(),
                 nodes :: [node()],
                 members = #{} :: #{node() := #member{}},
                 listeners = #{} :: #{pid() := LeaderPid :: pid()},
                 reply_to :: undefined | from(),
                 mnesia = {updated, 0} :: {updated | updating, osiris:epoch()},
                 target = running :: running | deleted
                }).

-record(?MODULE, {streams = #{} :: #{stream_id() => #stream{}},
                  monitors = #{} :: #{pid() => {stream_id(), stream_role()}},
                  listeners = #{} :: #{stream_id() =>
                                       #{pid() := queue_ref()}},
                  %% future extensibility
                  reserved_1,
                  reserved_2}).

-type state() :: #?MODULE{}.
-type command() :: {policy_changed, #{stream_id := stream_id()}} |
                   {start_cluster, #{queue := amqqueue:amqqueue()}} |
                   {start_cluster_reply, amqqueue:amqqueue()} |
                   {start_replica, #{stream_id := stream_id(),
                                     node := node(),
                                     retries := non_neg_integer()}} |
                   {start_replica_failed, #{stream_id := stream_id(),
                                            node := node(),
                                            retries := non_neg_integer()},
                    Reply :: term()} |
                   {start_replica_reply, stream_id(), pid()} |

                   {delete_replica, #{stream_id := stream_id(),
                                      node := node()}} |
                   {start_leader_election, stream_id(), osiris:epoch(),
                    Offsets :: term()} |
                   {leader_elected, stream_id(), NewLeaderPid :: pid()} |
                   {replicas_stopped, stream_id()} |
                   {phase_finished, stream_id(), Reply :: term()} |
                   {stream_updated, stream()} |
                   {register_listener, #{pid := pid(),
                                         stream_id := stream_id(),
                                         queue_ref := queue_ref()}} |
                   ra_machine:effect().


-export_type([command/0]).

start() ->
    Nodes = rabbit_mnesia:cluster_nodes(all),
    ServerId = {?MODULE, node()},
    case ra:restart_server(ServerId) of
        {error, Reason} when Reason == not_started orelse
                             Reason == name_not_registered -> 
            case ra:start_server(make_ra_conf(node(), Nodes)) of
                ok ->
                    global:set_lock(?STREAM_COORDINATOR_STARTUP),
                    case find_members(Nodes) of
                        [] ->
                            %% We're the first (and maybe only) one
                            ra:trigger_election(ServerId);
                        Members ->
                            %% What to do if we get a timeout?
                            {ok, _, _} = ra:add_member(Members, ServerId, 30000)
                    end,
                    global:del_lock(?STREAM_COORDINATOR_STARTUP),
                    _ = ra:members(ServerId),
                    ok;
                Error ->
                    exit(Error)
            end;
        ok ->
            ok;
        Error ->
            exit(Error)
    end.

find_members([]) ->
    [];
find_members([Node | Nodes]) ->
    case ra:members({?MODULE, Node}) of
        {_, Members, _} ->
            Members;
        {error, noproc} ->
            find_members(Nodes);
        {timeout, _} ->
            %% not sure what to do here
            find_members(Nodes)
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
    case process_command({delete_stream, StreamId, #{acting_user => ActingUser}}) of
        {ok, ok, _} ->
            QName = amqqueue:get_name(Q),
              _ = rabbit_amqqueue:internal_delete(QName, ActingUser),
            {ok, {ok, 0}};
        Err ->
            Err
    end.

%% end new api

recover() ->
    ra:restart_server({?MODULE, node()}).

start_cluster(Q) ->
    process_command({start_cluster, #{queue => Q}}).

add_replica(StreamId, Node) ->
    process_command({add_replica, StreamId, #{node => Node}}).

delete_replica(StreamId, Node) ->
    process_command({delete_replica, StreamId, #{node => Node}}).

policy_changed(Q) when ?is_amqqueue(Q) ->
    StreamId = maps:get(name, amqqueue:get_type_state(Q)),
    process_command({policy_changed, StreamId, #{queue => Q}}).

local_pid(StreamId) when is_list(StreamId) ->
    MFA = {?MODULE, query_local_pid, [StreamId, node()]},
    case ra:local_query({?MODULE, node()}, MFA) of
        {ok, {_, Result}, _} ->
            Result;
        {error, _} = Err ->
            Err;
        {timeout, _} ->
            {error, timeout}
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
    global:set_lock(?STREAM_COORDINATOR_STARTUP),
    Servers = ensure_coordinator_started(),
    global:del_lock(?STREAM_COORDINATOR_STARTUP),
    process_command(Servers, Cmd).

process_command([], _Cmd) ->
    {error, coordinator_unavailable};
process_command([Server | Servers], Cmd) ->
    case ra:process_command(Server, Cmd, ?CMD_TIMEOUT) of
        {timeout, _} ->
            rabbit_log:warning("Coordinator timeout on server ~p when processing command ~p",
                               [Server, element(1, Cmd)]),
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
            case ra:restart_server(Local) of
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
            end;
        _ ->
            AllNodes
    end.

start_coordinator_cluster() ->
    Nodes = rabbit_mnesia:cluster_nodes(running),
    rabbit_log:debug("Starting stream coordinator on nodes: ~w", [Nodes]),
    case ra:start_cluster([make_ra_conf(Node, Nodes) || Node <-  Nodes]) of
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
apply(Meta, {down, Pid, Reason} = Cmd,
      #?MODULE{streams = Streams0,
               listeners = Listeners0,
               monitors = Monitors0} = State) ->

    Effects0 = case Reason of
                   noconnection ->
                       [{monitor, node, node(Pid)}];
                   shutdown ->
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
    rabbit_log:debug("rabbit_stream_coordinator: unknown command ~W",
                     [UnkCmd, 10]),
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
state_enter(leader, #?MODULE{monitors = Monitors}) ->
    Pids = maps:keys(Monitors),
    Nodes = maps:from_list([{node(P), ok} || P <- Pids]),
    NodeMons = [{monitor, node, N} || N <- maps:keys(Nodes)],
    NodeMons ++ [{aux, fail_active_actions} |
                 [{monitor, process, P} || P <- Pids]];
state_enter(_S, _) ->
    [].

tick(_Ts, _State) ->
    [{aux, maybe_resize_coordinator_cluster}].

maybe_resize_coordinator_cluster() ->
    spawn(fun() ->
                  rabbit_log:info("resizing coordinator cluster", []),
                  case ra:members({?MODULE, node()}) of
                      {_, Members, _} ->
                          MemberNodes = [Node || {_, Node} <- Members],
                          Running = rabbit_mnesia:cluster_nodes(running),
                          All = rabbit_mnesia:cluster_nodes(all),
                          case Running -- MemberNodes of
                              [] ->
                                  ok;
                              New ->
                                  rabbit_log:warning("New rabbit node(s) detected, "
                                                     "adding stream coordinator in: ~p", [New]),
                                  add_members(Members, New)
                          end,
                          case MemberNodes -- All of
                              [] ->
                                  ok;
                              Old ->
                                  rabbit_log:warning("Rabbit node(s) removed from the cluster, "
                                                     "deleting stream coordinator in: ~p", [Old]),
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
    case ra:start_server(Conf) of
        ok ->
            case ra:add_member(Members, {?MODULE, Node}) of
                {ok, NewMembers, _} ->
                    add_members(NewMembers, Nodes);
                _ ->
                    add_members(Members, Nodes)
            end;
        Error ->
            rabbit_log:warning("Stream coordinator failed to start on node ~p : ~p",
                               [Node, Error]),
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
    % {#{}, undefined}.

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
    ActionFun = fun () -> phase_start_writer(StreamId, Args, Conf) end,
    run_action(starting, StreamId, Args, ActionFun, Aux, LogState);
handle_aux(leader, _, {start_replica, StreamId,
                       #{epoch := Epoch, node := Node} = Args, Conf},
           Aux, LogState, _) ->
    rabbit_log:debug("rabbit_stream_coordinator: running action: 'start_replica'"
                     " for ~s on node ~w in epoch ~b", [StreamId, Node, Epoch]),
    ActionFun = fun () -> phase_start_replica(StreamId, Args, Conf) end,
    run_action(starting, StreamId, Args, ActionFun, Aux, LogState);
handle_aux(leader, _, {stop, StreamId, #{node := Node,
                                         epoch := Epoch} = Args, Conf},
           Aux, LogState, _) ->
    rabbit_log:debug("rabbit_stream_coordinator: running action: 'stop'"
                     " for ~s on node ~w in epoch ~b", [StreamId, Node, Epoch]),
    ActionFun = fun () -> phase_stop_member(StreamId, Args, Conf) end,
    run_action(stopping, StreamId, Args, ActionFun, Aux, LogState);
handle_aux(leader, _, {update_mnesia, StreamId, Args, Conf},
           #aux{actions = _Monitors} = Aux, LogState,
           #?MODULE{streams = _Streams}) ->
    rabbit_log:debug("rabbit_stream_coordinator: running action: 'update_mnesia'"
                     " for ~s", [StreamId]),
    ActionFun = fun () -> phase_update_mnesia(StreamId, Args, Conf) end,
    run_action(updating_mnesia, StreamId, Args, ActionFun, Aux, LogState);
handle_aux(leader, _, {update_retention, StreamId, Args, _Conf},
           #aux{actions = _Monitors} = Aux, LogState,
           #?MODULE{streams = _Streams}) ->
    rabbit_log:debug("rabbit_stream_coordinator: running action: 'update_retention'"
                     " for ~s", [StreamId]),
    ActionFun = fun () -> phase_update_retention(StreamId, Args) end,
    run_action(update_retention, StreamId, Args, ActionFun, Aux, LogState);
handle_aux(leader, _, {delete_member, StreamId, #{node := Node} = Args, Conf},
           #aux{actions = _Monitors} = Aux, LogState,
           #?MODULE{streams = _Streams}) ->
    rabbit_log:debug("rabbit_stream_coordinator: running action: 'delete_member'"
                     " for ~s ~s", [StreamId, Node]),
    ActionFun = fun () -> phase_delete_member(StreamId, Args, Conf) end,
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
            rabbit_log:warning("Error while executing action for stream queue ~s, "
                               " node ~s, epoch ~b Err: ~w",
                               [StreamId, Node, Epoch, Reason]),
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
    Pid = spawn(ActionFun),
    Effects = [],
    Actions = Actions0#{Pid => {StreamId, Action, Args}},
    {no_reply, Aux#aux{actions = Actions}, Log, Effects}.

wrap_reply(From, Reply) ->
    [{reply, From, {wrap_reply, Reply}}].

phase_start_replica(StreamId, #{epoch := Epoch,
                                node := Node} = Args, Conf0) ->
    spawn(
      fun() ->
              try osiris_replica:start(Node, Conf0) of
                  {ok, Pid} ->
                      rabbit_log:debug("~s: ~s: replica started on ~s in ~b",
                                       [?MODULE, StreamId, Node, Epoch]),
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
                      rabbit_log:warning("~s: Error while starting replica for ~s : ~W",
                                         [?MODULE, maps:get(name, Conf0), Reason, 10]),
                      send_action_failed(StreamId, starting, Args)
              catch _:E ->
                        rabbit_log:warning("~s: Error while starting replica for ~s : ~p",
                                           [?MODULE, maps:get(name, Conf0), E]),
                        send_action_failed(StreamId, starting, Args)
              end
      end).

send_action_failed(StreamId, Action, Arg) ->
  send_self_command({action_failed, StreamId, Arg#{action => Action}}).

send_self_command(Cmd) ->
    ra:pipeline_command({?MODULE, node()}, Cmd),
    ok.


phase_delete_member(StreamId, #{node := Node} = Arg, Conf) ->
    spawn(
      fun() ->
              try osiris_server_sup:delete_child(Node, Conf) of
                  ok ->
                      send_self_command({member_deleted, StreamId, Arg});
                  _ ->
                      send_action_failed(StreamId, deleting, Arg)
              catch _:E ->
                      rabbit_log:warning("~s: Error while deleting member for ~s : on node ~s ~p",
                                         [?MODULE, StreamId, Node, E]),
                      send_action_failed(StreamId, deleting, Arg)
              end
      end).

phase_stop_member(StreamId, #{node := Node,
                              epoch := Epoch} = Arg0, Conf) ->
    spawn(
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
                              rabbit_log:warning("Stream coordinator failed to get tail
                                                  of member ~s ~w Error: ~w",
                                                 [StreamId, Node, Err]),
                              send_action_failed(StreamId, stopping, Arg0)
                      catch _:Err ->
                                rabbit_log:warning("Stream coordinator failed to get
                                                  tail of member ~s ~w Error: ~w",
                                                   [StreamId, Node, Err]),
                                send_action_failed(StreamId, stopping, Arg0)

                      end;
                  Err ->
                      rabbit_log:warning("Stream coordinator failed to stop
                                          member ~s ~w Error: ~w",
                                         [StreamId, Node, Err]),
                      send_action_failed(StreamId, stopping, Arg0)
              catch _:Err ->
                        rabbit_log:warning("Stream coordinator failed to stop
                                          member ~s ~w Error: ~w",
                                           [StreamId, Node, Err]),
                        send_action_failed(StreamId, stopping, Arg0)
              end
      end).

phase_start_writer(StreamId, #{epoch := Epoch,
                               node := Node} = Args0, Conf) ->
    spawn(
      fun() ->
              try osiris_writer:start(Conf) of
                  {ok, Pid} ->
                      Args = Args0#{epoch => Epoch, pid => Pid},
                      rabbit_log:warning("~s: started writer ~s on ~w in ~b",
                                         [?MODULE, StreamId, Node, Epoch]),
                      send_self_command({member_started, StreamId, Args});
                  Err ->
                      rabbit_log:warning("~s: failed to start
                                          writer ~s ~w Error: ~w",
                                         [?MODULE, StreamId, Node, Err]),
                        send_action_failed(StreamId, starting, Args0)
              catch _:Err ->
                      rabbit_log:warning("~s: failed to start
                                          writer ~s ~w Error: ~w",
                                         [?MODULE, StreamId, Node, Err]),
                        send_action_failed(StreamId, starting, Args0)

              end
      end).

phase_update_retention(StreamId, #{pid := Pid,
                                   retention := Retention} = Args) ->
    spawn(
      fun() ->
              try osiris:update_retention(Pid, Retention) of
                  ok ->
                      send_self_command({retention_updated, StreamId, Args});
                  {error, Err} ->
                      rabbit_log:warning("~s: failed to update
                                          retention for ~s ~w Error: ~w",
                                         [?MODULE, StreamId, node(Pid), Err]),
                      send_action_failed(StreamId, update_retention, Args)
              catch _:Err ->
                      rabbit_log:warning("~s: failed to update
                                          retention for ~s ~w Error: ~w",
                                         [?MODULE, StreamId, node(Pid), Err]),
                      send_action_failed(StreamId, update_retention, Args)
              end
      end).


get_replica_tail(Node, Conf) ->
    case rpc:call(Node, ?MODULE, log_overview, [Conf]) of
        {badrpc, nodedown} ->
            {error, nodedown};
        {_Range, Offsets} ->
            {ok, select_highest_offset(Offsets)}
    end.

select_highest_offset([]) ->
    empty;
select_highest_offset(Offsets) ->
    lists:last(Offsets).

log_overview(Config) ->
    Dir = osiris_log:directory(Config),
    osiris_log:overview(Dir).


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

    rabbit_log:debug("~s: running mnesia update for ~s: ~W",
                     [?MODULE, StreamId, Conf, 10]),
    Fun = fun (Q) ->
                  amqqueue:set_type_state(amqqueue:set_pid(Q, LeaderPid), Conf)
          end,
    spawn(fun() ->
                  try rabbit_misc:execute_mnesia_transaction(
                         fun() ->
                                 rabbit_amqqueue:update(QName, Fun)
                         end) of
                      not_found ->
                          rabbit_log:debug("~s: mnesia update for ~s: not_found",
                                           [?MODULE, StreamId]),
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
          end).

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


update_stream(#{system_time := _} = Meta,
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
update_stream(#{system_time := _Ts} = _Meta,
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
update_stream(#{system_time := _Ts} = _Meta,
              {add_replica, _StreamId, #{node := Node}},
              #stream{members = Members0,
                      epoch = Epoch,
                      nodes = Nodes,
                      target = _} = Stream0) ->
    case maps:is_key(Node, Members0) of
        true ->
            Stream0;
        false ->
            Members = maps:map(
                        fun (_, M) ->
                                M#member{target = stopped}
                        end, Members0#{Node => #member{role = {replica, Epoch},
                                                       node = Node,
                                                       target = stopped}}),
            Stream0#stream{members = Members,
                           nodes = lists:sort([Node | Nodes])}
    end;
update_stream(#{system_time := _Ts} = _Meta,
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
                            (_, M) ->
                                M#member{target = stopped}
                        end, Members0),
            Stream0#stream{members = Members,
                           nodes = lists:delete(Node, Nodes)};
        false ->
            Stream0
    end;
update_stream(#{system_time := _Ts},
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
update_stream(#{system_time := _Ts},
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
update_stream(#{system_time := _Ts},
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
            Member = Member0#member{state = {ready, Epoch},
                                    target = Target,
                                    current = undefined},
            Members1 = Members0#{Node => Member},
            Stream0#stream{members = Members1};
        #member{role = {_, Epoch},
                current = {stopping, Idx},
                state = _} = Member0 ->
            %% this is what we expect, member epoch should match overall
            %% epoch
            Member = case StoppedEpoch of
                         Epoch ->
                             Member0#member{state = {stopped, StoppedEpoch, Tail},
                                            target = Target,
                                            current = undefined};
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
update_stream(#{system_time := _Ts},
              {mnesia_updated, _StreamId, #{epoch := E}},
              Stream0) ->
    %% reset mnesia state
    case Stream0 of
        undefined ->
            undefined;
        _ ->
            Stream0#stream{mnesia = {updated, E}}
    end;
update_stream(#{system_time := _Ts},
              {retention_updated, _StreamId, #{node := Node}},
              #stream{members = Members0,
                      conf = Conf} = Stream0) ->
    Members = maps:update_with(Node, fun (M) ->
                                             M#member{current = undefined,
                                                      conf = Conf}
                                     end, Members0),
    Stream0#stream{members = Members};
update_stream(#{system_time := _Ts},
              {action_failed, _StreamId, #{action := updating_mnesia}},
              #stream{mnesia = {_, E}} = Stream0) ->
    Stream0#stream{mnesia = {updated, E}};
update_stream(#{system_time := _Ts},
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
            Members = maps:map(fun (_K, M) ->
                                       M#member{target = stopped}
                               end, Members1),
            Stream0#stream{members = Members};
        _ ->
            Stream0#stream{members = Members1}
    end;
update_stream(#{system_time := _Ts},
              {down, Pid, Reason},
              #stream{epoch = E,
                      members = Members0} = Stream0) ->
    DownNode = node(Pid),
    case Members0 of
        #{DownNode := #member{role = {writer, E},
                              state = {running, E, Pid}} = Member} ->
            Members1 = Members0#{DownNode => Member#member{state = {down, E}}},
            %% leader is down, set all members to stop
            Members = maps:map(fun (_, M) ->
                                       M#member{target = stopped}
                               end, Members1),
            Stream0#stream{members = Members};
        #{DownNode := #member{role = {replica, _},
                              state = {running, _, Pid}} = Member}
          when Reason == noconnection orelse
               Reason == shutdown ->
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
update_stream(#{system_time := _Ts},
              {down, _Pid, _Reason}, undefined) ->
    undefined;
update_stream(#{system_time := _Ts} = _Meta,
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
update_stream(#{system_time := _Ts},
              {policy_changed, _StreamId, #{queue := Q}},
              #stream{conf = Conf0,
                      members = _Members0} = Stream0) ->
    Conf = rabbit_stream_queue:update_stream_conf(Q, Conf0),
    Stream0#stream{conf = Conf}.

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
      fun (_, #member{state = {running, _, Pid}}, {M, E})
            when not is_map_key(Pid, M) ->
              {M#{Pid => {StreamId, member}},
               [{monitor, process, Pid} | E]};
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
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

new_stream_test() ->
    [N1, N2, N3] = Nodes = [r@n1, r@n2, r@n3],
    StreamId = atom_to_list(?FUNCTION_NAME),
    Name = list_to_binary(StreamId),
    TypeState = #{name => StreamId,
                  nodes => Nodes},
    Q = new_q(Name, TypeState),
    From = {self(), make_ref()},
    Meta = #{system_time => ?LINE,
             from => From},
    S0 = update_stream(Meta, {new_stream, StreamId,
                              #{leader_node => N1,
                                queue => Q}}, undefined),
    E = 1,
    %% ready means a new leader has been chosen
    %% and the epoch incremented
    ?assertMatch(#stream{nodes = Nodes,
                         members = #{N1 := #member{role = {writer, E},
                                                   current = undefined,
                                                   state = {ready, E}},
                                     N2 := #member{role = {replica, E},
                                                   current = undefined,
                                                   state = {ready, E}},
                                     N3 := #member{role = {replica, E},
                                                   current = undefined,
                                                   state = {ready, E}}}},
                 S0),

    %% we expect the next action to be starting the writer
    Idx1 = ?LINE,
    Meta1 = meta(Idx1),
    {S1, Actions} = evaluate_stream(Meta1, S0, []),
    ?assertMatch([{aux, {start_writer, StreamId,
                         #{node := N1, epoch := E, index := _},
                         #{epoch := E,
                           leader_node := N1,
                           replica_nodes := [N2, N3]}}}],
                 Actions),
    ?assertMatch(#stream{nodes = Nodes,
                         members = #{N1 := #member{role = {writer, E},
                                                   current = {starting, Idx1},
                                                   state = {ready, E}}}},

                 S1),

    E1LeaderPid = fake_pid(N1),
    Idx2 = ?LINE,
    Meta2 = meta(Idx2),
    S2 = update_stream(Meta2, {member_started, StreamId,
                              #{epoch => E,
                                index => Idx1,
                                pid => E1LeaderPid}}, S1),
    ?assertMatch(#stream{nodes = Nodes,
                         epoch = E,
                         members = #{N1 :=
                                     #member{role = {writer, E},
                                             current = undefined,
                                             state = {running, E, E1LeaderPid}}}},
                         S2),
    Idx3 = ?LINE,
    {S3, Actions2} = evaluate_stream(meta(Idx3), S2, []),
    ?assertMatch([{aux, {start_replica, StreamId, #{node := N2},
                         #{epoch := E,
                           leader_pid := E1LeaderPid,
                           leader_node := N1}}},
                  {aux, {start_replica, StreamId, #{node := N3},
                         #{epoch := E,
                           leader_pid := E1LeaderPid,
                           leader_node := N1}}},
                  {aux, {update_mnesia, _, _, _}},
                  %% we reply to the caller once the leader has started
                  {reply, From, {wrap_reply, {ok, E1LeaderPid}}}
                 ], lists:sort(Actions2)),

    ?assertMatch(#stream{nodes = Nodes,
                         members = #{N1 := #member{role = {writer, E},
                                                   current = undefined,
                                                   state = {running, E, E1LeaderPid}},
                                     N2 := #member{role = {replica, E},
                                                   current = {starting, Idx3},
                                                   state = {ready, E}},
                                     N3 := #member{role = {replica, E},
                                                   current = {starting, Idx3},
                                                   state = {ready, E}}}},
                 S3),
    R1Pid = fake_pid(N2),
    S4 = update_stream(Meta, {member_started, StreamId,
                              #{epoch => E, index => Idx3, pid => R1Pid}}, S3),
    {S5, []} = evaluate_stream(meta(?LINE), S4, []),
    R2Pid = fake_pid(N3),
    S6 = update_stream(Meta, {member_started, StreamId,
                              #{epoch => E, index => Idx3, pid => R2Pid}}, S5),
    {S7, []} = evaluate_stream(meta(?LINE), S6, []),
    %% actions should have start_replica requests
    ?assertMatch(#stream{nodes = Nodes,
                         members = #{N1 := #member{role = {writer, E},
                                                   current = undefined,
                                                   state = {running, E, E1LeaderPid}},
                                     N2 := #member{role = {replica, E},
                                                   current = undefined,
                                                   state = {running, E, R1Pid}},
                                     N3 := #member{role = {replica, E},
                                                   current = undefined,
                                                   state = {running, E, R2Pid}}}},
                 S7),

    ok.

leader_down_test() ->
    E = 1,
    StreamId = atom_to_list(?FUNCTION_NAME),
    LeaderPid = fake_pid(n1),
    [Replica1, Replica2] = ReplicaPids = [fake_pid(n2), fake_pid(n3)],
    N1 = node(LeaderPid),
    N2 = node(Replica1),
    N3 = node(Replica2),

    S0 = started_stream(StreamId, LeaderPid, ReplicaPids),
    S1 = update_stream(meta(?LINE), {down, LeaderPid, boom}, S0),
    ?assertMatch(#stream{members = #{N1 := #member{role = {writer, E},
                                                   current = undefined,
                                                   target = stopped,
                                                   state = {down, E}},
                                     N2 := #member{role = {replica, E},
                                                   target = stopped,
                                                   current = undefined,
                                                   state = {running, E, Replica1}},
                                     N3 := #member{role = {replica, E},
                                                   target = stopped,
                                                   current = undefined,
                                                   state = {running, E, Replica2}}}},
                 S1),
    Idx2 = ?LINE,
    {S2, Actions} = evaluate_stream(meta(Idx2), S1, []),
    %% expect all members to be stopping now
    %% replicas will receive downs however as will typically exit if leader does
    %% this is ok
    ?assertMatch(
       [{aux, {stop, StreamId,
               #{node := N1, epoch := E, index := Idx2},
               #{epoch := E}}},
        {aux, {stop, StreamId,
               #{node := N2, epoch := E, index := Idx2},
               #{epoch := E}}},
        {aux, {stop, StreamId,
               #{node := N3, epoch := E, index := Idx2},
               #{epoch := E}}}], lists:sort(Actions)),
    ?assertMatch(#stream{members = #{N1 := #member{role = {writer, E},
                                                   current = {stopping, Idx2},
                                                   state = {down, E}},
                                     N2 := #member{role = {replica, E},
                                                   current = {stopping, Idx2},
                                                   state = {running, E, Replica1}},
                                     N3 := #member{role = {replica, E},
                                                   current = {stopping, Idx2},
                                                   state = {running, E, Replica2}}}},
                 S2),

    %% idempotency check
    {S2, []} = evaluate_stream(meta(?LINE), S2, []),
    N2Tail = {E, 101},
    S3 = update_stream(meta(?LINE), {member_stopped, StreamId,
                                     #{node => N2,
                                       index => Idx2,
                                       epoch => E,
                                       tail => N2Tail}}, S2),
    ?assertMatch(#stream{members = #{N2 := #member{role = {replica, E},
                                                   current = undefined,
                                                   state = {stopped, E, N2Tail}}}},
                 S3),
    {S3, []} = evaluate_stream(meta(?LINE), S3, []),
    N3Tail = {E, 102},
    #{index := Idx4} = Meta4 = meta(?LINE),
    S4 = update_stream(Meta4, {member_stopped, StreamId,
                               #{node => N3,
                                 index => Idx2,
                                 epoch => E,
                                 tail => N3Tail}}, S3),
    E2 = E + 1,
    ?assertMatch(#stream{members = #{N1 := #member{role = {replica, E2},
                                                   current = {stopping, Idx2},
                                                   state = {down, E}},
                                     N2 := #member{role = {replica, E2},
                                                   current = undefined,
                                                   state = {ready, E2}},
                                     %% N3 has the higher offset so should
                                     %% be selected as writer of E2
                                     N3 := #member{role = {writer, E2},
                                                   current = undefined,
                                                   state = {ready, E2}}}},
                 S4),
    {S5, Actions4} = evaluate_stream(Meta4, S4, []),
    %% new leader has been selected so should be started
    ?assertMatch([{aux, {start_writer, StreamId, #{node := N3},
                         #{leader_node := N3}}}],
                 lists:sort(Actions4)),
    ?assertMatch(#stream{epoch = E2}, S5),

    E2LeaderPid = fake_pid(n3),
    #{index := Idx6} = Meta6 = meta(?LINE),
    S6 = update_stream(Meta6, {member_started, StreamId,
                               #{epoch => E2,
                                 index => Idx4,
                                 pid => E2LeaderPid}}, S5),
    ?assertMatch(#stream{members = #{N1 := #member{role = {replica, E2},
                                                   current = {stopping, Idx2},
                                                   state = {down, E}},
                                     N2 := #member{role = {replica, E2},
                                                   current = undefined,
                                                   state = {ready, E2}},
                                     %% N3 has the higher offset so should
                                     %% be selected as writer of E2
                                     N3 := #member{role = {writer, E2},
                                                   current = undefined,
                                                   state = {running, E2, E2LeaderPid}}}},
                 S6),
    {S7, Actions6} = evaluate_stream(Meta6, S6, []),
    ?assertMatch([
                  {aux, {start_replica, StreamId,
                         #{node := N2},
                         #{leader_pid := E2LeaderPid}}},
                  {aux, {update_mnesia, _, _, _}}
                 ],
                 lists:sort(Actions6)),
    ?assertMatch(#stream{members = #{N1 := #member{role = {replica, E2},
                                                   current = {stopping, _},
                                                   state = {down, E}},
                                     N2 := #member{role = {replica, E2},
                                                   current = {starting, Idx6},
                                                   state = {ready, E2}},
                                     N3 := #member{role = {writer, E2},
                                                   current = undefined,
                                                   state = {running, E2, E2LeaderPid}}}},
                 S7),
    E2RepllicaN2Pid = fake_pid(n2),
    S8 = update_stream(meta(?LINE), {member_started, StreamId,
                                     #{epoch => E2,
                                       index => Idx6,
                                       pid => E2RepllicaN2Pid}}, S7),
    ?assertMatch(#stream{members = #{N2 := #member{role = {replica, E2},
                                                   current = undefined,
                                                   state = {running, E2, E2RepllicaN2Pid}}}},
                 S8),
    %% nothing to do
    {S8, []} = evaluate_stream(meta(?LINE), S8, []),

    #{index := Idx9} = Meta9 = meta(?LINE),
    S9 = update_stream(Meta9, {action_failed, StreamId,
                               #{action => stopping,
                                 index => Idx2,
                                 node => N1,
                                 epoch => E}}, S8),
    ?assertMatch(#stream{members = #{N1 := #member{role = {replica, E2},
                                                   current = undefined,
                                                   state = {down, E}}}},
                 S9),

    {S10, Actions9} = evaluate_stream(Meta9, S9, []),
    %% retries action
    ?assertMatch([{aux, {stop, StreamId, #{node := N1, epoch := E2}, _}}],
                 lists:sort(Actions9)),
    ?assertMatch(#stream{members = #{N1 := #member{role = {replica, E2},
                                                   current = {stopping, Idx9},
                                                   state = {down, E}}}},
                 S10),

    %% now finally succeed in stopping the old writer
    N1Tail = {1, 107},
    S11 = update_stream(meta(?LINE),
                        {member_stopped, StreamId, #{node => N1,
                                                     index => Idx9,
                                                     epoch => E2,
                                                     tail => N1Tail}}, S10),
    %% skip straight to ready as cluster is already operative
    ?assertMatch(#stream{members = #{N1 := #member{role = {replica, E2},
                                                   current = undefined,
                                                   state = {ready, E2}}}},
                 S11),

    {S12, Actions11} = evaluate_stream(meta(?LINE), S11, []),
    ?assertMatch([{aux, {start_replica, StreamId, #{node := N1},
                         #{leader_pid := E2LeaderPid}}}],
                 lists:sort(Actions11)),
    ?assertMatch(#stream{members = #{N1 := #member{role = {replica, E2},
                                                   current = {starting, _},
                                                   state = {ready, E2}}}},
                 S12),
    ok.

replica_down_test() ->
    E = 1,
    StreamId = atom_to_list(?FUNCTION_NAME),
    LeaderPid = fake_pid(n1),
    [Replica1, Replica2] = ReplicaPids = [fake_pid(n2), fake_pid(n3)],
    N1 = node(LeaderPid),
    N2 = node(Replica1),
    N3 = node(Replica2),

    S0 = started_stream(StreamId, LeaderPid, ReplicaPids),
    S1 = update_stream(meta(?LINE), {down, Replica1, boom}, S0),
    ?assertMatch(#stream{members = #{N1 := #member{role = {writer, E},
                                                   current = undefined,
                                                   state = {running, E, LeaderPid}},
                                     N2 := #member{role = {replica, E},
                                                   current = undefined,
                                                   state = {down, E}},
                                     N3 := #member{role = {replica, E},
                                                   current = undefined,
                                                   state = {running, E, Replica2}}}},
                 S1),
    {S2, Actions} = evaluate_stream(meta(?LINE), S1, []),
    ?assertMatch([
                  {aux, {start_replica, StreamId, #{node := N2},
                         #{leader_pid := LeaderPid}}}
                 ],
                 lists:sort(Actions)),
    ?assertMatch(#stream{members = #{N2 := #member{role = {replica, E},
                                                   current = {starting, _},
                                                   state = {down, E}}
                                     }},
                 S2),
    ok.

leader_start_failed_test() ->

    %% after a leader is selected we need to handle the case where the leader
    %% start fails
    %% this can happen if a node hosting the leader disconnects then connects
    %% then disconnects again (rabbit seems to do this sometimes).
    E = 1,
    StreamId = atom_to_list(?FUNCTION_NAME),
    LeaderPid = fake_pid(n1),
    [Replica1, Replica2] = ReplicaPids = [fake_pid(n2), fake_pid(n3)],
    N1 = node(LeaderPid),
    N2 = node(Replica1),
    N3 = node(Replica2),

    S0 = started_stream(StreamId, LeaderPid, ReplicaPids),
    Idx2 = ?LINE,
    S1 = update_stream(meta(Idx2), {down, LeaderPid, boom}, S0),
    {S2, _Actions} = evaluate_stream(meta(Idx2), S1, []),
    %% leader was down but a temporary reconnection allowed the stop to complete
    S3 = update_stream(meta(?LINE),
                       {member_stopped, StreamId, #{node => N1,
                                                    index => Idx2,
                                                    epoch => E,
                                                    tail => {1, 2}}}, S2),

    {S3, []} = evaluate_stream(meta(?LINE), S3, []),
    Meta4 = meta(?LINE),
    S4 = update_stream(Meta4,
                       {member_stopped, StreamId, #{node => N2,
                                                    index => Idx2,
                                                    epoch => E,
                                                    tail => {1, 1}}}, S3),
    E2 = E+1,
    {S5, Actions4} = evaluate_stream(Meta4, S4, []),
    ?assertMatch([{aux, {start_writer, StreamId, _,
                         #{epoch := E2,
                           leader_node := N1}}}],
                 lists:sort(Actions4)),
    #{index := Idx4} = Meta4,
    S6 = update_stream(meta(?LINE),
                       {action_failed, StreamId, #{node => N1,
                                                   index => Idx4,
                                                   action => starting,
                                                   epoch => E2}}, S5),
    ?assertMatch(#stream{members = #{N1 := #member{role = {writer, E2},
                                                   current = undefined,
                                                   target = stopped,
                                                   state = {ready, E2}},
                                     N2 := #member{role = {replica, E2},
                                                   target = stopped,
                                                   current = undefined,
                                                   state = {ready, E2}},
                                     N3 := #member{role = {replica, E2},
                                                   target = stopped,
                                                   current = {stopping, _},
                                                   state = {running, E, _}}}},
                 S6),
    % E3 = E2+1,
    Idx7 = ?LINE,
    {S7, Actions6} = evaluate_stream(meta(Idx7), S6, []),
    ?assertMatch([{aux, {stop, StreamId, #{node := N1, epoch := E2}, _}},
                  {aux, {stop, StreamId, #{node := N2, epoch := E2}, _}}
                 ], lists:sort(Actions6)),
    %% late stop from prior epoch - need to run stop again to make sure
    Meta8 = meta(?LINE),
    S8 = update_stream(Meta8,
                       {member_stopped, StreamId, #{node => N3,
                                                    index => Idx2,
                                                    epoch => E,
                                                    tail => {1, 1}}}, S7),
    ?assertMatch(#stream{members = #{N1 := #member{role = {writer, E2},
                                                   current = {stopping, _},
                                                   target = stopped,
                                                   state = {ready, E2}},
                                     N2 := #member{role = {replica, E2},
                                                   target = stopped,
                                                   current = {stopping, _},
                                                   state = {ready, E2}},
                                     N3 := #member{role = {replica, E2},
                                                   target = stopped,
                                                   current = undefined,
                                                   state = {stopped, E, _}}}},
                 S8),
    {_S9, Actions8} = evaluate_stream(Meta8, S8, []),
    ?assertMatch([{aux, {stop, StreamId, #{node := N3, epoch := E2}, _}}
                 ], lists:sort(Actions8)),


    ok.

leader_down_scenario_1_test() ->
    %% leader ended up in a stopped state in epoch 2 but on ereplica was
    %% in ready, 2 and the other down 1

    E = 1,
    StreamId = atom_to_list(?FUNCTION_NAME),
    LeaderPid = fake_pid(n1),
    [Replica1, Replica2] = ReplicaPids = [fake_pid(n2), fake_pid(n3)],
    N1 = node(LeaderPid),
    N2 = node(Replica1),
    N3 = node(Replica2),

    S0 = started_stream(StreamId, LeaderPid, ReplicaPids),
    Idx1 = ?LINE,
    S1 = update_stream(meta(Idx1), {down, LeaderPid, boom}, S0),
    ?assertMatch(#stream{members = #{N1 := #member{role = {writer, E},
                                                   current = undefined,
                                                   state = {down, E}},
                                     N2 := #member{role = {replica, E},
                                                   current = undefined,
                                                   state = {running, E, Replica1}},
                                     N3 := #member{role = {replica, E},
                                                   current = undefined,
                                                   state = {running, E, Replica2}}}},
                 S1),
    {S2, Actions} = evaluate_stream(meta(Idx1), S1, []),
    %% expect all members to be stopping now
    %% replicas will receive downs however as will typically exit if leader does
    %% this is ok
    ?assertMatch([{aux, {stop, StreamId, #{node := N1, epoch := E2}, _}},
                  {aux, {stop, StreamId, #{node := N2, epoch := E2}, _}},
                  {aux, {stop, StreamId, #{node := N3, epoch := E2}, _}}],
                 lists:sort(Actions)),
    ?assertMatch(#stream{members = #{N1 := #member{role = {writer, E},
                                                   current = {stopping, Idx1},
                                                   state = {down, E}},
                                     N2 := #member{role = {replica, E},
                                                   current = {stopping, Idx1},
                                                   state = {running, E, Replica1}},
                                     N3 := #member{role = {replica, E},
                                                   current = {stopping, Idx1},
                                                   state = {running, E, Replica2}}}},
                 S2),

    %% idempotency check
    {S2, []} = evaluate_stream(meta(?LINE), S2, []),
    N2Tail = {E, 101},
    S3 = update_stream(meta(?LINE), {member_stopped, StreamId, #{node => N2,
                                                                 index => Idx1,
                                                                 epoch => E,
                                                                 tail => N2Tail}}, S2),
    ?assertMatch(#stream{members = #{N2 := #member{role = {replica, E},
                                                   current = undefined,
                                                   state = {stopped, E, N2Tail}}}},
                 S3),
    {S3, []} = evaluate_stream(meta(?LINE), S3, []),
    N3Tail = {E, 102},
    Meta4 = meta(?LINE),
    S4 = update_stream(Meta4, {member_stopped, StreamId, #{node => N3,
                                                           index => Idx1,
                                                           epoch => E,
                                                           tail => N3Tail}}, S3),
    E2 = E + 1,
    ?assertMatch(#stream{members = #{N1 := #member{role = {replica, E2},
                                                   current = {stopping, _},
                                                   state = {down, E}},
                                     N2 := #member{role = {replica, E2},
                                                   current = undefined,
                                                   state = {ready, E2}},
                                     %% N3 has the higher offset so should
                                     %% be selected as writer of E2
                                     N3 := #member{role = {writer, E2},
                                                   current = undefined,
                                                   state = {ready, E2}}}},
                 S4),
    {S5, Actions4} = evaluate_stream(Meta4, S4, []),
    %% new leader has been selected so should be started
    ?assertMatch([{aux, {start_writer, StreamId, _Args, #{leader_node := N3}}}],
                  lists:sort(Actions4)),
    ?assertMatch(#stream{epoch = E2}, S5),

    E2LeaderPid = fake_pid(n3),
    Meta6 = meta(?LINE),
    S6 = update_stream(Meta6, {member_started, StreamId,
                               Meta4#{epoch => E2, pid => E2LeaderPid}}, S5),
    ?assertMatch(#stream{members = #{N1 := #member{role = {replica, E2},
                                                   current = {stopping, _},
                                                   state = {down, E}},
                                     N2 := #member{role = {replica, E2},
                                                   current = undefined,
                                                   state = {ready, E2}},
                                     %% N3 has the higher offset so should
                                     %% be selected as writer of E2
                                     N3 := #member{role = {writer, E2},
                                                   current = undefined,
                                                   state = {running, E2, E2LeaderPid}}}},
                 S6),
    {S6b, Actions6} = evaluate_stream(Meta6, S6, []),
    ?assertMatch([
                  {aux, {start_replica, StreamId, #{node := N2}, _}},
                  {aux, {update_mnesia, _, _, _}}
                 ],
                 lists:sort(Actions6)),

    #{index := Idx7} = Meta7 = meta(?LINE),
    S7 = update_stream(Meta7, {down, E2LeaderPid, boom}, S6b),
    {S8, Actions7} = evaluate_stream(Meta7, S7, []),
    ?assertMatch([{aux, {stop, StreamId, #{node := N3, epoch := E2}, _}}],
                 lists:sort(Actions7)),
    ?assertMatch(#stream{members = #{N1 := #member{role = {replica, E2},
                                                   current = {stopping, _},
                                                   state = {down, E}},
                                     N2 := #member{role = {replica, E2},
                                                   current = {starting, _},
                                                   state = {ready, E2}},
                                     N3 := #member{role = {writer, E2},
                                                   current = {stopping, Idx7},
                                                   state = {down, E2}}}},
                 S8),
    %% writer is stopped before the ready replica has been started
    S9 = update_stream(meta(?LINE), {member_stopped, StreamId, #{node => N3,
                                                                 index => Idx7,
                                                                 epoch => E2,
                                                                 tail => N3Tail}},
                       S8),
    ?assertMatch(#stream{members = #{N3 := #member{role = {writer, E2},
                                                   current = undefined,
                                                   state = {stopped, E2, N3Tail}}}},
                 S9),
    {S10, []} = evaluate_stream(meta(?LINE), S9, []),
    #{index := Idx12} = Meta12 = meta(?LINE),
    S11 = update_stream(Meta12, {action_failed, StreamId,
                                      Meta6#{action => starting,
                                             node => N2,
                                             epoch => E2}},
                        S10),
    ?assertMatch(#stream{members = #{N2 := #member{role = {replica, E2},
                                                   current = undefined,
                                                   state = {ready, E2}}}},
                 S11),
    {S12, Actions11} = evaluate_stream(Meta12, S11, []),
    ?assertMatch([{aux, {stop, StreamId, #{node := N2, epoch := E2}, _}}],
                 lists:sort(Actions11)),
    ?assertMatch(#stream{members = #{N2 := #member{role = {replica, E2},
                                                   current = {stopping, Idx12},
                                                   state = {ready, E2}}}},
                 S12),
    S13 = update_stream(meta(?LINE), {member_stopped, StreamId, #{node => N2,
                                                                  index => Idx12,
                                                                  epoch => E2,
                                                                  tail => N2Tail}},
                        S12),
    E3 = E2 + 1,
    ?assertMatch(#stream{members = #{
                                     N1 := #member{role = {replica, E3},
                                                   current = {stopping, Idx1},
                                                   state = {down, E}},
                                     N2 := #member{role = {replica, E3},
                                                   current = undefined,
                                                   state = {ready, E3}},
                                     N3 := #member{role = {writer, E3},
                                                   current = undefined,
                                                   state = {ready, E3}}
                                    }},
                 S13),
    ok.

delete_stream_test() ->
    %% leader ended up in a stopped state in epoch 2 but one replica was
    %% in ready, 2 and the other down 1

    % E = 1,
    StreamId = atom_to_list(?FUNCTION_NAME),
    LeaderPid = fake_pid(n1),
    [Replica1, Replica2] = ReplicaPids = [fake_pid(n2), fake_pid(n3)],
    N1 = node(LeaderPid),
    N2 = node(Replica1),
    N3 = node(Replica2),

    S0 = started_stream(StreamId, LeaderPid, ReplicaPids),
    From = {self(), make_ref()},
    Meta1 = (meta(?LINE))#{from => From},
    S1 = update_stream(Meta1, {delete_stream, StreamId, #{}}, S0),
    ?assertMatch(#stream{target = deleted,
                         members = #{N3 := #member{target = deleted,
                                                   current = undefined,
                                                   state = _},
                                     N2 := #member{target = deleted,
                                                   current = undefined,
                                                   state = _},
                                     N1 := #member{target = deleted,
                                                   current = undefined,
                                                   state = _}
                                    }},
                 S1),
    {S2, Actions1} = evaluate_stream(meta(?LINE), S1, []),
    %% expect all members to be stopping now
    %% replicas will receive downs however as will typically exit if leader does
    %% this is ok
    ?assertMatch([{aux, {delete_member, StreamId, #{node := N1}, _}},
                  {aux, {delete_member, StreamId, #{node := N2}, _}},
                  {aux, {delete_member, StreamId, #{node := N3}, _}}
                  % {reply, From, {wrap_reply, {ok, 0}}}
                 ],
                 lists:sort(Actions1)),
    ?assertMatch(#stream{target = deleted,
                         members = #{N3 := #member{target = deleted,
                                                   current = {deleting, _},
                                                   state = _},
                                     N2 := #member{target = deleted,
                                                   current = {deleting, _},
                                                   state = _},
                                     N1 := #member{target = deleted,
                                                   current = {deleting, _},
                                                   state = _}
                                    }},
                 S2),
    S3 = update_stream(meta(?LINE), {member_deleted, StreamId, #{node => N1}},
                       S2),
    ?assertMatch(#stream{target = deleted,
                         members = #{N2 := _, N3 := _} = M3}
                   when not is_map_key(N1, M3), S3),
    {S4, []} = evaluate_stream(meta(?LINE), S3, []),
    ?assertMatch(#stream{target = deleted,
                         members = #{N2 := _, N3 := _} = M3}
                   when not is_map_key(N1, M3), S4),
    S5 = update_stream(meta(?LINE), {member_deleted, StreamId, #{node => N2}},
                       S4),
    ?assertMatch(#stream{target = deleted,
                         members = #{N3 := _} = M5}
                   when not is_map_key(N2, M5), S5),
    {S6, []} = evaluate_stream(meta(?LINE), S5, []),
    S7 = update_stream(meta(?LINE), {member_deleted, StreamId, #{node => N3}},
                       S6),
    ?assertEqual(undefined, S7),
    ok.

add_replica_test() ->
    E = 1,
    StreamId = atom_to_list(?FUNCTION_NAME),
    LeaderPid = fake_pid(n1),
    [Replica1, Replica2] = [fake_pid(n2), fake_pid(n3)],
    N1 = node(LeaderPid),
    N2 = node(Replica1),
    %% this is to be added
    N3 = node(Replica2),

    S0 = started_stream(StreamId, LeaderPid, [Replica1]),
    From = {self(), make_ref()},
    Meta1 = (meta(?LINE))#{from => From},
    S1 = update_stream(Meta1, {add_replica, StreamId, #{node => N3}}, S0),
    ?assertMatch(#stream{target = running,
                         nodes = [N1, N2, N3],
                         members = #{N1 := #member{target = stopped,
                                                   current = undefined,
                                                   state = {running, _, _}},
                                     N2 := #member{target = stopped,
                                                   current = undefined,
                                                   state = {running, _, _}},
                                     N3 := #member{target = stopped,
                                                   current = undefined,
                                                   state = {down, 0}}
                                    }},
                 S1),
    {S2, Actions1} = evaluate_stream(Meta1, S1, []),
    ?assertMatch([{aux, {stop, StreamId, #{node := N1, epoch := E}, _}},
                  {aux, {stop, StreamId, #{node := N2, epoch := E}, _}},
                  {aux, {stop, StreamId, #{node := N3, epoch := E}, _}}],
                 lists:sort(Actions1)),
    Idx1 = maps:get(index, Meta1),
    ?assertMatch(#stream{target = running,
                         nodes = [N1, N2, N3],
                         members = #{N1 := #member{target = stopped,
                                                   current = {stopping, Idx1},
                                                   state = {running, _, _}},
                                     N2 := #member{target = stopped,
                                                   current = {stopping, Idx1},
                                                   state = {running, _, _}},
                                     N3 := #member{target = stopped,
                                                   current = {stopping, Idx1},
                                                   state = {down, 0}}
                                    }},
                 S2),
    N1Tail = {E, 101},
    S3 = update_stream(meta(?LINE), {member_stopped, StreamId, #{node => N1,
                                                                 index => Idx1,
                                                                 epoch => E,
                                                                 tail => N1Tail}},
                        S2),
    ?assertMatch(#stream{target = running,
                         nodes = [N1, N2, N3],
                         members = #{N1 := #member{target = running,
                                                   current = undefined,
                                                   state = {stopped, _, _}},
                                     N2 := #member{target = stopped,
                                                   current = {stopping, Idx1},
                                                   state = {running, _, _}},
                                     N3 := #member{target = stopped,
                                                   current = {stopping, Idx1},
                                                   state = {down, 0}}
                                    }}, S3),
    {S3, []} = evaluate_stream(meta(?LINE), S3, []),
    N2Tail = {E, 100},
    S4 = update_stream(meta(?LINE), {member_stopped, StreamId, #{node => N2,
                                                                 index => Idx1,
                                                                 epoch => E,
                                                                 tail => N2Tail}},
                        S3),
    E2 = E + 1,
    ?assertMatch(#stream{target = running,
                         nodes = [N1, N2, N3],
                         members = #{N1 := #member{target = running,
                                                   current = undefined,
                                                   state = {ready, E2}},
                                     N2 := #member{target = running,
                                                   current = undefined,
                                                   state = {ready, E2}},
                                     N3 := #member{target = stopped,
                                                   current = {stopping, Idx1},
                                                   state = {down, 0}}
                                    }}, S4),
    Idx3 = ?LINE,
    {S3, []} = evaluate_stream(meta(Idx3), S3, []),
    {S5, Actions4} = evaluate_stream(meta(Idx3), S4, []),
    ?assertMatch([{aux, {start_writer, StreamId, #{index := Idx3},
                         #{leader_node := N1}}}],
                  lists:sort(Actions4)),
    ?assertMatch(#stream{epoch = E2}, S5),
    S6 = update_stream(meta(?LINE), {member_stopped, StreamId, #{node => N3,
                                                                 index => Idx1,
                                                                 epoch => E,
                                                                 tail => empty}},
                        S5),
    ?assertMatch(#stream{target = running,
                         nodes = [N1, N2, N3],
                         members = #{N1 := #member{target = running,
                                                   current = {starting, Idx3},
                                                   role = {writer, _},
                                                   state = {ready, E2}},
                                     N2 := #member{target = running,
                                                   current = undefined,
                                                   state = {ready, E2}},
                                     N3 := #member{target = running,
                                                   current = undefined,
                                                   state = {ready, E2}}
                                    }}, S6),
    ok.

delete_replica_test() ->
    %% TOOD: replica and leader needs to be tested
    E = 1,
    StreamId = atom_to_list(?FUNCTION_NAME),
    LeaderPid = fake_pid(n1),
    [Replica1, Replica2] = [fake_pid(n2), fake_pid(n3)],
    N1 = node(LeaderPid),
    N2 = node(Replica1),
    %% this is to be added
    N3 = node(Replica2),

    S0 = started_stream(StreamId, LeaderPid, [Replica1, Replica2]),
    From = {self(), make_ref()},
    Idx1 = ?LINE,
    Meta1 = (meta(Idx1))#{from => From},
    S1 = update_stream(Meta1, {delete_replica, StreamId, #{node => N3}}, S0),
    ?assertMatch(#stream{target = running,
                         nodes = [N1, N2],
                         members = #{N1 := #member{target = stopped,
                                                   current = undefined,
                                                   state = {running, _, _}},
                                     N2 := #member{target = stopped,
                                                   current = undefined,
                                                   state = {running, _, _}},
                                     N3 := #member{target = deleted,
                                                   current = undefined,
                                                   state = {running, _, _}}
                                    }},
                 S1),
    {S2, Actions1} = evaluate_stream(Meta1, S1, []),
    ?assertMatch([{aux, {delete_member, StreamId, #{node := N3}, _}},
                  {aux, {stop, StreamId, #{node := N1, epoch := E}, _}},
                  {aux, {stop, StreamId, #{node := N2, epoch := E}, _}}],
                 lists:sort(Actions1)),
    S3 = update_stream(meta(?LINE), {member_deleted, StreamId, #{node => N3}},
                       S2),
    ?assertMatch(#stream{target = running,
                         nodes = [N1, N2],
                         members = #{N1 := #member{target = stopped,
                                                   current = {stopping, _},
                                                   state = {running, _, _}},
                                     N2 := #member{target = stopped,
                                                   current = {stopping, _},
                                                   state = {running, _, _}}
                                    } = Members}
                   when not is_map_key(N3, Members), S3),
    {S3, []} = evaluate_stream(meta(?LINE), S3, []),
    S4 = update_stream(meta(?LINE),
                       {member_stopped, StreamId, #{node => N1,
                                                    index => Idx1,
                                                    epoch => E,
                                                    tail => {E, 100}}},
                       S3),
    {S4, []} = evaluate_stream(meta(?LINE), S4, []),
    S5 = update_stream(meta(?LINE),
                       {member_stopped, StreamId, #{node => N2,
                                                    index => Idx1,
                                                    epoch => E,
                                                    tail => {E, 101}}},
                       S4),
    {S6, Actions5} = evaluate_stream(meta(?LINE), S5, []),
    E2 = E + 1,
    ?assertMatch(#stream{target = running,
                         nodes = [N1, N2],
                         members = #{N1 := #member{target = running,
                                                   current = undefined,
                                                   state = {ready, E2}},
                                     N2 := #member{target = running,
                                                   role = {writer, E2},
                                                   current = {starting, _},
                                                   state = {ready, E2}}
                                    }}, S6),
    ?assertMatch([{aux, {start_writer, StreamId, _Args, #{nodes := [N1, N2]}}}
                  ], lists:sort(Actions5)),
    {S4, []} = evaluate_stream(meta(?LINE), S4, []),
    ok.

delete_replica_leader_test() ->
    %% TOOD: replica and leader needs to be tested
    E = 1,
    StreamId = atom_to_list(?FUNCTION_NAME),
    LeaderPid = fake_pid(n1),
    [Replica1, _Replica2] = [fake_pid(n2), fake_pid(n3)],
    N1 = node(LeaderPid),
    N2 = node(Replica1),
    %% this is to be added
    % N3 = node(Replica2),

    S0 = started_stream(StreamId, LeaderPid, [Replica1]),
    From = {self(), make_ref()},
    Meta1 = (meta(?LINE))#{from => From},
    S1 = update_stream(Meta1, {delete_replica, StreamId, #{node => N1}}, S0),
    ?assertMatch(#stream{target = running,
                         nodes = [N2],
                         members = #{N1 := #member{target = deleted,
                                                   current = undefined,
                                                   state = {running, _, _}},
                                     N2 := #member{target = stopped,
                                                   current = undefined,
                                                   state = {running, _, _}}
                                    }},
                 S1),
    Idx2 = ?LINE,
    {S2, Actions1} = evaluate_stream(meta(Idx2), S1, []),
    ?assertMatch([{aux, {delete_member, StreamId, #{node := N1}, _}},
                  {aux, {stop, StreamId, #{node := N2, epoch := E}, _}}],
                 lists:sort(Actions1)),
    S3 = S2,
    Idx4 = ?LINE,
    S4 = update_stream(meta(Idx4),
                       {member_stopped, StreamId, #{node => N2,
                                                    index => Idx2,
                                                    epoch => E,
                                                    tail => {E, 100}}},
                       S3),
    E2 = E+1,
    ?assertMatch(#stream{target = running,
                         nodes = [N2],
                         members = #{N1 := #member{target = deleted,
                                                   current = {deleting, Idx2},
                                                   state = {running, _, _}},
                                     N2 := #member{target = running,
                                                   role = {writer, E2},
                                                   current = undefined,
                                                   state = {ready, E2}}
                                    }},
                 S4),
    ok.

meta(N) when is_integer(N) ->
    #{index => N,
      system_time => N + 1000}.

started_stream(StreamId, LeaderPid, ReplicaPids) ->
    E = 1,
    Nodes = [node(LeaderPid) | [node(P) || P <- ReplicaPids]],
    Conf = #{name => StreamId,
             nodes => Nodes},

    VHost = <<"/">>,
    QName = #resource{kind = queue,
                      name = list_to_binary(StreamId),
                      virtual_host = VHost},
    Members0 = #{node(LeaderPid) => #member{role = {writer, E},
                                            node = node(LeaderPid),
                                            state = {running, E, LeaderPid},
                                            current = undefined}},
    Members = lists:foldl(fun (R, Acc) ->
                                  N = node(R),
                                  Acc#{N => #member{role = {replica, E},
                                                    node = N,
                                                    state = {running, E, R},
                                                    current = undefined}}
                          end, Members0, ReplicaPids),


    #stream{id = StreamId,
            epoch = 1,
            nodes = Nodes,
            queue_ref = QName,
            conf = Conf,
            mnesia = {updated, 1},
            members = Members}.

new_q(Name, TypeState) ->
    VHost = <<"/">>,
    QName = #resource{kind = queue,
                      name = Name,
                      virtual_host = VHost},
    amqqueue:set_type_state(
      amqqueue:new_with_version(amqqueue_v2,
                                QName,
                                none,
                                true,
                                false,
                                none,
                                [],
                                VHost,
                                #{},
                                rabbit_stream_queue), TypeState).

fake_pid(Node) ->
    NodeBin = atom_to_binary(Node),
    ThisNodeSize = size(term_to_binary(node())) + 1,
    Pid = spawn(fun () -> ok end),
    %% drop the local node data from a local pid
    <<Pre:ThisNodeSize/binary, LocalPidData/binary>> = term_to_binary(Pid),
    S = size(NodeBin),
    %% get the encoding type of the pid
    <<_:8, Type:8/unsigned, _/binary>> = Pre,
    %% replace it with the incoming node binary
    Final = <<131, Type, 100, S:16/unsigned, NodeBin/binary, LocalPidData/binary>>,
    binary_to_term(Final).
-endif.
