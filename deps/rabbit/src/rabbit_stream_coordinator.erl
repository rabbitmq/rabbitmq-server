%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.

-module(rabbit_stream_coordinator).

-behaviour(ra_machine).

-export([format_ra_event/2]).

%% machine callbacks
-export([init/1,
         apply/3,
         state_enter/2,
         init_aux/1,
         handle_aux/5,
         tick/2,
         version/0,
         which_module/1,
         overview/1]).

-export([update_config/2,
         policy_changed/1]).

%% coordinator API
-export([process_command/1,
         recover/0,
         stop/0,
         transfer_leadership/1,
         forget_node/1,
         status/0,
         member_overview/0]).

%% stream API
-export([new_stream/2,
         restart_stream/1,
         restart_stream/2,
         delete_stream/2,
         add_replica/2,
         delete_replica/2,
         register_listener/1,
         register_local_member_listener/1]).

-export([local_pid/1,
         writer_pid/1,
         members/1,
         stream_overview/1]).

%% machine queries
-export([query_local_pid/3,
         query_writer_pid/2,
         query_members/2,
         query_stream_overview/2,
         ra_local_query/1]).

-export([log_overview/1,
         key_metrics_rpc/1]).

%% for SAC coordinator
-export([sac_state/1]).

%% for testing and debugging
-export([eval_listeners/3,
         replay/1,
         state/0,
         sac_state/0]).

-import(rabbit_queue_type_util, [erpc_call/5]).

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
-define(V2_OR_MORE(Vsn), Vsn >= 2).
-define(V5_OR_MORE(Vsn), Vsn >= 5).
-define(SAC_V4, rabbit_stream_sac_coordinator_v4).
-define(SAC_CURRENT, rabbit_stream_sac_coordinator).

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
                                         node := node(),
                                         stream_id := stream_id(),
                                         type := leader | local_member}} |
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
                   {sac, rabbit_stream_sac_coordinator:command()} |
                   {machine_version, ra_machine:version(), ra_machine:version()} |
                   ra_machine:builtin_command().

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

%% stop the stream coordinator on the local node
stop() ->
    case erlang:whereis(?MODULE) of
        undefined ->
            ok;
        _Pid ->
            ra:stop_server(?RA_SYSTEM, {?MODULE, node()})
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

restart_stream(QRes) ->
    restart_stream(QRes, #{}).

-spec restart_stream(amqqueue:amqqueue() | rabbit_types:r(queue),
                     #{preferred_leader_node => node()}) ->
    {ok, node()} |
    {error, term()} |
    {timeout, term()}.
restart_stream(QRes, Options)
  when element(1, QRes) == resource ->
    restart_stream(hd(rabbit_amqqueue:lookup_many([QRes])), Options);
restart_stream(Q, Options)
  when ?is_amqqueue(Q) andalso
       ?amqqueue_is_stream(Q) ->
    rabbit_log:info("restarting stream ~s in vhost ~s with options ~p",
                    [maps:get(name, amqqueue:get_type_state(Q)), amqqueue:get_vhost(Q), Options]),
    #{name := StreamId} = amqqueue:get_type_state(Q),
    case process_command({restart_stream, StreamId, Options}) of
        {ok, {ok, LeaderPid}, _} ->
            {ok, node(LeaderPid)};
        Err ->
            Err
    end.

delete_stream(Q, ActingUser)
  when ?is_amqqueue(Q) ->
    #{name := StreamId} = amqqueue:get_type_state(Q),
    case process_command({delete_stream, StreamId, #{}}) of
        {ok, ok, _} ->
            case rabbit_amqqueue:internal_delete(Q, ActingUser) of
                ok ->
                    {ok, {ok, 0}};
                {error, timeout} = Err ->
                    Err
            end;
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
                rabbit_log:info("~ts : adding replica ~ts to ~ts Replication State: ~w",
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
    Config = rabbit_stream_queue:update_stream_conf(Q, #{}),
    case update_config(Q, Config) of
        {ok, ok, _} = Res ->
            Res;
        {error, feature_not_enabled} ->
            %% backwards compatibility
            %% TODO: remove in future
            process_command({policy_changed, StreamId, #{queue => Q}});
        Err ->
            Err
    end.

-spec update_config(amqqueue:amqqueue(), #{atom() => term()}) ->
    {ok, ok, ra:server_id()} | {error, not_supported | term()}.
update_config(Q, Config)
  when ?is_amqqueue(Q) andalso is_map(Config) ->
    %% there are the only a few configuration keys that are safe to
    %% update
    StreamId = maps:get(name, amqqueue:get_type_state(Q)),
    case maps:with([filter_size,
                    retention,
                    writer_mod,
                    replica_mod], Config) of
        Conf when map_size(Conf) > 0 ->
            process_command({update_config, StreamId, Conf});
        _ ->
            {error, no_updatable_keys}
    end.

sac_state(#?MODULE{single_active_consumer = SacState}) ->
    SacState.

%% for debugging
state() ->
    case ra_local_query(fun(State) -> State end) of
        {ok, {_, Res}, _} ->
            Res;
        Any ->
            Any
    end.

%% for debugging
sac_state() ->
    case state() of
        S when is_record(S, ?MODULE) ->
            sac_state(S);
        R ->
            R
    end.


writer_pid(StreamId) when is_list(StreamId) ->
    MFA = {?MODULE, query_writer_pid, [StreamId]},
    query_pid(StreamId, MFA).

-spec local_pid(string()) ->
    {ok, pid()} | {error, not_found | term()}.
local_pid(StreamId) when is_list(StreamId) ->
    MFA = {?MODULE, query_local_pid, [StreamId, node()]},
    query_pid(StreamId, MFA).

query_pid(StreamId, MFA) when is_list(StreamId) ->
    case ra_local_query(MFA) of
        {ok, {_, {ok, Pid}}, _} ->
            case erpc:call(node(Pid), erlang, is_process_alive, [Pid]) of
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


-spec stream_overview(stream_id()) ->
    {ok, #{epoch := osiris:epoch(),
           members := #{node() := #{state := term(),
                                    role := {writer | replica, osiris:epoch()},
                                    current := term(),
                                    target := running | stopped}},
           num_listeners := non_neg_integer(),
           target := running | stopped}} |
    {error, term()}.
stream_overview(StreamId) when is_list(StreamId) ->
    MFA = {?MODULE, query_stream_overview, [StreamId]},
    do_query(MFA).

-spec members(stream_id()) ->
    {ok, #{node() := {pid() | undefined, writer | replica}}} |
    {error, not_found}.
members(StreamId) when is_list(StreamId) ->
    MFA = {?MODULE, query_members, [StreamId]},
    do_query(MFA).

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

query_stream_overview(StreamId, #?MODULE{streams = Streams}) ->
    case Streams of
        #{StreamId := #stream{} = Stream} ->
            {ok, stream_overview0(Stream)};
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

query_writer_pid(StreamId, #?MODULE{streams = Streams}) ->
    case Streams of
        #{StreamId := #stream{members = Members}} ->
            maps:fold(
              fun (_Node, #member{role = {writer, _},
                                  state = {running, _, Pid}}, _Acc) ->
                      {ok, Pid};
                  (_, _, Acc) ->
                      Acc
              end, {error, writer_not_found}, Members);
        _ ->
            {error, stream_not_found}
    end.

do_query(MFA) ->
    case ra_local_query(MFA) of
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

-spec register_listener(amqqueue:amqqueue()) ->
    {error, term()} | {ok, ok | stream_not_found, atom() | {atom(), atom()}}.
register_listener(Q) when ?is_amqqueue(Q)->
    #{name := StreamId} = amqqueue:get_type_state(Q),
    process_command({register_listener,
                     #{pid => self(),
                       stream_id => StreamId}}).

-spec register_local_member_listener(amqqueue:amqqueue()) ->
    {error, term()} | {ok, ok | stream_not_found, atom() | {atom(), atom()}}.
register_local_member_listener(Q) when ?is_amqqueue(Q) ->
    #{name := StreamId} = amqqueue:get_type_state(Q),
    process_command({register_listener,
                     #{pid => self(),
                       node => node(self()),
                       stream_id => StreamId,
                       type => local_member}}).

process_command(Cmd) ->
    Servers = ensure_coordinator_started(),
    process_command(Servers, Cmd).

process_command([], _Cmd) ->
    {error, coordinator_unavailable};
process_command([Server | Servers], Cmd) ->
    case ra:process_command(Server, Cmd, cmd_timeout()) of
        {timeout, _} ->
            CmdLabel = case Cmd of
                           {sac, SacCmd} ->
                               element(1, SacCmd);
                           _ ->
                               element(1, Cmd)
                       end,
            rabbit_log:warning("Coordinator timeout on server ~w when processing command ~W",
                               [element(2, Server), CmdLabel, 10]),
            process_command(Servers, Cmd);
        {error, noproc} ->
            process_command(Servers, Cmd);
        {error, nodedown} ->
            process_command(Servers, Cmd);
        Reply ->
            Reply
    end.

cmd_timeout() ->
    application:get_env(rabbit, stream_cmd_timeout, ?CMD_TIMEOUT).

ensure_coordinator_started() ->
    Local = {?MODULE, node()},
    ExpectedMembers = expected_coord_members(),
    case whereis(?MODULE) of
        undefined ->
            global:set_lock(?STREAM_COORDINATOR_STARTUP),
            Nodes = case ra:restart_server(?RA_SYSTEM, Local) of
                        {error, Reason} when Reason == not_started orelse
                                             Reason == name_not_registered ->
                            OtherNodes = ExpectedMembers -- [Local],
                            %% this could potentially be slow if some expected
                            %% members are on nodes that have recently terminated
                            %% and have left a dangling TCP connection
                            %% I suspect this very rarely happens as the local coordinator
                            %% server is started in recover/0
                            case lists:filter(
                                   fun({_, N}) ->
                                           is_pid(erpc_call(N, erlang,
                                                            whereis, [?MODULE],
                                                            1000))
                                   end, OtherNodes) of
                                [] ->
                                    start_coordinator_cluster();
                                _ ->
                                    OtherNodes
                            end;
                        ok ->
                            %% TODO: it may be better to do a leader call
                            %% here as the local member may not have caught up
                            %% yet
                            locally_known_members();
                        {error, {already_started, _}} ->
                            locally_known_members();
                        _ ->
                            locally_known_members()
                    end,
            global:del_lock(?STREAM_COORDINATOR_STARTUP),
            Nodes;
        _ ->
            locally_known_members()
    end.

locally_known_members() ->
    %% TODO: use ra_leaderboard and fallback if leaderboard not populated
    case ra:members({local, {?MODULE, node()}}) of
        {_, Members, _} ->
            Members;
        Err ->
            exit({error_fetching_locally_known_coordinator_members, Err})
    end.

start_coordinator_cluster() ->
    Nodes = rabbit_nodes:list_reachable(),
    true = Nodes =/= [],

    Versions = [V || {ok, V} <- erpc:multicall(Nodes,
                                               ?MODULE, version, [])],
    MinVersion = lists:min([version() | Versions]),
    rabbit_log:debug("Starting stream coordinator on nodes: ~w, "
                     "initial machine version ~b",
                     [Nodes, MinVersion]),
    case ra:start_cluster(?RA_SYSTEM,
                          [make_ra_conf(Node, Nodes, MinVersion)
                           || Node <- Nodes]) of
        {ok, Started, _} ->
            rabbit_log:debug("Started stream coordinator on ~w", [Started]),
            Started;
        {error, cluster_not_formed} ->
            rabbit_log:warning("Stream coordinator could not be started on nodes ~w",
                               [Nodes]),
            []
    end.

expected_coord_members() ->
    Nodes = rabbit_nodes:list_members(),
    [{?MODULE, Node} || Node <- Nodes].

reachable_coord_members() ->
    Nodes = rabbit_nodes:list_reachable(),
    [{?MODULE, Node} || Node <- Nodes].

version() -> 5.

which_module(_) ->
    ?MODULE.

init(#{machine_version := Vsn}) when ?V5_OR_MORE(Vsn) ->
    #?MODULE{single_active_consumer =
             rabbit_stream_sac_coordinator:init_state()};
init(_) ->
    #?MODULE{single_active_consumer = rabbit_stream_sac_coordinator_v4:init_state()}.

-spec apply(ra_machine:command_meta_data(), command(), state()) ->
    {state(), term(), ra_machine:effects()}.
apply(#{index := _Idx, machine_version := MachineVersion} = Meta0,
      {_CmdTag, StreamId, #{}} = Cmd,
      #?MODULE{streams = Streams0,
               monitors = Monitors0} = State0) ->
    Stream0 = maps:get(StreamId, Streams0, undefined),
    Meta = maps:without([term], Meta0),
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
                           Reply, inform_listeners_eol(MachineVersion, Stream0));
                _ ->
                    {Stream2, Effects0} = evaluate_stream(Meta, Stream1, []),
                    {Stream3, Effects1} = eval_listeners(MachineVersion, Stream2, Stream0, Effects0),
                    {Stream, Effects2} = eval_retention(Meta, Stream3, Effects1),
                    {Monitors, Effects} = ensure_monitors(Stream, Monitors0, Effects2),
                    return(Meta,
                           State0#?MODULE{streams = Streams0#{StreamId => Stream},
                                          monitors = Monitors}, Reply, Effects)
            end;
        Reply ->
            return(Meta, State0, Reply, [])
    end;
apply(Meta, {sac, SacCommand}, #?MODULE{single_active_consumer = SacState0,
                                        monitors = Monitors0} = State0) ->
    Mod = sac_module(Meta),
    {SacState1, Reply, Effects0} = Mod:apply(SacCommand, SacState0),
    {SacState2, Monitors1, Effects1} =
         Mod:ensure_monitors(SacCommand, SacState1, Monitors0, Effects0),
    return(Meta, State0#?MODULE{single_active_consumer = SacState2,
                                monitors = Monitors1}, Reply, Effects1);
apply(#{machine_version := Vsn} = Meta, {down, Pid, Reason} = Cmd,
      #?MODULE{streams = Streams0,
               monitors = Monitors0,
               listeners = StateListeners0,
               single_active_consumer = SacState0 } = State) ->
    Effects0 = case Reason of
                   noconnection ->
                       [{monitor, node, node(Pid)}];
                   _ ->
                       []
               end,
    case maps:take(Pid, Monitors0) of
        {{StreamId, listener}, Monitors} when Vsn < 2 ->
            Listeners = case maps:take(StreamId, StateListeners0) of
                            error ->
                                StateListeners0;
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
        {{PidStreams, listener}, Monitors} when ?V2_OR_MORE(Vsn) ->
            Streams = maps:fold(
                fun(StreamId, _, Acc) ->
                    case Acc of
                        #{StreamId := Stream = #stream{listeners = Listeners0}} ->
                            %% it will be either a leader or member, not very
                            %% generic but it is a lot faster than iterating the
                            %% whole listeners map each time
                            Listeners = maps:remove({Pid, leader},
                                                    maps:remove({Pid, member},
                                                                Listeners0)),
                            Acc#{StreamId => Stream#stream{listeners = Listeners}};
                        _ ->
                            Acc
                    end
                end, Streams0, PidStreams),
            return(Meta, State#?MODULE{streams = Streams,
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
        {sac, Monitors1} ->
            {SacState1, SacEffects} = sac_handle_connection_down(SacState0, Pid,
                                                                 Reason, Vsn),
            return(Meta, State#?MODULE{single_active_consumer = SacState1,
                                       monitors = Monitors1},
                   ok, [Effects0 ++ SacEffects]);
        error ->
            return(Meta, State, ok, Effects0)
    end;
apply(#{machine_version := MachineVersion} = Meta,
      {register_listener, #{pid := Pid,
                            stream_id := StreamId} = Args},
      #?MODULE{streams = Streams,
               monitors = Monitors0} = State0) when MachineVersion =< 1 ->
    Type = maps:get(type, Args, leader),
    case {Streams, Type} of
        {#{StreamId := #stream{listeners = Listeners0} = Stream0}, leader} ->
            Stream1 = Stream0#stream{listeners = maps:put(Pid, undefined, Listeners0)},
            {Stream, Effects} = eval_listeners(MachineVersion, Stream1, []),
            Monitors = maps:put(Pid, {StreamId, listener}, Monitors0),
            return(Meta,
                   State0#?MODULE{streams = maps:put(StreamId, Stream, Streams),
                                  monitors = Monitors}, ok,
                   [{monitor, process, Pid} | Effects]);
        {#{StreamId := _Stream}, local_member} ->
            %% registering a local member listener does not change the state in v1
            return(Meta, State0, ok, []);
        _ ->
            return(Meta, State0, stream_not_found, [])
    end;

apply(#{machine_version := Vsn} = Meta,
      {register_listener, #{pid := Pid,
                            stream_id := StreamId} = Args},
      #?MODULE{streams = Streams,
               monitors = Monitors0} = State0) when ?V2_OR_MORE(Vsn) ->
    Node = maps:get(node, Args, node(Pid)),
    Type = maps:get(type, Args, leader),

    case Streams of
        #{StreamId := #stream{listeners = Listeners0} = Stream0} ->
            {LKey, LValue} =
                case Type of
                    leader ->
                        {{Pid, leader}, undefined};
                    local_member ->
                        {{Pid, member}, {Node, undefined}}
                end,
            {Listeners, Effects} = eval_listener(LKey, LValue, {Listeners0, []}, Stream0),
            Stream = Stream0#stream{listeners = Listeners},
            {PidStreams, listener} = maps:get(Pid, Monitors0, {#{}, listener}),
            Monitors = maps:put(Pid, {PidStreams#{StreamId => ok}, listener}, Monitors0),
            return(Meta,
                   State0#?MODULE{streams = maps:put(StreamId, Stream, Streams),
                                  monitors = Monitors}, ok,
                   [{monitor, process, Pid} | Effects]);
        _ ->
            return(Meta, State0, stream_not_found, [])
    end;
apply(Meta, {nodeup, Node} = Cmd,
      #?MODULE{monitors = Monitors0,
               streams = Streams0,
               single_active_consumer = Sac0} = State)  ->
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
    {Streams, Effects1} =
        maps:fold(fun (Id, S0, {Ss, E0}) ->
                          S1 = update_stream(Meta, Cmd, S0),
                          {S, E} = evaluate_stream(Meta, S1, E0),
                          {Ss#{Id => S}, E}
                  end, {Streams0, Effects0}, Streams0),


    {Sac1, Effects2} = sac_handle_node_reconnected(Meta, Node, Sac0, Effects1),
    return(Meta, State#?MODULE{monitors = Monitors,
                               streams = Streams,
                               single_active_consumer = Sac1}, ok, Effects2);
apply(Meta, {machine_version, From, To}, State0) ->
    rabbit_log:info("Stream coordinator machine version changes from ~tp to ~tp, "
                    ++ "applying incremental upgrade.", [From, To]),
    %% RA applies machine upgrades from any version to any version, e.g. 0 -> 2.
    %% We fill in the gaps here, applying all 1-to-1 machine upgrades.
    {State1, Effects} = lists:foldl(fun(Version, {S0, Eff0}) ->
                                            {S1, Eff1} = machine_version(Version, Version + 1, S0),
                                            {S1, Eff0 ++ Eff1}
                                    end, {State0, []}, lists:seq(From, To - 1)),
    return(Meta, State1, ok, Effects);
apply(Meta, {timeout, {sac, node_disconnected, #{connection_pid := Pid}}},
      #?MODULE{single_active_consumer = SacState0} = State0) ->
    Mod = sac_module(Meta),
    {SacState1, Effects} = Mod:presume_connection_down(Pid, SacState0),
    return(Meta, State0#?MODULE{single_active_consumer = SacState1}, ok,
           Effects);
apply(Meta, UnkCmd, State) ->
    rabbit_log:debug("~ts: unknown command ~W",
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
                             monitors = Monitors,
                             single_active_consumer = SacState}) ->
    Pids = maps:keys(Monitors),
    %% monitor all the known nodes
    Nodes = all_member_nodes(Streams),
    NodeMons = [{monitor, node, N} || N <- Nodes],
    SacEffects = ?SAC_CURRENT:state_enter(leader, SacState),
    SacEffects ++ NodeMons ++ [{aux, fail_active_actions} |
                               [{monitor, process, P} || P <- Pids]];
state_enter(_S, _) ->
    [].

sac_module(#{machine_version := Vsn}) when ?V5_OR_MORE(Vsn) ->
    ?SAC_CURRENT;
sac_module(_) ->
    ?SAC_V4.

all_member_nodes(Streams) ->
    maps:keys(
      maps:fold(
        fun (_, #stream{members = M}, Acc) ->
                maps:merge(Acc, M)
        end, #{}, Streams)).

tick(_Ts, #?MODULE{single_active_consumer = SacState}) ->
    [{aux, maybe_resize_coordinator_cluster} |
     maybe_update_sac_configuration(SacState)].

members() ->
    %% TODO: this can be replaced with a ra_leaderboard
    %% lookup after Ra 2.7.3_
    LocalServerId = {?MODULE, node()},
    case whereis(?MODULE) of
        undefined ->
            %% no local member running, we need to try the cluster
            OtherMembers = lists:delete(LocalServerId, reachable_coord_members()),
            case ra:members(OtherMembers) of
                {_, Members, Leader} ->
                    {ok, Members, Leader};
                Err ->
                    Err
            end;
        _Pid ->
            case ra:members({local, LocalServerId}) of
                {_, Members, Leader} ->
                    {ok, Members, Leader};
                Err ->
                    Err
            end
    end.

maybe_resize_coordinator_cluster(LeaderPid, SacNodes, MachineVersion) ->
    spawn(fun() ->
                  RabbitIsRunning = rabbit:is_running(),
                  case members() of
                      {ok, Members, Leader} when RabbitIsRunning ->
                          MemberNodes = [Node || {_, Node} <- Members],
                          %% TODO: in the future replace with
                          %% rabbit_presence:list_present/0
                          Present = rabbit_nodes:list_running(),
                          RabbitNodes = rabbit_nodes:list_members(),
                          AddableNodes = [N || N <- RabbitNodes,
                                               lists:member(N, Present)],
                          case AddableNodes -- MemberNodes of
                              [] ->
                                  ok;
                              [New | _] ->
                                  %% any remaining members will be added
                                  %% next tick
                                  rabbit_log:info("~ts: New rabbit node(s) detected, "
                                                  "adding : ~w",
                                                  [?MODULE, New]),
                                  add_member(Members, New)
                          end,
                          case MemberNodes -- RabbitNodes of
                              [] ->
                                  ok;
                              [Old | _]  when length(RabbitNodes) > 0 ->
                                  %% this ought to be rather rare as the stream
                                  %% coordinator member is now removed as part
                                  %% of the forget_cluster_node command
                                  rabbit_log:info("~ts: Rabbit node(s) removed "
                                                  "from the cluster, "
                                                  "deleting: ~w", [?MODULE, Old]),
                                  _ = remove_member(Leader, Members, Old),
                                  ok
                          end,
                          maybe_handle_stale_nodes(SacNodes, RabbitNodes,
                                                   LeaderPid,
                                                   MachineVersion);
                      _ ->
                          ok
                  end
          end).

maybe_handle_stale_nodes(SacNodes, BrokerNodes,
                         LeaderPid, Vsn) when ?V5_OR_MORE(Vsn) ->
    case SacNodes -- BrokerNodes of
        [] ->
            ok;
        Stale when length(BrokerNodes) > 0 ->
            rabbit_log:debug("Stale nodes detected in stream SAC "
                             "coordinator: ~w. Purging state.",
                             [Stale]),
            ra:pipeline_command(LeaderPid, sac_make_purge_nodes(Stale)),
            ok;
        _ ->
            ok
    end;
maybe_handle_stale_nodes(_, _, _, _) ->
    ok.

maybe_update_sac_configuration(SacState) ->
    case sac_check_conf_change(SacState) of
        {new, UpdatedConf} ->
            [{append, sac_make_update_conf(UpdatedConf), noreply}];
        _ ->
            []
    end.

add_member(Members, Node) ->
    MinMacVersion = erpc:call(Node, ?MODULE, version, []),
    Conf = make_ra_conf(Node, [N || {_, N} <- Members], MinMacVersion),
    ServerId = {?MODULE, Node},
    case ra:start_server(?RA_SYSTEM, Conf) of
        ok ->
            case ra:add_member(Members, ServerId) of
                {ok, _, _} ->
                    ok;
                {error, Err} ->
                    rabbit_log:warning("~ts: Failed to add member, reason ~w"
                                       "deleting started server on ~w",
                                       [?MODULE, Err, Node]),
                    case ra:force_delete_server(?RA_SYSTEM, ServerId) of
                        ok ->
                            ok;
                        Err ->
                            rabbit_log:warning("~ts: Failed to delete server "
                                               "on ~w, reason ~w",
                                               [?MODULE, Node, Err]),
                            ok
                    end
            end;
        {error, {already_started, _}} ->
            case lists:member(ServerId, Members) of
                true ->
                    %% this feels like an unlikely scenario but best to handle
                    %% it just in case
                    ok;
                false ->
                    %% there is a server running but is not a member of the
                    %% stream coordinator cluster
                    %% In this case it needs to be deleted
                    rabbit_log:warning("~ts: server already running on ~w but not
                                       part of cluster, "
                                       "deleting started server",
                                       [?MODULE, Node]),
                    case ra:force_delete_server(?RA_SYSTEM, ServerId) of
                        ok ->
                            ok;
                        Err ->
                            rabbit_log:warning("~ts: Failed to delete server "
                                               "on ~w, reason ~w",
                                               [?MODULE, Node, Err]),
                            ok
                    end
            end;
        Error ->
            rabbit_log:warning("Stream coordinator server failed to start on node ~ts : ~W",
                               [Node, Error, 10]),
            ok
    end.

remove_member(Leader, Members, Node) ->
    ToRemove = {?MODULE, Node},
    case lists:member(ToRemove, Members) of
        true ->
            ra:leave_and_delete_server(?RA_SYSTEM, Leader, ToRemove);
        false ->
            ok
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
           #aux{resizer = undefined} = Aux, RaAux) ->
    Leader = ra_aux:leader_id(RaAux),
    MachineVersion = ra_aux:effective_machine_version(RaAux),
    SacNodes = sac_list_nodes(ra_aux:machine_state(RaAux), MachineVersion),
    Pid = maybe_resize_coordinator_cluster(Leader, SacNodes, MachineVersion),
    {no_reply, Aux#aux{resizer = Pid}, RaAux, [{monitor, process, aux, Pid}]};
handle_aux(leader, _, maybe_resize_coordinator_cluster,
           AuxState, RaAux) ->
    %% Coordinator resizing is still happening, let's ignore this tick event
    {no_reply, AuxState, RaAux};
handle_aux(leader, _, {down, Pid, _},
           #aux{resizer = Pid} = Aux, RaAux) ->
    %% Coordinator resizing has finished
    {no_reply, Aux#aux{resizer = undefined}, RaAux};
handle_aux(leader, _, {start_writer, StreamId,
                       #{epoch := Epoch, node := Node} = Args, Conf},
           Aux, RaAux) ->
    rabbit_log:debug("~ts: running action: 'start_writer'"
                     " for ~ts on node ~w in epoch ~b",
                     [?MODULE, StreamId, Node, Epoch]),
    ActionFun = phase_start_writer(StreamId, Args, Conf),
    run_action(starting, StreamId, Args, ActionFun, Aux, RaAux);
handle_aux(leader, _, {start_replica, StreamId,
                       #{epoch := Epoch, node := Node} = Args, Conf},
           Aux, RaAux) ->
    rabbit_log:debug("~ts: running action: 'start_replica'"
                     " for ~ts on node ~w in epoch ~b",
                     [?MODULE, StreamId, Node, Epoch]),
    ActionFun = phase_start_replica(StreamId, Args, Conf),
    run_action(starting, StreamId, Args, ActionFun, Aux, RaAux);
handle_aux(leader, _, {stop, StreamId, #{node := Node,
                                         epoch := Epoch} = Args, Conf},
           Aux, RaAux) ->
    rabbit_log:debug("~ts: running action: 'stop'"
                     " for ~ts on node ~w in epoch ~b",
                     [?MODULE, StreamId, Node, Epoch]),
    ActionFun = phase_stop_member(StreamId, Args, Conf),
    run_action(stopping, StreamId, Args, ActionFun, Aux, RaAux);
handle_aux(leader, _, {update_mnesia, StreamId, Args, Conf},
           #aux{actions = _Monitors} = Aux, RaAux) ->
    rabbit_log:debug("~ts: running action: 'update_mnesia'"
                     " for ~ts", [?MODULE, StreamId]),
    ActionFun = phase_update_mnesia(StreamId, Args, Conf),
    run_action(updating_mnesia, StreamId, Args, ActionFun, Aux, RaAux);
handle_aux(leader, _, {update_retention, StreamId, Args, _Conf},
           #aux{actions = _Monitors} = Aux, RaAux) ->
    rabbit_log:debug("~ts: running action: 'update_retention'"
                     " for ~ts", [?MODULE, StreamId]),
    ActionFun = phase_update_retention(StreamId, Args),
    run_action(update_retention, StreamId, Args, ActionFun, Aux, RaAux);
handle_aux(leader, _, {delete_member, StreamId, #{node := Node} = Args, Conf},
           #aux{actions = _Monitors} = Aux, RaAux) ->
    rabbit_log:debug("~ts: running action: 'delete_member'"
                     " for ~ts ~ts", [?MODULE, StreamId, Node]),
    ActionFun = phase_delete_member(StreamId, Args, Conf),
    run_action(delete_member, StreamId, Args, ActionFun, Aux, RaAux);
handle_aux(leader, _, fail_active_actions,
           #aux{actions = Actions} = Aux, RaAux) ->
    %% this bit of code just creates an exclude map of currently running
    %% tasks to avoid failing them, this could only really happen during
    %% a leader flipflap
    Exclude = maps:from_list([{S, ok}
                              || {P, {S, _, _}} <- maps_to_list(Actions),
                             is_process_alive(P)]),
    rabbit_log:debug("~ts: failing actions: ~w", [?MODULE, Exclude]),
    #?MODULE{streams = Streams} = ra_aux:machine_state(RaAux),
    fail_active_actions(Streams, Exclude),
    {no_reply, Aux, RaAux, []};
handle_aux(leader, _, {down, Pid, normal},
           #aux{actions = Monitors} = Aux, RaAux) ->
    %% action process finished normally, just remove from actions map
    {no_reply, Aux#aux{actions = maps:remove(Pid, Monitors)}, RaAux, []};
handle_aux(leader, _, {down, Pid, Reason},
           #aux{actions = Monitors0} = Aux, RaAux) ->
    %% An action has failed - report back to the state machine
    case maps:get(Pid, Monitors0, undefined) of
        {StreamId, Action, #{node := Node, epoch := Epoch} = Args} ->
            rabbit_log:warning("~ts: error while executing action ~w for stream queue ~ts, "
                               " node ~ts, epoch ~b Err: ~w",
                               [?MODULE, Action, StreamId, Node, Epoch, Reason]),
            Monitors = maps:remove(Pid, Monitors0),
            Cmd = {action_failed, StreamId, Args#{action => Action}},
            send_self_command(Cmd),
            {no_reply, Aux#aux{actions = maps:remove(Pid, Monitors)},
             RaAux, []};
        undefined ->
            %% should this ever happen?
            {no_reply, Aux, RaAux, []}
    end;
handle_aux(_, _, _, AuxState, RaAux) ->
    {no_reply, AuxState, RaAux}.

overview(#?MODULE{streams = Streams,
                  monitors = Monitors,
                  single_active_consumer = Sac}) ->
    StreamsOverview = maps:map(
                        fun (_, Stream) ->
                                stream_overview0(Stream)
                        end, Streams),
    #{
      num_streams => map_size(Streams),
      num_monitors => map_size(Monitors),
      single_active_consumer => rabbit_stream_sac_coordinator:overview(Sac),
      streams => StreamsOverview
     }.

stream_overview0(#stream{epoch = Epoch,
                         members = Members,
                         listeners = StreamListeners,
                         target = Target}) ->
    MembO = maps:map(fun (_, #member{state = MS,
                                     role = R,
                                     current = C,
                                     target = T}) ->
                             #{state => MS,
                               role => R,
                               current => C,
                               target => T}
                     end, Members),
    #{epoch => Epoch,
      members => MembO,
      num_listeners => map_size(StreamListeners),
      target => Target}.

run_action(Action, StreamId, #{node := _Node,
                               epoch := _Epoch} = Args,
           ActionFun, #aux{actions = Actions0} = Aux, RaAux) ->
    Coordinator = self(),
    Pid = spawn_link(fun() ->
                             ActionFun(),
                             unlink(Coordinator)
                     end),
    Effects = [{monitor, process, aux, Pid}],
    Actions = Actions0#{Pid => {StreamId, Action, Args}},
    {no_reply, Aux#aux{actions = Actions}, RaAux, Effects}.

wrap_reply(From, Reply) ->
    [{reply, From, {wrap_reply, Reply}}].

phase_start_replica(StreamId, #{epoch := Epoch,
                                node := Node} = Args, Conf0) ->
    fun() ->
            try osiris_replica:start(Node, Conf0) of
                {ok, Pid} ->
                    rabbit_log:info("~ts: ~ts: replica started on ~ts in ~b pid ~w",
                                    [?MODULE, StreamId, Node, Epoch, Pid]),
                    send_self_command({member_started, StreamId,
                                       Args#{pid => Pid}});
                {error, already_present} ->
                    %% need to remove child record if this is the case
                    %% can it ever happen?
                    _ = osiris:stop_member(Node, Conf0),
                    send_action_failed(StreamId, starting, Args);
                {error, {already_started, Pid}} ->
                    %% TODO: we need to check that the current epoch is the same
                    %% before we can be 100% sure it is started in the correct
                    %% epoch, can this happen? who knows...
                    send_self_command({member_started, StreamId,
                                       Args#{pid => Pid}});
                {error, Reason} ->
                    rabbit_log:warning("~ts: Error while starting replica for ~ts on node ~ts in ~b : ~W",
                                       [?MODULE, maps:get(name, Conf0), Node, Epoch, Reason, 10]),
                    maybe_sleep(Reason),
                    send_action_failed(StreamId, starting, Args)
            catch _:Error ->
                    rabbit_log:warning("~ts: Error while starting replica for ~ts on node ~ts in ~b : ~W",
                                       [?MODULE, maps:get(name, Conf0), Node, Epoch, Error, 10]),
                    maybe_sleep(Error),
                    send_action_failed(StreamId, starting, Args)
            end
    end.

send_action_failed(StreamId, Action, Arg) ->
  send_self_command({action_failed, StreamId, Arg#{action => Action}}).

send_self_command(Cmd) ->
    ra:pipeline_command({?MODULE, node()}, Cmd, no_correlation, normal),
    ok.


phase_delete_member(StreamId, #{node := Node} = Arg, Conf) ->
    fun() ->
            case rabbit_nodes:is_member(Node) of
                true ->
                    try osiris:delete_member(Node, Conf) of
                        ok ->
                            rabbit_log:info("~ts: Member deleted for ~ts : on node ~ts",
                                            [?MODULE, StreamId, Node]),
                            send_self_command({member_deleted, StreamId, Arg});
                        _ ->
                            send_action_failed(StreamId, deleting, Arg)
                    catch _:E ->
                              rabbit_log:warning("~ts: Error while deleting member for ~ts : on node ~ts ~W",
                                                 [?MODULE, StreamId, Node, E, 10]),
                              maybe_sleep(E),
                              send_action_failed(StreamId, deleting, Arg)
                    end;
                false ->
                    %% node is no longer a cluster member, we return success to avoid
                    %% trying to delete the member indefinitely
                    rabbit_log:info("~ts: Member deleted/forgotten for ~ts : node ~ts is no longer a cluster member",
                                    [?MODULE, StreamId, Node]),
                    send_self_command({member_deleted, StreamId, Arg})
            end
    end.

phase_stop_member(StreamId, #{node := Node, epoch := Epoch} = Arg0, Conf) ->
    fun() ->
            try osiris_member:stop(Node, Conf) of
                ok ->
                    %% get tail
                    try get_replica_tail(Node, Conf) of
                        {ok, Tail} ->
                            Arg = Arg0#{tail => Tail},
                            rabbit_log:debug("~ts: ~ts: member stopped on ~ts in ~b Tail ~w",
                                             [?MODULE, StreamId, Node, Epoch, Tail]),
                            send_self_command({member_stopped, StreamId, Arg});
                        Err ->
                            rabbit_log:warning("~ts: failed to get tail of member ~ts on ~ts in ~b Error: ~w",
                                               [?MODULE, StreamId, Node, Epoch, Err]),
                            maybe_sleep(Err),
                            send_action_failed(StreamId, stopping, Arg0)
                    catch _:Err ->
                            rabbit_log:warning("~ts: failed to get tail of member ~ts on ~ts in ~b Error: ~w",
                                               [?MODULE, StreamId, Node, Epoch, Err]),
                            maybe_sleep(Err),
                            send_action_failed(StreamId, stopping, Arg0)
                    end
            catch _:Err ->
                      rabbit_log:warning("~ts: failed to stop member ~ts ~w Error: ~w",
                                         [?MODULE, StreamId, Node, Err]),
                      maybe_sleep(Err),
                      send_action_failed(StreamId, stopping, Arg0)
            end
    end.

phase_start_writer(StreamId, #{epoch := Epoch, node := Node} = Args0, Conf) ->
    fun() ->
            try osiris:start_writer(Conf) of
                {ok, Pid} ->
                    Args = Args0#{epoch => Epoch, pid => Pid},
                    rabbit_log:info("~ts: started writer ~ts on ~w in ~b",
                                    [?MODULE, StreamId, Node, Epoch]),
                    send_self_command({member_started, StreamId, Args});
                Err ->
                    %% no sleep for writer failures as we want to trigger a new
                    %% election asap
                    rabbit_log:warning("~ts: failed to start writer ~ts on ~ts in ~b Error: ~w",
                                       [?MODULE, StreamId, Node, Epoch, Err]),
                    send_action_failed(StreamId, starting, Args0)
            catch _:Err ->
                    rabbit_log:warning("~ts: failed to start writer ~ts on ~ts in ~b Error: ~w",
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
                    rabbit_log:warning("~ts: failed to update retention for ~ts ~w Reason: ~w",
                                       [?MODULE, StreamId, node(Pid), Reason]),
                    maybe_sleep(Err),
                    send_action_failed(StreamId, update_retention, Args)
            catch _:Err ->
                    rabbit_log:warning("~ts: failed to update retention for ~ts ~w Error: ~w",
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
            rabbit_log:debug("~ts: running mnesia update for ~ts: ~W",
                             [?MODULE, StreamId, Conf, 10]),
            Fun = fun (Q) ->
                          case amqqueue:get_type_state(Q) of
                              #{name := S} when S == StreamId ->
                                  %% the stream id matches so we can update the
                                  %% amqqueue record
                                  amqqueue:set_type_state(
                                    amqqueue:set_pid(Q, LeaderPid), Conf);
                              Ts ->
                                  S = maps:get(name, Ts, undefined),
                                  %% TODO log as side-effect
                                  rabbit_log:debug("~ts: refusing mnesia update for stale stream id ~s, current ~s",
                                                   [?MODULE, StreamId, S]),
                                  %% if the stream id isn't a match this is a stale
                                  %% update from a previous stream incarnation for the
                                  %% same queue name and we ignore it
                                  Q
                          end
                  end,
            try rabbit_amqqueue:update(QName, Fun) of
                not_found ->
                    rabbit_log:debug("~ts: resource for stream id ~ts not found, "
                                     "recovering from rabbit_durable_queue",
                                     [?MODULE, StreamId]),
                    %% This can happen during recovery
                    %% we need to re-initialise the queue record
                    %% if the stream id is a match
                    case rabbit_amqqueue:lookup_durable_queue(QName) of
                        {error, not_found} ->
                            %% queue not found at all, it must have been deleted
                            ok;
                        {ok, Q} ->
                            case amqqueue:get_type_state(Q) of
                                #{name := S} when S == StreamId ->
                                    rabbit_log:debug("~ts: initializing queue record for stream id  ~ts",
                                                     [?MODULE, StreamId]),
                                    ok = rabbit_amqqueue:ensure_rabbit_queue_record_is_initialized(Fun(Q)),
                                    ok;
                                _ ->
                                    ok
                            end,
                            send_self_command({mnesia_updated, StreamId, Args})
                    end;
                _ ->
                    send_self_command({mnesia_updated, StreamId, Args})
            catch _:E ->
                    rabbit_log:debug("~ts: failed to update mnesia for ~ts: ~W",
                                     [?MODULE, StreamId, E, 10]),
                    send_action_failed(StreamId, updating_mnesia, Args)
            end
    end.

format_ra_event(ServerId, Evt) ->
    {stream_coordinator_event, ServerId, Evt}.

make_ra_conf(Node, Nodes, MinMacVersion) ->
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
      initial_machine_version => MinMacVersion,
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
              "~ts failed to delete replica on node ~ts for stream ~ts: refusing to delete the only replica",
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
              "~ts failed to update stream:~n~W~n~W",
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
update_stream0(#{machine_version := MacVer} = Meta,
               {restart_stream, _StreamId, Options},
               #stream{members = Members0} = Stream0)
  when MacVer >= 4 ->
    Preferred = maps:get(preferred_leader_node, Options, undefined),
    Members = maps:map(fun (N, M) when N == Preferred ->
                               M#member{preferred = true,
                                        target = stopped};
                           (_N, M) ->
                               M#member{preferred = false,
                                        target = stopped}
                       end, Members0),
    Stream0#stream{members = Members,
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
                   %% reset reply_to here to ensure a reply
                   %% is returned as the command has been accepted
                   reply_to = undefined,
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
            rabbit_log:warning("~ts: member started unexpected ~w ~w",
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
update_stream0(Meta,
               {member_stopped, _StreamId,
                #{node := Node,
                  index := Idx,
                  epoch := StoppedEpoch,
                  tail := Tail}},
               #stream{epoch = Epoch,
                       target = Target,
                       nodes = Nodes,
                       members = Members0} = Stream0) ->
    IsLeaderInCurrent = case find_leader(Members0) of
                            {{_Node, #member{role = {writer, Epoch},
                                             target = running,
                                             state = {ready, Epoch}}},
                             _Replicas} ->
                                true;
                            {{_Node, #member{role = {writer, Epoch},
                                             target = running,
                                             state = {running, Epoch, _}}},
                             _Replicas} ->
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

            StoppedInCurrent =
                maps:filter(fun (_N, #member{state = {stopped, E, _T},
                                             target = running})
                                  when E == Epoch ->
                                    true;
                                (_, _) ->
                                    false
                            end, Members1),
            case is_quorum(length(Nodes), map_size(StoppedInCurrent)) of
                true ->
                    %% select leader
                    NewWriterNode = select_leader(Meta, StoppedInCurrent),
                    NextEpoch = Epoch + 1,
                    Members = maps:map(
                                fun (N, #member{state = {stopped, E, _}} = M)
                                      when E == Epoch ->
                                        case NewWriterNode of
                                            N ->
                                                %% new leader
                                                M#member{role = {writer, NextEpoch},
                                                         preferred = false,
                                                         state = {ready, NextEpoch}};
                                            _ ->
                                                M#member{role = {replica, NextEpoch},
                                                         preferred = false,
                                                         state = {ready, NextEpoch}}
                                        end;
                                    (_N, #member{target = deleted} = M) ->
                                        M;
                                    (_N, M) ->
                                        M#member{role = {replica, NextEpoch},
                                                 preferred = false}
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
                fun (N, #member{current = {sleeping, nodeup}} = M)
                      when N == Node ->
                        M#member{current = undefined};
                    (_, M) ->
                        M
                end, Members0),
    Stream0#stream{members = Members};
update_stream0(_Meta, {policy_changed, _StreamId, #{queue := Q}},
               #stream{conf = Conf0} = Stream0) ->
    Conf = rabbit_stream_queue:update_stream_conf(Q, Conf0),
    Stream0#stream{conf = Conf};
update_stream0(_Meta, {update_config, _StreamId, Conf},
               #stream{conf = Conf0} = Stream0) ->
    Stream0#stream{conf = maps:merge(Conf0, Conf)};
update_stream0(_Meta, _Cmd, undefined) ->
    undefined.

inform_listeners_eol(Vsn,
                     #stream{target = deleted,
                             listeners = Listeners,
                             queue_ref = QRef})
  when Vsn =< 1 ->
    lists:map(fun(Pid) ->
                      {send_msg, Pid,
                       {queue_event, QRef, eol},
                       cast}
              end, maps:keys(Listeners));
inform_listeners_eol(Vsn,
                        #stream{target    = deleted,
                                listeners = Listeners,
                                queue_ref = QRef}) when ?V2_OR_MORE(Vsn) ->
    LPidsMap = maps:fold(fun({P, _}, _V, Acc) ->
                            Acc#{P => ok}
                         end, #{}, Listeners),
    lists:map(fun(Pid) ->
                      {send_msg,
                       Pid,
                       {queue_event, QRef, eol},
                       cast}
              end, maps:keys(LPidsMap));
inform_listeners_eol(_, _) ->
    [].


eval_listeners(MachineVersion, #stream{} = Stream, Effects) ->
    eval_listeners(MachineVersion, Stream, undefined, Effects).

eval_listeners(_MachineVersion,
               #stream{members = Members} = Stream,
               #stream{members = Members},
               Effects0) ->
    %% if the Members have not changed don't evaluate as this is an
    %% expensive operation when there are many listeners
    {Stream, Effects0};
eval_listeners(MachineVersion, #stream{listeners = Listeners0,
                                       queue_ref = QRef,
                                       members = Members} = Stream,
               _OldStream, Effects0)
  when MachineVersion =< 1 ->
    case find_leader(Members) of
        {{_LeaderNode, #member{state = {running, _, LeaderPid}}},
         _Replicas} ->
            %% a leader is running, check all listeners to see if any of them
            %% has not been notified of the current leader pid
            {Listeners, Effects} =
                maps:fold(fun(_, P, Acc) when P == LeaderPid ->
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
    end;
eval_listeners(Vsn, #stream{listeners = Listeners0} = Stream0,
               _OldStream, Effects0)
  when ?V2_OR_MORE(Vsn) ->
    %% Iterating over stream listeners.
    %% Returning the new map of listeners and the effects (notification of changes)
    {Listeners1, Effects1} =
        maps:fold(fun(ListenerSpec, ListLPid0, {Lsts0, Effs0}) ->
                          eval_listener(ListenerSpec, ListLPid0, {Lsts0, Effs0}, Stream0)
                  end, {Listeners0, Effects0}, Listeners0),
    {Stream0#stream{listeners = Listeners1}, Effects1}.

eval_listener({P, leader}, ListLPid0, {Lsts0, Effs0},
              #stream{queue_ref = QRef,
                      members = Members}) ->
    %% iterating over member to find the leader
    {ListLPid1, Effs1} =
        maps:fold(fun(_N, #member{state  = {running, _, LeaderPid},
                                  role   = {writer, _},
                                  target = T}, A)
                        when ListLPid0 == LeaderPid, T /= deleted ->
                          %% it's the leader, same PID, nothing to do
                          A;
                     (_N, #member{state  = {running, _, LeaderPid},
                                  role   = {writer, _},
                                  target = T}, {_, Efs})
                       when T /= deleted ->
                          %% it's the leader, not same PID, assign the new leader, add effect
                          {LeaderPid, [{send_msg, P,
                                        {queue_event, QRef,
                                         {stream_leader_change, LeaderPid}},
                                        cast} | Efs]};
                     (_N, _M, Acc) ->
                          %% it's not the leader, nothing to do
                          Acc
                  end, {ListLPid0, Effs0}, Members),
    {Lsts0#{{P, leader} => ListLPid1}, Effs1};
eval_listener({P, member}, {ListNode, ListMPid0}, {Lsts0, Effs0},
              #stream{queue_ref = QRef, members = Members}) ->
    %% listening to a member on a given node
    %% iterating over the members to find the member on this node
    {ListMPid1, Effs1} =
        maps:fold(fun(MNode, #member{state = {running, _, MemberPid},
                                     target = T}, Acc)
                        when ListMPid0 == MemberPid,
                             ListNode == MNode,
                             T /= deleted ->
                          %% it's the local member of this listener
                          %% it has not changed, nothing to do
                          Acc;
                     (MNode, #member{state = {running, _, MemberPid},
                                     target = T}, {_, Efs})
                       when ListNode == MNode,
                            T /= deleted ->
                          %% it's the local member of this listener
                          %% the PID is not the same, updating it in the listener, add effect
                          {MemberPid, [{send_msg, P,
                                        {queue_event, QRef,
                                         {stream_local_member_change, MemberPid}},
                                        cast} | Efs]};
                     (_MNode, #member{state = {running, _, MemberPid},
                                      role = {replica, _},
                                      target = deleted}, {_, Efs}) ->
                          {MemberPid, [{send_msg, P,
                                        {queue_event, QRef, deleted_replica},
                                        cast} | Efs]};
                     (_N, _M, Acc) ->
                          %% not a replica, nothing to do
                          Acc
                  end, {ListMPid0, Effs0}, Members),
    {Lsts0#{{P, member} => {ListNode, ListMPid1}}, Effs1}.

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
        {{LeaderNode, #member{state = LState,
                              target = deleted,
                              current = undefined} = Writer0},
         Replicas}
           when LState =/= deleted ->
             Action = {aux, {delete_member, StreamId, LeaderNode,
                             make_writer_conf(LeaderNode, Stream0)}},
             Writer = Writer0#member{current = {deleting, Idx}},
             Effs = [Action | Effs0],
             Stream = Stream0#stream{reply_to = undefined},
             eval_replicas(Meta, {LeaderNode, Writer}, Replicas, Stream, Effs);
        {{LeaderNode, #member{state = {down, Epoch},
                              target = stopped,
                              current = undefined} = Writer0},
         Replicas} ->
             %% leader is down - all replicas need to be stopped
             %% and tail infos retrieved
             %% some replicas may already be in stopping or ready state
             Args = Meta#{epoch => Epoch,
                          node => LeaderNode},
             Conf = make_writer_conf(LeaderNode, Stream0),
             Action = {aux, {stop, StreamId, Args, Conf}},
             Writer = Writer0#member{current = {stopping, Idx}},
             eval_replicas(Meta, {LeaderNode, Writer}, Replicas, Stream0, [Action | Effs0]);
        {{LeaderNode, #member{state = {ready, Epoch}, %% writer ready in current epoch
                              target = running,
                              current = undefined} = Writer0},
         _Replicas} ->
             %% ready check has been completed and a new leader has been chosen
             %% time to start writer,
             %% if leader start fails, revert back to down state for all and re-run
             WConf = make_writer_conf(LeaderNode, Stream0),
             Members = Members0#{LeaderNode =>
                                 Writer0#member{current = {starting, Idx},
                                                conf = WConf}},
             Args = Meta#{node => LeaderNode, epoch => Epoch},
             Actions = [{aux, {start_writer, StreamId, Args, WConf}} | Effs0],
             {Stream0#stream{members = Members}, Actions};
        {{_WriterNode, #member{state = {running, Epoch, LeaderPid},
                               target = running}} = Writer, Replicas} ->
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
        {{LeaderNode, #member{state = S,
                              target = stopped,
                              current = undefined} = Writer0}, Replicas}
           when element(1, S) =/= stopped ->
             %% leader should be stopped
             Args = Meta#{node => LeaderNode, epoch => Epoch},
             Action = {aux, {stop, StreamId, Args,
                             make_writer_conf(LeaderNode, Stream0)}},
             Writer = Writer0#member{current = {stopping, Idx}},
             eval_replicas(Meta, {LeaderNode, Writer}, Replicas, Stream0,
                           [Action | Effs0]);
         {Writer, Replicas} ->
             eval_replicas(Meta, Writer, Replicas, Stream0, Effs0)
     end.

eval_replicas(Meta, undefined, Replicas, Stream, Actions0) ->
    {Members, Actions} = maps:fold(
                           fun (Node, R, Acc) ->
                                   eval_replica(Meta, Node, R, deleted, Stream, Acc)
                           end, {#{}, Actions0},
                           Replicas),
    {Stream#stream{members = Members}, Actions};
eval_replicas(Meta, {WriterNode, #member{state = LeaderState} = Writer}, Replicas,
              Stream, Actions0) ->
    {Members, Actions} = maps:fold(
                           fun (Node, R, Acc) ->
                                   eval_replica(Meta, Node, R, LeaderState,
                                                Stream, Acc)
                           end, {#{WriterNode => Writer}, Actions0},
                           Replicas),
    {Stream#stream{members = Members}, Actions}.

eval_replica(#{index := Idx} = Meta,
             Node,
             #member{state = _State,
                     target = stopped,
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
eval_replica(#{index := Idx} = Meta,
             Node,
             #member{current = Current,
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
eval_replica(#{index := Idx} = Meta, Node,
             #member{state = {State, Epoch},
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
eval_replica(_Meta, Node, #member{state = {running, Epoch, _},
                                  target = running} = Replica,
             {running, Epoch, _}, _Stream, {Replicas, Actions}) ->
    {Replicas#{Node => Replica}, Actions};
eval_replica(_Meta, Node, #member{state = {stopped, _E, _},
                                  current = undefined} = Replica,
             _LeaderState, _Stream,
             {Replicas, Actions}) ->
    %%  if stopped we should just wait for a quorum to reach stopped and
    %%  update_stream will move to ready state
    {Replicas#{Node => Replica}, Actions};
eval_replica(_Meta, Node, #member{state = {ready, E},
                                  target = running,
                                  current = undefined} = Replica,
             {ready, E}, _Stream,
             {Replicas, Actions}) ->
    %% if we're ready and so is the leader we just wait a swell
    {Replicas#{Node => Replica}, Actions};
eval_replica(_Meta, Node, #member{} = Replica, _LeaderState, _Stream,
             {Replicas, Actions}) ->
    {Replicas#{Node => Replica}, Actions}.

fail_active_actions(Streams, Exclude) ->
    _ = maps:map(
      fun (_,  #stream{id = Id,
                       members = Members,
                       mnesia = Mnesia})
            when not is_map_key(Id, Exclude)  ->
              _ = maps:map(fun(N, M) ->
                                   fail_action(Id, N, M)
                           end, Members),
              case Mnesia of
                  {updating, E} ->
                      rabbit_log:debug("~ts: failing stale action to trigger retry. "
                                       "Stream ID: ~ts, node: ~w, action: ~w",
                                       [?MODULE, Id, node(), updating_mnesia]),
                      send_self_command({action_failed, Id,
                                         #{action => updating_mnesia,
                                           index => 0,
                                           node => node(),
                                           epoch => E}});
                  _ ->
                      ok
              end,
              ok
      end, Streams),

    ok.

fail_action(_StreamId, _, #member{current = undefined}) ->
    ok;
fail_action(StreamId, Node, #member{role = {_, E},
                                    current = {Action, Idx}}) ->
    rabbit_log:debug("~ts: failing stale action to trigger retry. "
                     "Stream ID: ~ts, node: ~w, action: ~w",
                     [?MODULE, StreamId, node(), Action]),
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

make_writer_conf(Node, #stream{epoch = Epoch,
                               nodes = Nodes,
                               conf = Conf}) ->
    Conf#{leader_node => Node,
          nodes => Nodes,
          replica_nodes => lists:delete(Node, Nodes),
          epoch => Epoch}.

find_leader(Members) ->
    case lists:partition(
           fun ({_, #member{target = deleted}}) ->
                   false;
               ({_, #member{role = {Role, _}}}) ->
                   Role == writer
           end, maps_to_list(Members)) of
        {[Writer], Replicas} ->
            {Writer, maps:from_list(Replicas)};
        {[], Replicas} ->
            {undefined, maps:from_list(Replicas)}
    end.

select_leader(#{machine_version := 0}, EpochOffsets)
  when is_list(EpochOffsets) ->
    %% this is the version 0 faulty version of this code,
    %% retained for versioning
    [{Node, _} | _] = lists:sort(fun({_, {Ao, E}}, {_, {Bo, E}}) ->
                                         Ao >= Bo;
                                    ({_, {_, Ae}}, {_, {_, Be}}) ->
                                         Ae >= Be;
                                    ({_, empty}, _) ->
                                         false;
                                    (_, {_, empty}) ->
                                         true
                                 end, EpochOffsets),
    Node;
select_leader(#{machine_version := MacVer}, EpochOffsets)
  when MacVer =< 3
  andalso is_list(EpochOffsets) ->
    %% this is the logic up til v3
    [{Node, _} | _] = lists:sort(
                        fun({_, {Epoch, OffsetA}}, {_, {Epoch, OffsetB}}) ->
                                OffsetA >= OffsetB;
                           ({_, {EpochA, _}}, {_, {EpochB, _}}) ->
                                EpochA >= EpochB;
                           ({_, empty}, _) ->
                                false;
                           (_, {_, empty}) ->
                                true
                        end, EpochOffsets),
    Node;
select_leader(#{system_time := Ts,
                machine_version := MacVer,
                index := Idx},
              Stopped)
  when is_map(Stopped) andalso MacVer >= 4 ->
    %% this logic gets all potential nodes and does a selection with some
    %% degree of random
    [{_, #member{state = MState}} | _] = Sorted =
        lists:sort(fun({_, #member{state = {stopped, _, {Epoch, OffsetA}}}},
                       {_, #member{state = {stopped, _, {Epoch, OffsetB}}}}) ->
                           %% same epoch, compare last chunk ids
                           OffsetA >= OffsetB;
                      ({_, #member{state = {stopped, _, {EpochA, _}}}},
                       {_, #member{state = {stopped, _, {EpochB, _}}}}) ->
                           EpochA >= EpochB;
                      ({_, #member{state = {stopped, _, empty}}}, _) ->
                           false;
                      (_, {_, #member{state = {stopped, _, empty}}}) ->
                           true
                   end, maps_to_list(Stopped)),
    Potential = lists:takewhile(fun ({_N, #member{state = S}}) ->
                                        S == MState
                                end, Sorted),
    case Potential of
        [{Node, _}] ->
            Node;
        _ ->
            case preferred_leader(Potential) of
                undefined ->
                    % there are more than one and no preferred leader
                    % use modulo to select
                    Nth = ((Ts + Idx) rem length(Potential)) + 1,
                    {Node, _} = lists:nth(Nth, Potential),
                    Node;
                N ->
                    N
            end
    end;
select_leader(Meta, Stopped) ->
    %% recurse with old format
    select_leader(Meta,
                  maps_to_list(
                    maps:map(
                      fun (_N, #member{state = {stopped, _, Tail}}) ->
                              Tail
                      end, Stopped))).

preferred_leader([]) ->
    undefined;
preferred_leader([{N, #member{preferred = true}} | _Rem]) ->
    N;
preferred_leader([{_N, #member{}} | Rem]) ->
    preferred_leader(Rem).

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

machine_version(1, 2, State = #?MODULE{streams = Streams0,
                                       monitors = Monitors0}) ->
    rabbit_log:info("Stream coordinator machine version changes from 1 to 2, updating state."),
    %% conversion from old state to new state
    %% additional operation: the stream listeners are never collected in the previous version
    %% so we'll emit monitors for all listener PIDs
    %% this way we'll get the DOWN event for dead listener PIDs and
    %% we'll clean the stream listeners the in the DOWN event callback

    %% transform the listeners of each stream and accumulate listener PIDs
    {Streams1, Listeners} =
    maps:fold(fun(S, #stream{listeners = L0} = S0, {StreamAcc, GlobalListAcc}) ->
                      {L1, GlobalListAcc1} = maps:fold(
                                    fun(ListPid, LeaderPid, {LAcc, GLAcc}) ->
                                        {LAcc#{{ListPid, leader} => LeaderPid},
                                        GLAcc#{ListPid => S}}
                                    end, {#{}, GlobalListAcc}, L0),
                      {StreamAcc#{S => S0#stream{listeners = L1}}, GlobalListAcc1}
              end, {#{}, #{}}, Streams0),
    %% accumulate monitors for the map and create the effects to emit the monitors
    {ExtraMonitors, Effects} = maps:fold(fun(P, StreamId, {MAcc, EAcc}) ->
                                                 {MAcc#{P => {StreamId, listener}},
                                                 [{monitor, process, P} | EAcc]}
                          end, {#{}, []}, Listeners),
    Monitors1 = maps:merge(Monitors0, ExtraMonitors),
    Monitors2 = maps:fold(fun(P, {StreamId, listener}, Acc) ->
                                  Acc#{P => {#{StreamId => ok}, listener}};
                             (P, V, Acc) ->
                                  Acc#{P => V}
                          end, #{}, Monitors1),
    {State#?MODULE{streams = Streams1,
                   monitors = Monitors2,
                   listeners = undefined}, Effects};
machine_version(2, 3, State) ->
    rabbit_log:info("Stream coordinator machine version changes from 2 to 3, "
                    "updating state."),
    SacState = rabbit_stream_sac_coordinator_v4:init_state(),
    {State#?MODULE{single_active_consumer = SacState},
     []};
machine_version(3, 4, #?MODULE{streams = Streams0} = State) ->
    rabbit_log:info("Stream coordinator machine version changes from 3 to 4, updating state."),
    %% the "preferred" field takes the place of the "node" field in this version
    %% initializing the "preferred" field to false
    Streams = maps:map(
                fun (_, #stream{members = Members} = S) ->
                        S#stream{members = maps:map(
                                             fun (_N, M) ->
                                                     M#member{preferred = false}
                                             end, Members)}
                end, Streams0),
    {State#?MODULE{streams = Streams}, []};
machine_version(4 = From, 5, #?MODULE{single_active_consumer = Sac0} = State) ->
    rabbit_log:info("Stream coordinator machine version changes from 4 to 5, updating state."),
    SacExport = rabbit_stream_sac_coordinator_v4:state_to_map(Sac0),
    Sac1 = rabbit_stream_sac_coordinator:import_state(From, SacExport),
    {State#?MODULE{single_active_consumer = Sac1}, []};
machine_version(From, To, State) ->
    rabbit_log:info("Stream coordinator machine version changes from ~tp to ~tp, no state changes required.",
                    [From, To]),
    {State, []}.

-spec transfer_leadership([node()]) -> {ok, in_progress | undefined | node()} | {error, any()}.
transfer_leadership([Destination | _] = _TransferCandidates) ->
    case ra_leaderboard:lookup_leader(?MODULE) of
        {Name, Node} = Id when Node == node() ->
            case ra:transfer_leadership(Id, {Name, Destination}) of
                ok ->
                    case ra:members(Id) of
                        {_, _, {_, NewNode}} ->
                            {ok, NewNode};
                        {timeout, _} ->
                            {error, not_migrated}
                    end;
                already_leader ->
                    {ok, Destination};
                {error, _} = Error ->
                    Error;
                {timeout, _} ->
                    {error, timeout}
            end;
        {_, Node} ->
            {ok, Node};
        undefined ->
            {ok, undefined}
    end.

-spec forget_node(node()) -> ok | {error, term()}.
forget_node(Node) when is_atom(Node) ->
    case ra_directory:uid_of(?RA_SYSTEM, ?MODULE) of
        undefined ->
            %% if there is no local stream coordinator registered it is likely that the
            %% system does not use streams at all and we just return ok
            %% here. The alternative would be to do a cluster wide rpc here
            %% to check but given there is a fallback
            ok;
        _ ->
            IsRunning = rabbit_nodes:is_running(Node),
            ExpectedMembers = expected_coord_members(),
            ToRemove = {?MODULE, Node},
            case ra:members(ExpectedMembers) of
                {ok, Members, Leader} ->
                    case lists:member(ToRemove, Members) of
                        true ->
                            case ra:remove_member(Leader, ToRemove) of
                                {ok, _, _} when IsRunning ->
                                    _ = ra:force_delete_server(?RA_SYSTEM, ToRemove),
                                    ok;
                                {ok, _, _} ->
                                    ok;
                                {error, _} = Err ->
                                    Err
                            end;
                        false ->
                            ok
                    end;
                Err ->
                    Err
            end
    end.


-spec member_overview() ->
    {ok, map()} | {error, term()}.
member_overview() ->
    case whereis(?MODULE) of
        undefined ->
            {error, local_stream_coordinator_not_running};
        _ ->
            case ra:member_overview({?MODULE, node()}) of
                {ok, Result, _} ->
                    {ok, maps:remove(system_config, Result)};
                Err ->
                    Err
            end
    end.

-spec status() ->
    [[{binary(), term()}]] | {error, term()}.
status() ->
    case members() of
        {ok, Members, _} ->
            [begin
                 case erpc_call(N, ?MODULE, key_metrics_rpc, [ServerId], ?RPC_TIMEOUT) of
                     #{state := RaftState,
                       membership := Membership,
                       commit_index := Commit,
                       term := Term,
                       last_index := Last,
                       last_applied := LastApplied,
                       last_written_index := LastWritten,
                       snapshot_index := SnapIdx,
                       machine_version := MacVer} ->
                         [{<<"Node Name">>, N},
                          {<<"Raft State">>, RaftState},
                          {<<"Membership">>, Membership},
                          {<<"Last Log Index">>, Last},
                          {<<"Last Written">>, LastWritten},
                          {<<"Last Applied">>, LastApplied},
                          {<<"Commit Index">>, Commit},
                          {<<"Snapshot Index">>, SnapIdx},
                          {<<"Term">>, Term},
                          {<<"Machine Version">>, MacVer}
                         ];
                     {error, Err} ->
                         [{<<"Node Name">>, N},
                          {<<"Raft State">>, Err},
                          {<<"Membership">>, <<>>},
                          {<<"LastLog Index">>, <<>>},
                          {<<"Last Written">>, <<>>},
                          {<<"Last Applied">>, <<>>},
                          {<<"Commit Index">>, <<>>},
                          {<<"Snapshot Index">>, <<>>},
                          {<<"Term">>, <<>>},
                          {<<"Machine Version">>, <<>>}
                         ]
                 end
             end || {_, N} = ServerId <- Members];
        {error, {no_more_servers_to_try, _}} ->
            {error, coordinator_not_started_or_available};
        Err ->
            Err
    end.

key_metrics_rpc(ServerId) ->
    Metrics = ra:key_metrics(ServerId),
    Metrics#{machine_version => version()}.

maps_to_list(M) ->
    lists:sort(maps:to_list(M)).

ra_local_query(QueryFun) ->
    ra:local_query({?MODULE, node()}, QueryFun, infinity).

sac_handle_connection_down(SacState, Pid, Reason, Vsn) when ?V5_OR_MORE(Vsn) ->
    ?SAC_CURRENT:handle_connection_down(Pid, Reason, SacState);
sac_handle_connection_down(SacState, Pid, _Reason, _Vsn) ->
    ?SAC_V4:handle_connection_down(Pid, SacState).

sac_handle_node_reconnected(#{machine_version := Vsn} = Meta, Node,
                            Sac, Effects) ->
    case ?V5_OR_MORE(Vsn) of
        true ->
            SacMod = sac_module(Meta),
            SacMod:handle_node_reconnected(Node,
                                           Sac, Effects);
        false ->
            {Sac, Effects}
    end.

sac_make_purge_nodes(Nodes) ->
    rabbit_stream_sac_coordinator:make_purge_nodes(Nodes).

sac_make_update_conf(Conf) ->
    rabbit_stream_sac_coordinator:make_update_conf(Conf).

sac_check_conf_change(SacState) ->
    rabbit_stream_sac_coordinator:check_conf_change(SacState).

sac_list_nodes(State, Vsn) when ?V5_OR_MORE(Vsn) ->
    rabbit_stream_sac_coordinator:list_nodes(sac_state(State));
sac_list_nodes(_, _) ->
    [].
