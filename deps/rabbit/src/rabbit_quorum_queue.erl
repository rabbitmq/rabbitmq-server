%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2018-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_quorum_queue).

-export([init_state/2, handle_event/2]).
-export([declare/1, recover/1, stop/1, delete/4, delete_immediately/2]).
-export([info/1, info/2, stat/1, stat/2, infos/1]).
-export([ack/3, reject/4, basic_get/4, basic_consume/10, basic_cancel/4]).
-export([credit/4]).
-export([purge/1]).
-export([stateless_deliver/2, deliver/3]).
-export([dead_letter_publish/4]).
-export([queue_name/1]).
-export([cluster_state/1, status/2]).
-export([update_consumer_handler/8, update_consumer/9]).
-export([cancel_consumer_handler/2, cancel_consumer/3]).
-export([become_leader/2, handle_tick/3, spawn_deleter/1]).
-export([rpc_delete_metrics/1]).
-export([format/1]).
-export([open_files/1]).
-export([peek/2, peek/3]).
-export([add_member/4]).
-export([delete_member/3]).
-export([requeue/3]).
-export([policy_changed/2]).
-export([cleanup_data_dir/0]).
-export([shrink_all/1,
         grow/4]).
-export([transfer_leadership/2, get_replicas/1, queue_length/1]).
-export([file_handle_leader_reservation/1, file_handle_other_reservation/0]).
-export([file_handle_release_reservation/0]).
-export([list_with_minimum_quorum/0, list_with_minimum_quorum_for_cli/0,
         filter_quorum_critical/1, filter_quorum_critical/2,
         all_replica_states/0]).
-export([is_policy_applicable/2]).
-export([repair_amqqueue_nodes/1,
         repair_amqqueue_nodes/2
         ]).
-export([reclaim_memory/2]).
-export([notify_decorators/1,
         notify_decorators/3,
         spawn_notify_decorators/3]).

-include_lib("stdlib/include/qlc.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").
-include("amqqueue.hrl").

-type msg_id() :: non_neg_integer().
-type qmsg() :: {rabbit_types:r('queue'), pid(), msg_id(), boolean(), rabbit_types:message()}.

-define(STATISTICS_KEYS,
        [policy,
         operator_policy,
         effective_policy_definition,
         consumers,
         memory,
         state,
         garbage_collection,
         leader,
         online,
         members,
         open_files,
         single_active_consumer_pid,
         single_active_consumer_ctag,
         messages_ram,
         message_bytes_ram
        ]).

-define(RPC_TIMEOUT, 1000).
-define(TICK_TIMEOUT, 5000). %% the ra server tick time
-define(DELETE_TIMEOUT, 5000).
-define(ADD_MEMBER_TIMEOUT, 5000).

%%----------------------------------------------------------------------------

-spec init_state(amqqueue:ra_server_id(), rabbit_amqqueue:name()) ->
    rabbit_fifo_client:state().
init_state({Name, _}, QName = #resource{}) ->
    {ok, SoftLimit} = application:get_env(rabbit, quorum_commands_soft_limit),
    %% This lookup could potentially return an {error, not_found}, but we do not
    %% know what to do if the queue has `disappeared`. Let it crash.
    {ok, Q} = rabbit_amqqueue:lookup(QName),
    Leader = amqqueue:get_pid(Q),
    Nodes = rabbit_amqqueue:get_quorum_nodes(Q),
    %% Ensure the leader is listed first
    Servers0 = [{Name, N} || N <- Nodes],
    Servers = [Leader | lists:delete(Leader, Servers0)],
    rabbit_fifo_client:init(QName, Servers, SoftLimit,
                            fun() -> credit_flow:block(Name) end,
                            fun() -> credit_flow:unblock(Name), ok end).

-spec handle_event({'ra_event', amqqueue:ra_server_id(), any()},
                   rabbit_fifo_client:state()) ->
    {internal, Correlators :: [term()], rabbit_fifo_client:actions(),
     rabbit_fifo_client:state()} |
    {rabbit_fifo:client_msg(), rabbit_fifo_client:state()} |
    eol.
handle_event({ra_event, From, Evt}, QState) ->
    rabbit_fifo_client:handle_ra_event(From, Evt, QState).

-spec declare(amqqueue:amqqueue()) ->
    {new | existing, amqqueue:amqqueue()} | rabbit_types:channel_exit().

declare(Q) when ?amqqueue_is_quorum(Q) ->
    rabbit_log:info("quorum_queue declaring ~w", [Q]),
    QName = amqqueue:get_name(Q),
    Durable = amqqueue:is_durable(Q),
    AutoDelete = amqqueue:is_auto_delete(Q),
    Arguments = amqqueue:get_arguments(Q),
    Opts = amqqueue:get_options(Q),
    ActingUser = maps:get(user, Opts, ?UNKNOWN_USER),
    check_invalid_arguments(QName, Arguments),
    check_auto_delete(Q),
    check_exclusive(Q),
    check_non_durable(Q),
    QuorumSize = get_default_quorum_initial_group_size(Arguments),
    RaName = case qname_to_internal_name(QName) of
                 {ok, A} ->
                     A;
                 {error, {too_long, N}} ->
                     rabbit_data_coercion:to_atom(ra:new_uid(N))
             end,
    Id = {RaName, node()},
    Nodes = select_quorum_nodes(QuorumSize, rabbit_mnesia:cluster_nodes(all)),
    NewQ0 = amqqueue:set_pid(Q, Id),
    NewQ1 = amqqueue:set_type_state(NewQ0, #{nodes => Nodes}),

    rabbit_log:debug("Will start up to ~p replicas for quorum queue ~s", [QuorumSize, rabbit_misc:rs(QName)]),
    case rabbit_amqqueue:internal_declare(NewQ1, false) of
        {created, NewQ} ->
            TickTimeout = application:get_env(rabbit, quorum_tick_interval,
                                              ?TICK_TIMEOUT),
            RaConfs = [make_ra_conf(NewQ, ServerId, TickTimeout)
                       || ServerId <- members(NewQ)],
            case ra:start_cluster(RaConfs) of
                {ok, _, _} ->
                    %% TODO: handle error - what should be done if the
                    %% config cannot be updated
                    ok = rabbit_fifo_client:update_machine_state(Id,
                                                                 ra_machine_config(NewQ)),
                    %% force a policy change to ensure the latest config is
                    %% updated even when running the machine version from 0
                    notify_decorators(QName, startup),
                    rabbit_event:notify(queue_created,
                                        [{name, QName},
                                         {durable, Durable},
                                         {auto_delete, AutoDelete},
                                         {arguments, Arguments},
                                         {user_who_performed_action,
                                          ActingUser}]),
                    {new, NewQ};
                {error, Error} ->
                    _ = rabbit_amqqueue:internal_delete(QName, ActingUser),
                    rabbit_misc:protocol_error(
                      internal_error,
                      "Cannot declare a queue '~s' on node '~s': ~255p",
                      [rabbit_misc:rs(QName), node(), Error])
            end;
        {existing, _} = Ex ->
            Ex
    end.

ra_machine(Q) ->
    {module, rabbit_fifo, ra_machine_config(Q)}.

ra_machine_config(Q) when ?is_amqqueue(Q) ->
    QName = amqqueue:get_name(Q),
    {Name, _} = amqqueue:get_pid(Q),
    %% take the minimum value of the policy and the queue arg if present
    MaxLength = args_policy_lookup(<<"max-length">>, fun min/2, Q),
    %% prefer the policy defined strategy if available
    Overflow = args_policy_lookup(<<"overflow">>, fun (A, _B) -> A end , Q),
    MaxBytes = args_policy_lookup(<<"max-length-bytes">>, fun min/2, Q),
    MaxMemoryLength = args_policy_lookup(<<"max-in-memory-length">>, fun min/2, Q),
    MaxMemoryBytes = args_policy_lookup(<<"max-in-memory-bytes">>, fun min/2, Q),
    DeliveryLimit = args_policy_lookup(<<"delivery-limit">>, fun min/2, Q),
    Expires = args_policy_lookup(<<"expires">>,
                                 fun (A, _B) -> A end,
                                 Q),
    #{name => Name,
      queue_resource => QName,
      dead_letter_handler => dlx_mfa(Q),
      become_leader_handler => {?MODULE, become_leader, [QName]},
      max_length => MaxLength,
      max_bytes => MaxBytes,
      max_in_memory_length => MaxMemoryLength,
      max_in_memory_bytes => MaxMemoryBytes,
      single_active_consumer_on => single_active_consumer_on(Q),
      delivery_limit => DeliveryLimit,
      overflow_strategy => overflow(Overflow, drop_head),
      created => erlang:system_time(millisecond),
      expires => Expires
     }.

single_active_consumer_on(Q) ->
    QArguments = amqqueue:get_arguments(Q),
    case rabbit_misc:table_lookup(QArguments, <<"x-single-active-consumer">>) of
        {bool, true} -> true;
        _            -> false
    end.

update_consumer_handler(QName, {ConsumerTag, ChPid}, Exclusive, AckRequired, Prefetch, Active, ActivityStatus, Args) ->
    local_or_remote_handler(ChPid, rabbit_quorum_queue, update_consumer,
        [QName, ChPid, ConsumerTag, Exclusive, AckRequired, Prefetch, Active, ActivityStatus, Args]).

update_consumer(QName, ChPid, ConsumerTag, Exclusive, AckRequired, Prefetch, Active, ActivityStatus, Args) ->
    catch rabbit_core_metrics:consumer_updated(ChPid, ConsumerTag, Exclusive, AckRequired,
                                               QName, Prefetch, Active, ActivityStatus, Args).

cancel_consumer_handler(QName, {ConsumerTag, ChPid}) ->
    local_or_remote_handler(ChPid, rabbit_quorum_queue, cancel_consumer,
                            [QName, ChPid, ConsumerTag]).

cancel_consumer(QName, ChPid, ConsumerTag) ->
    catch rabbit_core_metrics:consumer_deleted(ChPid, ConsumerTag, QName),
    emit_consumer_deleted(ChPid, ConsumerTag, QName, ?INTERNAL_USER).

local_or_remote_handler(ChPid, Module, Function, Args) ->
    Node = node(ChPid),
    case Node == node() of
        true ->
            erlang:apply(Module, Function, Args);
        false ->
            %% this could potentially block for a while if the node is
            %% in disconnected state or tcp buffers are full
            rpc:cast(Node, Module, Function, Args)
    end.

become_leader(QName, Name) ->
    Fun = fun (Q1) ->
                  amqqueue:set_state(
                    amqqueue:set_pid(Q1, {Name, node()}),
                    live)
          end,
    %% as this function is called synchronously when a ra node becomes leader
    %% we need to ensure there is no chance of blocking as else the ra node
    %% may not be able to establish it's leadership
    spawn(fun() ->
                  rabbit_misc:execute_mnesia_transaction(
                    fun() ->
                            rabbit_amqqueue:update(QName, Fun)
                    end),
                  case rabbit_amqqueue:lookup(QName) of
                      {ok, Q0} when ?is_amqqueue(Q0) ->
                          Nodes = get_nodes(Q0),
                          [rpc:call(Node, ?MODULE, rpc_delete_metrics,
                                    [QName], ?RPC_TIMEOUT)
                           || Node <- Nodes, Node =/= node()];
                      _ ->
                          ok
                  end
          end).

-spec all_replica_states() -> {node(), #{atom() => atom()}}.
all_replica_states() ->
    Rows = ets:tab2list(ra_state),
    {node(), maps:from_list(Rows)}.

-spec list_with_minimum_quorum() -> [amqqueue:amqqueue()].
list_with_minimum_quorum() ->
    filter_quorum_critical(rabbit_amqqueue:list_local_quorum_queues()).

-spec list_with_minimum_quorum_for_cli() -> [#{binary() => term()}].
list_with_minimum_quorum_for_cli() ->
    QQs = list_with_minimum_quorum(),
    [begin
         #resource{name = Name} = amqqueue:get_name(Q),
         #{
             <<"readable_name">> => rabbit_data_coercion:to_binary(rabbit_misc:rs(amqqueue:get_name(Q))),
             <<"name">> => Name,
             <<"virtual_host">> => amqqueue:get_vhost(Q),
             <<"type">> => <<"quorum">>
         }
     end || Q <- QQs].

-spec filter_quorum_critical([amqqueue:amqqueue()]) -> [amqqueue:amqqueue()].
filter_quorum_critical(Queues) ->
    %% Example map of QQ replica states:
    %%    #{rabbit@warp10 =>
    %%      #{'%2F_qq.636' => leader,'%2F_qq.243' => leader,
    %%        '%2F_qq.1939' => leader,'%2F_qq.1150' => leader,
    %%        '%2F_qq.1109' => leader,'%2F_qq.1654' => leader,
    %%        '%2F_qq.1679' => leader,'%2F_qq.1003' => leader,
    %%        '%2F_qq.1593' => leader,'%2F_qq.1765' => leader,
    %%        '%2F_qq.933' => leader,'%2F_qq.38' => leader,
    %%        '%2F_qq.1357' => leader,'%2F_qq.1345' => leader,
    %%        '%2F_qq.1694' => leader,'%2F_qq.994' => leader,
    %%        '%2F_qq.490' => leader,'%2F_qq.1704' => leader,
    %%        '%2F_qq.58' => leader,'%2F_qq.564' => leader,
    %%        '%2F_qq.683' => leader,'%2F_qq.386' => leader,
    %%        '%2F_qq.753' => leader,'%2F_qq.6' => leader,
    %%        '%2F_qq.1590' => leader,'%2F_qq.1363' => leader,
    %%        '%2F_qq.882' => leader,'%2F_qq.1161' => leader,...}}
    ReplicaStates = maps:from_list(
                        rabbit_misc:append_rpc_all_nodes(rabbit_nodes:all_running(),
                            ?MODULE, all_replica_states, [])),
    filter_quorum_critical(Queues, ReplicaStates).

-spec filter_quorum_critical([amqqueue:amqqueue()], #{node() => #{atom() => atom()}}) -> [amqqueue:amqqueue()].

filter_quorum_critical(Queues, ReplicaStates) ->
    lists:filter(fun (Q) ->
                    MemberNodes = rabbit_amqqueue:get_quorum_nodes(Q),
                    {Name, _Node} = amqqueue:get_pid(Q),
                    AllUp = lists:filter(fun (N) ->
                                            {Name, _} = amqqueue:get_pid(Q),
                                            case maps:get(N, ReplicaStates, undefined) of
                                                #{Name := State} when State =:= follower orelse State =:= leader ->
                                                    true;
                                                _ -> false
                                            end
                                         end, MemberNodes),
                    MinQuorum = length(MemberNodes) div 2 + 1,
                    length(AllUp) =< MinQuorum
                 end, Queues).

-spec is_policy_applicable(amqqueue:amqqueue(), any()) -> boolean().
is_policy_applicable(_Q, Policy) ->
    NotApplicable = [ %% Classic policies
                    <<"message-ttl">>, <<"max-priority">>, <<"queue-mode">>,
                    <<"single-active-consumer">>, <<"ha-mode">>, <<"ha-params">>,
                    <<"ha-sync-mode">>, <<"ha-promote-on-shutdown">>, <<"ha-promote-on-failure">>,
                    <<"queue-master-locator">>],
    lists:all(fun({P, _}) ->
                      not lists:member(P, NotApplicable)
              end, Policy).

rpc_delete_metrics(QName) ->
    ets:delete(queue_coarse_metrics, QName),
    ets:delete(queue_metrics, QName),
    ok.

spawn_deleter(QName) ->
    spawn(fun () ->
                  {ok, Q} = rabbit_amqqueue:lookup(QName),
                  delete(Q, false, false, <<"expired">>)
          end).

spawn_notify_decorators(QName, Fun, Args) ->
    spawn(fun () ->
                  notify_decorators(QName, Fun, Args)
          end).

handle_tick(QName,
            {Name, MR, MU, M, C, MsgBytesReady, MsgBytesUnack},
            Nodes) ->
    %% this makes calls to remote processes so cannot be run inside the
    %% ra server
    Self = self(),
    _ = spawn(fun() ->
                      R = reductions(Name),
                      rabbit_core_metrics:queue_stats(QName, MR, MU, M, R),
                      Util = case C of
                                 0 -> 0;
                                 _ -> rabbit_fifo:usage(Name)
                             end,
                      Infos = [{consumers, C},
                               {consumer_capacity, Util},
                               {consumer_utilisation, Util},
                               {message_bytes_ready, MsgBytesReady},
                               {message_bytes_unacknowledged, MsgBytesUnack},
                               {message_bytes, MsgBytesReady + MsgBytesUnack},
                               {message_bytes_persistent, MsgBytesReady + MsgBytesUnack},
                               {messages_persistent, M}

                               | infos(QName, ?STATISTICS_KEYS -- [consumers])],
                      rabbit_core_metrics:queue_stats(QName, Infos),
                      rabbit_event:notify(queue_stats,
                                          Infos ++ [{name, QName},
                                                    {messages, M},
                                                    {messages_ready, MR},
                                                    {messages_unacknowledged, MU},
                                                    {reductions, R}]),
                      ok = repair_leader_record(QName, Self),
                      ExpectedNodes = rabbit_mnesia:cluster_nodes(all),
                      case Nodes -- ExpectedNodes of
                          [] ->
                              ok;
                          Stale ->
                              rabbit_log:info("~s: stale nodes detected. Purging ~w~n",
                                              [rabbit_misc:rs(QName), Stale]),
                              %% pipeline purge command
                              {ok, Q} = rabbit_amqqueue:lookup(QName),
                              ok = ra:pipeline_command(amqqueue:get_pid(Q),
                                                       rabbit_fifo:make_purge_nodes(Stale)),

                              ok
                      end
              end),
    ok.

repair_leader_record(QName, Self) ->
    {ok, Q} = rabbit_amqqueue:lookup(QName),
    Node = node(),
    case amqqueue:get_pid(Q) of
        {_, Node} ->
            %% it's ok - we don't need to do anything
            ok;
        _ ->
            rabbit_log:debug("~s: repairing leader record",
                             [rabbit_misc:rs(QName)]),
            {_, Name} = erlang:process_info(Self, registered_name),
            become_leader(QName, Name)
    end,
    ok.

repair_amqqueue_nodes(VHost, QueueName) ->
    QName = #resource{virtual_host = VHost, name = QueueName, kind = queue},
    repair_amqqueue_nodes(QName).

-spec repair_amqqueue_nodes(rabbit_types:r('queue') | amqqueue:amqqueue()) ->
    ok | repaired.
repair_amqqueue_nodes(QName = #resource{}) ->
    {ok, Q0} = rabbit_amqqueue:lookup(QName),
    repair_amqqueue_nodes(Q0);
repair_amqqueue_nodes(Q0) ->
    QName = amqqueue:get_name(Q0),
    Leader = amqqueue:get_pid(Q0),
    {ok, Members, _} = ra:members(Leader),
    RaNodes = [N || {_, N} <- Members],
    #{nodes := Nodes} = amqqueue:get_type_state(Q0),
    case lists:sort(RaNodes) =:= lists:sort(Nodes) of
        true ->
            %% up to date
            ok;
        false ->
            %% update amqqueue record
            Fun = fun (Q) ->
                          TS0 = amqqueue:get_type_state(Q),
                          TS = TS0#{nodes => RaNodes},
                          amqqueue:set_type_state(Q, TS)
                  end,
            rabbit_misc:execute_mnesia_transaction(
              fun() ->
                      rabbit_amqqueue:update(QName, Fun)
              end),
            repaired
    end.

reductions(Name) ->
    try
        {reductions, R} = process_info(whereis(Name), reductions),
        R
    catch
        error:badarg ->
            0
    end.

-spec recover([amqqueue:amqqueue()]) -> [amqqueue:amqqueue()].

recover(Queues) ->
    [begin
         {Name, _} = amqqueue:get_pid(Q0),
         case ra:restart_server({Name, node()}) of
             ok ->
                 % queue was restarted, good
                 ok;
             {error, Err1}
               when Err1 == not_started orelse
                    Err1 == name_not_registered ->
                 % queue was never started on this node
                 % so needs to be started from scratch.
                 TickTimeout = application:get_env(rabbit, quorum_tick_interval,
                                                   ?TICK_TIMEOUT),
                 Conf = make_ra_conf(Q0, {Name, node()}, TickTimeout),
                 case ra:start_server(Conf) of
                     ok ->
                         ok;
                     Err2 ->
                         rabbit_log:warning("recover: quorum queue ~w could not"
                                            " be started ~w", [Name, Err2]),
                         ok
                 end;
             {error, {already_started, _}} ->
                 %% this is fine and can happen if a vhost crashes and performs
                 %% recovery whilst the ra application and servers are still
                 %% running
                 ok;
             Err ->
                 %% catch all clause to avoid causing the vhost not to start
                 rabbit_log:warning("recover: quorum queue ~w could not be "
                                    "restarted ~w", [Name, Err]),
                 ok
         end,
         %% we have to ensure the  quorum queue is
         %% present in the rabbit_queue table and not just in rabbit_durable_queue
         %% So many code paths are dependent on this.
         {ok, Q} = rabbit_amqqueue:ensure_rabbit_queue_record_is_initialized(Q0),
         Q
     end || Q0 <- Queues].

-spec stop(rabbit_types:vhost()) -> 'ok'.

stop(VHost) ->
    _ = [begin
             Pid = amqqueue:get_pid(Q),
             ra:stop_server(Pid)
         end || Q <- find_quorum_queues(VHost)],
    ok.

-spec delete(amqqueue:amqqueue(),
             boolean(), boolean(),
             rabbit_types:username()) ->
    {ok, QLen :: non_neg_integer()}.

delete(Q, true, _IfEmpty, _ActingUser) when ?amqqueue_is_quorum(Q) ->
    rabbit_misc:protocol_error(
      not_implemented,
      "cannot delete ~s. queue.delete operations with if-unused flag set are not supported by quorum queues",
      [rabbit_misc:rs(amqqueue:get_name(Q))]);
delete(Q, _IfUnused, true, _ActingUser) when ?amqqueue_is_quorum(Q) ->
    rabbit_misc:protocol_error(
      not_implemented,
      "cannot delete ~s. queue.delete operations with if-empty flag set are not supported by quorum queues",
      [rabbit_misc:rs(amqqueue:get_name(Q))]);
delete(Q,
       _IfUnused, _IfEmpty, ActingUser) when ?amqqueue_is_quorum(Q) ->
    {Name, _} = amqqueue:get_pid(Q),
    QName = amqqueue:get_name(Q),
    QNodes = get_nodes(Q),
    %% TODO Quorum queue needs to support consumer tracking for IfUnused
    Timeout = ?DELETE_TIMEOUT,
    {ok, ReadyMsgs, _} = stat(Q),
    Servers = [{Name, Node} || Node <- QNodes],
    case ra:delete_cluster(Servers, Timeout) of
        {ok, {_, LeaderNode} = Leader} ->
            MRef = erlang:monitor(process, Leader),
            receive
                {'DOWN', MRef, process, _, _} ->
                    ok
            after Timeout ->
                    ok = force_delete_queue(Servers)
            end,
            notify_decorators(QName, shutdown),
            ok = delete_queue_data(QName, ActingUser),
            rpc:call(LeaderNode, rabbit_core_metrics, queue_deleted, [QName],
                     ?RPC_TIMEOUT),
            {ok, ReadyMsgs};
        {error, {no_more_servers_to_try, Errs}} ->
            case lists:all(fun({{error, noproc}, _}) -> true;
                              (_) -> false
                           end, Errs) of
                true ->
                    %% If all ra nodes were already down, the delete
                    %% has succeed
                    delete_queue_data(QName, ActingUser),
                    {ok, ReadyMsgs};
                false ->
                    %% attempt forced deletion of all servers
                    rabbit_log:warning(
                      "Could not delete quorum queue '~s', not enough nodes "
                       " online to reach a quorum: ~255p."
                       " Attempting force delete.",
                      [rabbit_misc:rs(QName), Errs]),
                    ok = force_delete_queue(Servers),
                    notify_decorators(QName, shutdown),
                    delete_queue_data(QName, ActingUser),
                    {ok, ReadyMsgs}
            end
    end.


force_delete_queue(Servers) ->
    [begin
         case catch(ra:force_delete_server(S)) of
             ok -> ok;
             Err ->
                 rabbit_log:warning(
                   "Force delete of ~w failed with: ~w"
                   "This may require manual data clean up~n",
                   [S, Err]),
                 ok
         end
     end || S <- Servers],
    ok.

delete_queue_data(QName, ActingUser) ->
    _ = rabbit_amqqueue:internal_delete(QName, ActingUser),
    ok.


delete_immediately(Resource, {_Name, _} = QPid) ->
    _ = rabbit_amqqueue:internal_delete(Resource, ?INTERNAL_USER),
    {ok, _} = ra:delete_cluster([QPid]),
    rabbit_core_metrics:queue_deleted(Resource),
    ok.

-spec ack(rabbit_types:ctag(), [msg_id()], rabbit_fifo_client:state()) ->
                 {'ok', rabbit_fifo_client:state()}.

ack(CTag, MsgIds, QState) ->
    rabbit_fifo_client:settle(quorum_ctag(CTag), MsgIds, QState).

-spec reject(Confirm :: boolean(), rabbit_types:ctag(), [msg_id()], rabbit_fifo_client:state()) ->
                    {'ok', rabbit_fifo_client:state()}.

reject(true, CTag, MsgIds, QState) ->
    rabbit_fifo_client:return(quorum_ctag(CTag), MsgIds, QState);
reject(false, CTag, MsgIds, QState) ->
    rabbit_fifo_client:discard(quorum_ctag(CTag), MsgIds, QState).

credit(CTag, Credit, Drain, QState) ->
    rabbit_fifo_client:credit(quorum_ctag(CTag), Credit, Drain, QState).

-spec basic_get(amqqueue:amqqueue(), NoAck :: boolean(), rabbit_types:ctag(),
                rabbit_fifo_client:state()) ->
    {'ok', 'empty', rabbit_fifo_client:state()} |
    {'ok', QLen :: non_neg_integer(), qmsg(), rabbit_fifo_client:state()} |
    {error, timeout | term()}.

basic_get(Q, NoAck, CTag0, QState0) when ?amqqueue_is_quorum(Q) ->
    QName = amqqueue:get_name(Q),
    Id = amqqueue:get_pid(Q),
    CTag = quorum_ctag(CTag0),
    Settlement = case NoAck of
                     true ->
                         settled;
                     false ->
                         unsettled
                 end,
    case rabbit_fifo_client:dequeue(CTag, Settlement, QState0) of
        {ok, empty, QState} ->
            {ok, empty, QState};
        {ok, {{MsgId, {MsgHeader, Msg0}}, MsgsReady}, QState} ->
            Count = case MsgHeader of
                        #{delivery_count := C} -> C;
                       _ -> 0
                    end,
            IsDelivered = Count > 0,
            Msg = rabbit_basic:add_header(<<"x-delivery-count">>, long, Count, Msg0),
            {ok, MsgsReady, {QName, Id, MsgId, IsDelivered, Msg}, QState};
        {error, unsupported} ->
            rabbit_misc:protocol_error(
              resource_locked,
              "cannot obtain access to locked ~s. basic.get operations are not supported by quorum queues with single active consumer",
              [rabbit_misc:rs(QName)]);
        {error, _} = Err ->
            Err;
        {timeout, _} ->
            {error, timeout}
    end.

-spec basic_consume(amqqueue:amqqueue(), NoAck :: boolean(), ChPid :: pid(),
                    ConsumerPrefetchCount :: non_neg_integer(),
                    rabbit_types:ctag(), ExclusiveConsume :: boolean(),
                    Args :: rabbit_framing:amqp_table(), ActingUser :: binary(),
                    any(), rabbit_fifo_client:state()) ->
    {'ok', rabbit_fifo_client:state()} |
    {error, timeout | term()}.

basic_consume(Q, NoAck, ChPid,
              ConsumerPrefetchCount, ConsumerTag0, ExclusiveConsume, Args,
              ActingUser, OkMsg, QState0) when ?amqqueue_is_quorum(Q) ->
    %% TODO: validate consumer arguments
    %% currently quorum queues do not support any arguments
    QName = amqqueue:get_name(Q),
    QPid = amqqueue:get_pid(Q),
    maybe_send_reply(ChPid, OkMsg),
    ConsumerTag = quorum_ctag(ConsumerTag0),
    %% A prefetch count of 0 means no limitation,
    %% let's make it into something large for ra
    Prefetch = case ConsumerPrefetchCount of
                   0 -> 2000;
                   Other -> Other
               end,
    %% consumer info is used to describe the consumer properties
    AckRequired = not NoAck,
    ConsumerMeta = #{ack => AckRequired,
                     prefetch => ConsumerPrefetchCount,
                     args => Args,
                     username => ActingUser},
    {ok, QState} = rabbit_fifo_client:checkout(ConsumerTag,
                                               Prefetch,
                                               ConsumerMeta,
                                               QState0),
    case ra:local_query(QPid,
                        fun rabbit_fifo:query_single_active_consumer/1) of
        {ok, {_, SacResult}, _} ->
            SingleActiveConsumerOn = single_active_consumer_on(Q),
            {IsSingleActiveConsumer, ActivityStatus} = case {SingleActiveConsumerOn, SacResult} of
                                                           {false, _} ->
                                                               {true, up};
                                                           {true, {value, {ConsumerTag, ChPid}}} ->
                                                               {true, single_active};
                                                           _ ->
                                                               {false, waiting}
                                                       end,
            rabbit_core_metrics:consumer_created(
                    ChPid, ConsumerTag, ExclusiveConsume,
                    AckRequired, QName,
                    ConsumerPrefetchCount, IsSingleActiveConsumer,
                    ActivityStatus, Args),
            emit_consumer_created(ChPid, ConsumerTag, ExclusiveConsume,
                    AckRequired, QName, Prefetch,
                    Args, none, ActingUser),
            {ok, QState};
        {error, Error} ->
            Error;
        {timeout, _} ->
            {error, timeout}
    end.

-spec basic_cancel(rabbit_types:ctag(), ChPid :: pid(), any(), rabbit_fifo_client:state()) ->
                          {'ok', rabbit_fifo_client:state()}.

basic_cancel(ConsumerTag, ChPid, OkMsg, QState0) ->
    maybe_send_reply(ChPid, OkMsg),
    rabbit_fifo_client:cancel_checkout(quorum_ctag(ConsumerTag), QState0).

emit_consumer_created(ChPid, CTag, Exclusive, AckRequired, QName, PrefetchCount, Args, Ref, ActingUser) ->
    rabbit_event:notify(consumer_created,
                        [{consumer_tag,   CTag},
                         {exclusive,      Exclusive},
                         {ack_required,   AckRequired},
                         {channel,        ChPid},
                         {queue,          QName},
                         {prefetch_count, PrefetchCount},
                         {arguments,      Args},
                         {user_who_performed_action, ActingUser}],
                        Ref).

emit_consumer_deleted(ChPid, ConsumerTag, QName, ActingUser) ->
    rabbit_event:notify(consumer_deleted,
        [{consumer_tag, ConsumerTag},
            {channel, ChPid},
            {queue, QName},
            {user_who_performed_action, ActingUser}]).

-spec stateless_deliver(amqqueue:ra_server_id(), rabbit_types:delivery()) -> 'ok'.

stateless_deliver(ServerId, Delivery) ->
    ok = rabbit_fifo_client:untracked_enqueue([ServerId],
                                              Delivery#delivery.message).

-spec deliver(Confirm :: boolean(), rabbit_types:delivery(),
              rabbit_fifo_client:state()) ->
    {ok | slow, rabbit_fifo_client:state()} |
    {reject_publish, rabbit_fifo_client:state()}.

deliver(false, Delivery, QState0) ->
    case rabbit_fifo_client:enqueue(Delivery#delivery.message, QState0) of
        {ok, _} = Res -> Res;
        {slow, _} = Res -> Res;
        {reject_publish, State} ->
            {ok, State}
    end;
deliver(true, Delivery, QState0) ->
    Seq = Delivery#delivery.msg_seq_no,
    case rabbit_fifo_client:enqueue(Delivery#delivery.msg_seq_no,
                                    Delivery#delivery.message, QState0) of
        {ok, _} = Res -> Res;
        {slow, _} = Res -> Res;
        {reject_publish, State} ->
            %% TODO: this works fine but once the queue types interface is in
            %% place it could be replaced with an action or similar to avoid
            %% self publishing messages.
            gen_server2:cast(self(), {reject_publish, Seq, undefined}),
            {ok, State}
    end.

-spec info(amqqueue:amqqueue()) -> rabbit_types:infos().

info(Q) ->
    info(Q, [name, durable, auto_delete, arguments, pid, state, messages,
             messages_ready, messages_unacknowledged]).

-spec infos(rabbit_types:r('queue')) -> rabbit_types:infos().

infos(QName) ->
    infos(QName, ?STATISTICS_KEYS).

infos(QName, Keys) ->
    case rabbit_amqqueue:lookup(QName) of
        {ok, Q} ->
            info(Q, Keys);
        {error, not_found} ->
            []
    end.

-spec info(amqqueue:amqqueue(), rabbit_types:info_keys()) -> rabbit_types:infos().

info(Q, Items) ->
    lists:foldr(fun(totals, Acc) ->
                        i_totals(Q) ++ Acc;
                   (type_specific, Acc) ->
                        format(Q) ++ Acc;
                   (Item, Acc) ->
                        [{Item, i(Item, Q)} | Acc]
                end, [], Items).

-spec stat(amqqueue:amqqueue()) -> {'ok', non_neg_integer(), non_neg_integer()}.

stat(Q) when ?is_amqqueue(Q) ->
    %% same short default timeout as in rabbit_fifo_client:stat/1
    stat(Q, 250).

-spec stat(amqqueue:amqqueue(), non_neg_integer()) -> {'ok', non_neg_integer(), non_neg_integer()}.

stat(Q, Timeout) when ?is_amqqueue(Q) ->
    Leader = amqqueue:get_pid(Q),
    try
        case rabbit_fifo_client:stat(Leader, Timeout) of
          {ok, _, _} = Success -> Success;
          {error, _}           -> {ok, 0, 0};
          {timeout, _}         -> {ok, 0, 0}
        end
    catch
        _:_ ->
            %% Leader is not available, cluster might be in minority
            {ok, 0, 0}
    end.

purge(Node) ->
    rabbit_fifo_client:purge(Node).

requeue(ConsumerTag, MsgIds, QState) ->
    rabbit_fifo_client:return(quorum_ctag(ConsumerTag), MsgIds, QState).

cleanup_data_dir() ->
    Names = [begin
                 {Name, _} = amqqueue:get_pid(Q),
                 Name
             end
             || Q <- rabbit_amqqueue:list_by_type(?MODULE),
                lists:member(node(), get_nodes(Q))],
    Registered = ra_directory:list_registered(),
    _ = [maybe_delete_data_dir(UId) || {Name, UId} <- Registered,
                                       not lists:member(Name, Names)],
    ok.

maybe_delete_data_dir(UId) ->
    Dir = ra_env:server_data_dir(UId),
    {ok, Config} = ra_log:read_config(Dir),
    case maps:get(machine, Config) of
        {module, rabbit_fifo, _} ->
            ra_lib:recursive_delete(Dir),
            ra_directory:unregister_name(UId);
        _ ->
            ok
    end.

policy_changed(QName, Server) ->
    {ok, Q} = rabbit_amqqueue:lookup(QName),
    rabbit_fifo_client:update_machine_state(Server, ra_machine_config(Q)).

-spec cluster_state(Name :: atom()) -> 'down' | 'recovering' | 'running'.

cluster_state(Name) ->
    case whereis(Name) of
        undefined -> down;
        _ ->
            case ets:lookup(ra_state, Name) of
                [{_, recover}] -> recovering;
                _ -> running
            end
    end.

-spec status(rabbit_types:vhost(), Name :: rabbit_misc:resource_name()) ->
    [[{binary(), term()}]] | {error, term()}.
status(Vhost, QueueName) ->
    %% Handle not found queues
    QName = #resource{virtual_host = Vhost, name = QueueName, kind = queue},
    case rabbit_amqqueue:lookup(QName) of
        {ok, Q} when ?amqqueue_is_classic(Q) ->
            {error, classic_queue_not_supported};
        {ok, Q} when ?amqqueue_is_quorum(Q) ->
            {RName, _} = amqqueue:get_pid(Q),
            Nodes = get_nodes(Q),
            [begin
                 case get_sys_status({RName, N}) of
                     {ok, Sys} ->
                         {_, M} = lists:keyfind(ra_server_state, 1, Sys),
                         {_, RaftState} = lists:keyfind(raft_state, 1, Sys),
                         #{commit_index := Commit,
                           machine_version := MacVer,
                           current_term := Term,
                           log := #{last_index := Last,
                                    snapshot_index := SnapIdx}} = M,
                         [{<<"Node Name">>, N},
                          {<<"Raft State">>, RaftState},
                          {<<"Log Index">>, Last},
                          {<<"Commit Index">>, Commit},
                          {<<"Snapshot Index">>, SnapIdx},
                          {<<"Term">>, Term},
                          {<<"Machine Version">>, MacVer}
                         ];
                     {error, Err} ->
                         [{<<"Node Name">>, N},
                          {<<"Raft State">>, Err},
                          {<<"Log Index">>, <<>>},
                          {<<"Commit Index">>, <<>>},
                          {<<"Snapshot Index">>, <<>>},
                          {<<"Term">>, <<>>},
                          {<<"Machine Version">>, <<>>}
                         ]
                 end
             end || N <- Nodes];
        {error, not_found} = E ->
            E
    end.

get_sys_status(Proc) ->
    try lists:nth(5, element(4, sys:get_status(Proc))) of
        Sys -> {ok, Sys}
    catch
        _:Err when is_tuple(Err) ->
            {error, element(1, Err)};
        _:_ ->
            {error, other}

    end.


add_member(VHost, Name, Node, Timeout) ->
    QName = #resource{virtual_host = VHost, name = Name, kind = queue},
    case rabbit_amqqueue:lookup(QName) of
        {ok, Q} when ?amqqueue_is_classic(Q) ->
            {error, classic_queue_not_supported};
        {ok, Q} when ?amqqueue_is_quorum(Q) ->
            QNodes = get_nodes(Q),
            case lists:member(Node, rabbit_nodes:all_running()) of
                false ->
                    {error, node_not_running};
                true ->
                    case lists:member(Node, QNodes) of
                        true ->
                          %% idempotent by design
                          ok;
                        false ->
                            add_member(Q, Node, Timeout)
                    end
            end;
        {error, not_found} = E ->
                    E
    end.

add_member(Q, Node, Timeout) when ?amqqueue_is_quorum(Q) ->
    {RaName, _} = amqqueue:get_pid(Q),
    QName = amqqueue:get_name(Q),
    %% TODO parallel calls might crash this, or add a duplicate in quorum_nodes
    ServerId = {RaName, Node},
    Members = members(Q),
    TickTimeout = application:get_env(rabbit, quorum_tick_interval,
                                      ?TICK_TIMEOUT),
    Conf = make_ra_conf(Q, ServerId, TickTimeout),
    case ra:start_server(Conf) of
        ok ->
            case ra:add_member(Members, ServerId, Timeout) of
                {ok, _, Leader} ->
                    Fun = fun(Q1) ->
                                  Q2 = update_type_state(
                                         Q1, fun(#{nodes := Nodes} = Ts) ->
                                                     Ts#{nodes => [Node | Nodes]}
                                             end),
                                  amqqueue:set_pid(Q2, Leader)
                          end,
                    rabbit_misc:execute_mnesia_transaction(
                      fun() -> rabbit_amqqueue:update(QName, Fun) end),
                    ok;
                {timeout, _} ->
                    _ = ra:force_delete_server(ServerId),
                    _ = ra:remove_member(Members, ServerId),
                    {error, timeout};
                E ->
                    _ = ra:force_delete_server(ServerId),
                    E
            end;
        E ->
            E
    end.

delete_member(VHost, Name, Node) ->
    QName = #resource{virtual_host = VHost, name = Name, kind = queue},
    case rabbit_amqqueue:lookup(QName) of
        {ok, Q} when ?amqqueue_is_classic(Q) ->
            {error, classic_queue_not_supported};
        {ok, Q} when ?amqqueue_is_quorum(Q) ->
            QNodes = get_nodes(Q),
            case lists:member(Node, QNodes) of
                false ->
                    %% idempotent by design
                    ok;
                true ->
                    delete_member(Q, Node)
            end;
        {error, not_found} = E ->
                    E
    end.


delete_member(Q, Node) when ?amqqueue_is_quorum(Q) ->
    QName = amqqueue:get_name(Q),
    {RaName, _} = amqqueue:get_pid(Q),
    ServerId = {RaName, Node},
    case members(Q) of
        [{_, Node}] ->

            %% deleting the last member is not allowed
            {error, last_node};
        Members ->
            case ra:remove_member(Members, ServerId) of
                {ok, _, _Leader} ->
                    Fun = fun(Q1) ->
                                  update_type_state(
                                    Q1,
                                    fun(#{nodes := Nodes} = Ts) ->
                                            Ts#{nodes => lists:delete(Node, Nodes)}
                                    end)
                          end,
                    rabbit_misc:execute_mnesia_transaction(
                      fun() -> rabbit_amqqueue:update(QName, Fun) end),
                    case ra:force_delete_server(ServerId) of
                        ok ->
                            ok;
                        {error, {badrpc, nodedown}} ->
                            ok;
                        {error, {badrpc, {'EXIT', {badarg, _}}}} ->
                            %% DETS/ETS tables can't be found, application isn't running
                            ok;
                        {error, _} = Err ->
                            Err;
                        Err ->
                            {error, Err}
                    end;
                {timeout, _} ->
                    {error, timeout};
                E ->
                    E
            end
    end.

-spec shrink_all(node()) ->
    [{rabbit_amqqueue:name(),
      {ok, pos_integer()} | {error, pos_integer(), term()}}].
shrink_all(Node) ->
    [begin
         QName = amqqueue:get_name(Q),
         rabbit_log:info("~s: removing member (replica) on node ~w",
                         [rabbit_misc:rs(QName), Node]),
         Size = length(get_nodes(Q)),
         case delete_member(Q, Node) of
             ok ->
                 {QName, {ok, Size-1}};
             {error, Err} ->
                 rabbit_log:warning("~s: failed to remove member (replica) on node ~w, error: ~w",
                                    [rabbit_misc:rs(QName), Node, Err]),
                 {QName, {error, Size, Err}}
         end
     end || Q <- rabbit_amqqueue:list(),
            amqqueue:get_type(Q) == ?MODULE,
            lists:member(Node, get_nodes(Q))].

-spec grow(node(), binary(), binary(), all | even) ->
    [{rabbit_amqqueue:name(),
      {ok, pos_integer()} | {error, pos_integer(), term()}}].
grow(Node, VhostSpec, QueueSpec, Strategy) ->
    Running = rabbit_nodes:all_running(),
    [begin
         Size = length(get_nodes(Q)),
         QName = amqqueue:get_name(Q),
         rabbit_log:info("~s: adding a new member (replica) on node ~w",
                         [rabbit_misc:rs(QName), Node]),
         case add_member(Q, Node, ?ADD_MEMBER_TIMEOUT) of
             ok ->
                 {QName, {ok, Size + 1}};
             {error, Err} ->
                 rabbit_log:warning(
                   "~s: failed to add member (replica) on node ~w, error: ~w",
                   [rabbit_misc:rs(QName), Node, Err]),
                 {QName, {error, Size, Err}}
         end
     end
     || Q <- rabbit_amqqueue:list(),
        amqqueue:get_type(Q) == ?MODULE,
        %% don't add a member if there is already one on the node
        not lists:member(Node, get_nodes(Q)),
        %% node needs to be running
        lists:member(Node, Running),
        matches_strategy(Strategy, get_nodes(Q)),
        is_match(amqqueue:get_vhost(Q), VhostSpec) andalso
        is_match(get_resource_name(amqqueue:get_name(Q)), QueueSpec) ].

transfer_leadership(Q, Destination) ->
    {RaName, _} = Pid = amqqueue:get_pid(Q),
    case ra:transfer_leadership(Pid, {RaName, Destination}) of
        ok ->
          case ra:members(Pid) of
            {_, _, {_, NewNode}} ->
              {migrated, NewNode};
            {timeout, _} ->
              {not_migrated, ra_members_timeout}
          end;
        already_leader ->
            {not_migrated, already_leader};
        {error, Reason} ->
            {not_migrated, Reason};
        {timeout, _} ->
            %% TODO should we retry once?
            {not_migrated, timeout}
    end.

queue_length(Q) ->
    Name = amqqueue:get_name(Q),
    case ets:lookup(ra_metrics, Name) of
        [] -> 0;
        [{_, _, SnapIdx, _, _, LastIdx, _}] -> LastIdx - SnapIdx
    end.

get_replicas(Q) ->
    get_nodes(Q).

get_resource_name(#resource{name  = Name}) ->
    Name.

matches_strategy(all, _) -> true;
matches_strategy(even, Members) ->
    length(Members) rem 2 == 0.

is_match(Subj, E) ->
   nomatch /= re:run(Subj, E).

file_handle_leader_reservation(QName) ->
    {ok, Q} = rabbit_amqqueue:lookup(QName),
    ClusterSize = length(get_nodes(Q)),
    file_handle_cache:set_reservation(2 + ClusterSize).

file_handle_other_reservation() ->
    file_handle_cache:set_reservation(2).

file_handle_release_reservation() ->
    file_handle_cache:release_reservation().

-spec reclaim_memory(rabbit_types:vhost(), Name :: rabbit_misc:resource_name()) -> ok | {error, term()}.
reclaim_memory(Vhost, QueueName) ->
    QName = #resource{virtual_host = Vhost, name = QueueName, kind = queue},
    case rabbit_amqqueue:lookup(QName) of
        {ok, Q} when ?amqqueue_is_classic(Q) ->
            {error, classic_queue_not_supported};
        {ok, Q} when ?amqqueue_is_quorum(Q) ->
            ok = ra:pipeline_command(amqqueue:get_pid(Q),
                                     rabbit_fifo:make_garbage_collection());
        {error, not_found} = E ->
            E
    end.

%%----------------------------------------------------------------------------
dlx_mfa(Q) ->
    DLX = init_dlx(args_policy_lookup(<<"dead-letter-exchange">>,
                                      fun res_arg/2, Q), Q),
    DLXRKey = args_policy_lookup(<<"dead-letter-routing-key">>,
                                 fun res_arg/2, Q),
    {?MODULE, dead_letter_publish, [DLX, DLXRKey, amqqueue:get_name(Q)]}.

init_dlx(undefined, _Q) ->
    undefined;
init_dlx(DLX, Q) when ?is_amqqueue(Q) ->
    QName = amqqueue:get_name(Q),
    rabbit_misc:r(QName, exchange, DLX).

res_arg(_PolVal, ArgVal) -> ArgVal.

args_policy_lookup(Name, Resolve, Q) when ?is_amqqueue(Q) ->
    Args = amqqueue:get_arguments(Q),
    AName = <<"x-", Name/binary>>,
    case {rabbit_policy:get(Name, Q), rabbit_misc:table_lookup(Args, AName)} of
        {undefined, undefined}       -> undefined;
        {undefined, {_Type, Val}}    -> Val;
        {Val,       undefined}       -> Val;
        {PolVal,    {_Type, ArgVal}} -> Resolve(PolVal, ArgVal)
    end.

dead_letter_publish(undefined, _, _, _) ->
    ok;
dead_letter_publish(X, RK, QName, ReasonMsgs) ->
    case rabbit_exchange:lookup(X) of
        {ok, Exchange} ->
            [rabbit_dead_letter:publish(Msg, Reason, Exchange, RK, QName)
             || {Reason, Msg} <- ReasonMsgs];
        {error, not_found} ->
            ok
    end.

%% TODO escape hack
qname_to_internal_name(QName) ->
    case name_concat(QName) of
        Name when byte_size(Name) =< 255 ->
            {ok, erlang:binary_to_atom(Name)};
        Name ->
            {error, {too_long, Name}}
    end.

name_concat(#resource{virtual_host = <<"/">>, name = Name}) ->
    <<"%2F_", Name/binary>>;
name_concat(#resource{virtual_host = VHost, name = Name}) ->
    <<VHost/binary, "_", Name/binary>>.

find_quorum_queues(VHost) ->
    Node = node(),
    mnesia:async_dirty(
      fun () ->
              qlc:e(qlc:q([Q || Q <- mnesia:table(rabbit_durable_queue),
                                ?amqqueue_is_quorum(Q),
                                amqqueue:get_vhost(Q) =:= VHost,
                                amqqueue:qnode(Q) == Node]))
      end).

i_totals(Q) when ?is_amqqueue(Q) ->
    QName = amqqueue:get_name(Q),
    case ets:lookup(queue_coarse_metrics, QName) of
        [{_, MR, MU, M, _}] ->
            [{messages_ready, MR},
             {messages_unacknowledged, MU},
             {messages, M}];
        [] ->
            [{messages_ready, 0},
             {messages_unacknowledged, 0},
             {messages, 0}]
    end.

i(name,        Q) when ?is_amqqueue(Q) -> amqqueue:get_name(Q);
i(durable,     Q) when ?is_amqqueue(Q) -> amqqueue:is_durable(Q);
i(auto_delete, Q) when ?is_amqqueue(Q) -> amqqueue:is_auto_delete(Q);
i(arguments,   Q) when ?is_amqqueue(Q) -> amqqueue:get_arguments(Q);
i(pid, Q) when ?is_amqqueue(Q) ->
    {Name, _} = amqqueue:get_pid(Q),
    whereis(Name);
i(messages, Q) when ?is_amqqueue(Q) ->
    QName = amqqueue:get_name(Q),
    quorum_messages(QName);
i(messages_ready, Q) when ?is_amqqueue(Q) ->
    QName = amqqueue:get_name(Q),
    case ets:lookup(queue_coarse_metrics, QName) of
        [{_, MR, _, _, _}] ->
            MR;
        [] ->
            0
    end;
i(messages_unacknowledged, Q) when ?is_amqqueue(Q) ->
    QName = amqqueue:get_name(Q),
    case ets:lookup(queue_coarse_metrics, QName) of
        [{_, _, MU, _, _}] ->
            MU;
        [] ->
            0
    end;
i(policy, Q) ->
    case rabbit_policy:name(Q) of
        none   -> '';
        Policy -> Policy
    end;
i(operator_policy, Q) ->
    case rabbit_policy:name_op(Q) of
        none   -> '';
        Policy -> Policy
    end;
i(effective_policy_definition, Q) ->
    case rabbit_policy:effective_definition(Q) of
        undefined -> [];
        Def       -> Def
    end;
i(consumers, Q) when ?is_amqqueue(Q) ->
    QName = amqqueue:get_name(Q),
    case ets:lookup(queue_metrics, QName) of
        [{_, M, _}] ->
            proplists:get_value(consumers, M, 0);
        [] ->
            0
    end;
i(memory, Q) when ?is_amqqueue(Q) ->
    {Name, _} = amqqueue:get_pid(Q),
    try
        {memory, M} = process_info(whereis(Name), memory),
        M
    catch
        error:badarg ->
            0
    end;
i(state, Q) when ?is_amqqueue(Q) ->
    {Name, Node} = amqqueue:get_pid(Q),
    %% Check against the leader or last known leader
    case rpc:call(Node, ?MODULE, cluster_state, [Name], ?RPC_TIMEOUT) of
        {badrpc, _} -> down;
        State -> State
    end;
i(local_state, Q) when ?is_amqqueue(Q) ->
    {Name, _} = amqqueue:get_pid(Q),
    case ets:lookup(ra_state, Name) of
        [{_, State}] -> State;
        _ -> not_member
    end;
i(garbage_collection, Q) when ?is_amqqueue(Q) ->
    {Name, _} = amqqueue:get_pid(Q),
    try
        rabbit_misc:get_gc_info(whereis(Name))
    catch
        error:badarg ->
            []
    end;
i(members, Q) when ?is_amqqueue(Q) ->
    get_nodes(Q);
i(online, Q) -> online(Q);
i(leader, Q) -> leader(Q);
i(open_files, Q) when ?is_amqqueue(Q) ->
    {Name, _} = amqqueue:get_pid(Q),
    Nodes = get_nodes(Q),
    {Data, _} = rpc:multicall(Nodes, ?MODULE, open_files, [Name]),
    lists:flatten(Data);
i(single_active_consumer_pid, Q) when ?is_amqqueue(Q) ->
    QPid = amqqueue:get_pid(Q),
    case ra:local_query(QPid, fun rabbit_fifo:query_single_active_consumer/1) of
        {ok, {_, {value, {_ConsumerTag, ChPid}}}, _} ->
            ChPid;
        {ok, _, _} ->
            '';
        {error, _} ->
            '';
        {timeout, _} ->
            ''
    end;
i(single_active_consumer_ctag, Q) when ?is_amqqueue(Q) ->
    QPid = amqqueue:get_pid(Q),
    case ra:local_query(QPid,
                        fun rabbit_fifo:query_single_active_consumer/1) of
        {ok, {_, {value, {ConsumerTag, _ChPid}}}, _} ->
            ConsumerTag;
        {ok, _, _} ->
            '';
        {error, _} ->
            '';
        {timeout, _} ->
            ''
    end;
i(type, _) -> quorum;
i(messages_ram, Q) when ?is_amqqueue(Q) ->
    QPid = amqqueue:get_pid(Q),
    case ra:local_query(QPid,
                        fun rabbit_fifo:query_in_memory_usage/1) of
        {ok, {_, {Length, _}}, _} ->
            Length;
        {error, _} ->
            0;
        {timeout, _} ->
            0
    end;
i(message_bytes_ram, Q) when ?is_amqqueue(Q) ->
    QPid = amqqueue:get_pid(Q),
    case ra:local_query(QPid,
                        fun rabbit_fifo:query_in_memory_usage/1) of
        {ok, {_, {_, Bytes}}, _} ->
            Bytes;
        {error, _} ->
            0;
        {timeout, _} ->
            0
    end;
i(_K, _Q) -> ''.

open_files(Name) ->
    case whereis(Name) of
        undefined -> {node(), 0};
        Pid -> case ets:lookup(ra_open_file_metrics, Pid) of
                   [] -> {node(), 0};
                   [{_, Count}] -> {node(), Count}
               end
    end.

leader(Q) when ?is_amqqueue(Q) ->
    {Name, Leader} = amqqueue:get_pid(Q),
    case is_process_alive(Name, Leader) of
        true -> Leader;
        false -> ''
    end.

peek(Vhost, Queue, Pos) ->
    peek(Pos, rabbit_misc:r(Vhost, queue, Queue)).

peek(Pos, #resource{} = QName) ->
    case rabbit_amqqueue:lookup(QName) of
        {ok, Q} ->
            peek(Pos, Q);
        Err ->
            Err
    end;
peek(Pos, Q) when ?is_amqqueue(Q) andalso ?amqqueue_is_quorum(Q) ->
    LeaderPid = amqqueue:get_pid(Q),
    case ra:aux_command(LeaderPid, {peek, Pos}) of
        {ok, {MsgHeader, Msg0}} ->
            Count = case MsgHeader of
                        #{delivery_count := C} -> C;
                       _ -> 0
                    end,
            Msg = rabbit_basic:add_header(<<"x-delivery-count">>, long,
                                          Count, Msg0),
            {ok, rabbit_basic:peek_fmt_message(Msg)};
        {error, Err} ->
            {error, Err};
        Err ->
            Err
    end;
peek(_Pos, Q) when ?is_amqqueue(Q) andalso ?amqqueue_is_classic(Q) ->
    {error, classic_queue_not_supported}.

online(Q) when ?is_amqqueue(Q) ->
    Nodes = get_nodes(Q),
    {Name, _} = amqqueue:get_pid(Q),
    [Node || Node <- Nodes, is_process_alive(Name, Node)].

format(Q) when ?is_amqqueue(Q) ->
    Nodes = get_nodes(Q),
    [{members, Nodes}, {online, online(Q)}, {leader, leader(Q)}].

is_process_alive(Name, Node) ->
    erlang:is_pid(rpc:call(Node, erlang, whereis, [Name], ?RPC_TIMEOUT)).

-spec quorum_messages(rabbit_amqqueue:name()) -> non_neg_integer().

quorum_messages(QName) ->
    case ets:lookup(queue_coarse_metrics, QName) of
        [{_, _, _, M, _}] ->
            M;
        [] ->
            0
    end.

quorum_ctag(Int) when is_integer(Int) ->
    integer_to_binary(Int);
quorum_ctag(Other) ->
    Other.

maybe_send_reply(_ChPid, undefined) -> ok;
maybe_send_reply(ChPid, Msg) -> ok = rabbit_channel:send_command(ChPid, Msg).

check_invalid_arguments(QueueName, Args) ->
    Keys = [<<"x-message-ttl">>,
            <<"x-max-priority">>, <<"x-queue-mode">>],
    [case rabbit_misc:table_lookup(Args, Key) of
         undefined -> ok;
         _TypeVal   -> rabbit_misc:protocol_error(
                         precondition_failed,
                         "invalid arg '~s' for ~s",
                         [Key, rabbit_misc:rs(QueueName)])
     end || Key <- Keys],

    case rabbit_misc:table_lookup(Args, <<"x-overflow">>) of
        undefined -> ok;
        {_, <<"reject-publish-dlx">>} ->
            rabbit_misc:protocol_error(
              precondition_failed,
              "invalid arg 'x-overflow' with value 'reject-publish-dlx' for ~s",
              [rabbit_misc:rs(QueueName)]);
        _ ->
            ok
    end,
    ok.

check_auto_delete(Q) when ?amqqueue_is_auto_delete(Q) ->
    Name = amqqueue:get_name(Q),
    rabbit_misc:protocol_error(
      precondition_failed,
      "invalid property 'auto-delete' for ~s",
      [rabbit_misc:rs(Name)]);
check_auto_delete(_) ->
    ok.

check_exclusive(Q) when ?amqqueue_exclusive_owner_is(Q, none) ->
    ok;
check_exclusive(Q) when ?is_amqqueue(Q) ->
    Name = amqqueue:get_name(Q),
    rabbit_misc:protocol_error(
      precondition_failed,
      "invalid property 'exclusive-owner' for ~s",
      [rabbit_misc:rs(Name)]).

check_non_durable(Q) when ?amqqueue_is_durable(Q) ->
    ok;
check_non_durable(Q) when not ?amqqueue_is_durable(Q) ->
    Name = amqqueue:get_name(Q),
    rabbit_misc:protocol_error(
      precondition_failed,
      "invalid property 'non-durable' for ~s",
      [rabbit_misc:rs(Name)]).

queue_name(RaFifoState) ->
    rabbit_fifo_client:cluster_name(RaFifoState).

get_default_quorum_initial_group_size(Arguments) ->
    case rabbit_misc:table_lookup(Arguments, <<"x-quorum-initial-group-size">>) of
        undefined ->
            application:get_env(rabbit, quorum_cluster_size, 3);
        {_Type, Val} ->
            Val
    end.

select_quorum_nodes(Size, All) when length(All) =< Size ->
    All;
select_quorum_nodes(Size, All) ->
    Node = node(),
    case lists:member(Node, All) of
        true ->
            select_quorum_nodes(Size - 1, lists:delete(Node, All), [Node]);
        false ->
            select_quorum_nodes(Size, All, [])
    end.

select_quorum_nodes(0, _, Selected) ->
    Selected;
select_quorum_nodes(Size, Rest, Selected) ->
    S = lists:nth(rand:uniform(length(Rest)), Rest),
    select_quorum_nodes(Size - 1, lists:delete(S, Rest), [S | Selected]).

%% member with the current leader first
members(Q) when ?amqqueue_is_quorum(Q) ->
    {RaName, LeaderNode} = amqqueue:get_pid(Q),
    Nodes = lists:delete(LeaderNode, get_nodes(Q)),
    [{RaName, N} || N <- [LeaderNode | Nodes]].

make_ra_conf(Q, ServerId, TickTimeout) ->
    QName = amqqueue:get_name(Q),
    RaMachine = ra_machine(Q),
    [{ClusterName, _} | _] = Members = members(Q),
    UId = ra:new_uid(ra_lib:to_binary(ClusterName)),
    FName = rabbit_misc:rs(QName),
    #{cluster_name => ClusterName,
      id => ServerId,
      uid => UId,
      friendly_name => FName,
      metrics_key => QName,
      initial_members => Members,
      log_init_args => #{uid => UId},
      tick_timeout => TickTimeout,
      machine => RaMachine}.

get_nodes(Q) when ?is_amqqueue(Q) ->
    #{nodes := Nodes} = amqqueue:get_type_state(Q),
    Nodes.

update_type_state(Q, Fun) when ?is_amqqueue(Q) ->
    Ts = amqqueue:get_type_state(Q),
    amqqueue:set_type_state(Q, Fun(Ts)).

overflow(undefined, Def) -> Def;
overflow(<<"reject-publish">>, _Def) -> reject_publish;
overflow(<<"drop-head">>, _Def) -> drop_head.

-spec notify_decorators(amqqueue:amqqueue()) -> 'ok'.
notify_decorators(Q) when ?is_amqqueue(Q) ->
    QName = amqqueue:get_name(Q),
    QPid = amqqueue:get_pid(Q),
    case ra:local_query(QPid, fun rabbit_fifo:query_notify_decorators_info/1) of
        {ok, {_, {MaxActivePriority, IsEmpty}}, _} ->
            notify_decorators(QName, consumer_state_changed, [MaxActivePriority, IsEmpty]);
        _ -> ok
    end.

notify_decorators(QName, Event) ->
    notify_decorators(QName, Event, []).

notify_decorators(QName, F, A) ->
    %% Look up again in case policy and hence decorators have changed
    case rabbit_amqqueue:lookup(QName) of
        {ok, Q} ->
            Ds = amqqueue:get_decorators(Q),
            [ok = apply(M, F, [Q|A]) || M <- rabbit_queue_decorator:select(Ds)];
        {error, not_found} ->
            ok
    end.
