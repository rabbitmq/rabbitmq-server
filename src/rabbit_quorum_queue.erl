%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2018-2019 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_quorum_queue).

-export([init_state/2, handle_event/2]).
-export([declare/1, recover/1, stop/1, delete/4, delete_immediately/2]).
-export([info/1, info/2, stat/1, infos/1]).
-export([ack/3, reject/4, basic_get/4, basic_consume/10, basic_cancel/4]).
-export([credit/4]).
-export([purge/1]).
-export([stateless_deliver/2, deliver/3]).
-export([dead_letter_publish/4]).
-export([queue_name/1]).
-export([cluster_state/1, status/2]).
-export([update_consumer_handler/8, update_consumer/9]).
-export([cancel_consumer_handler/2, cancel_consumer/3]).
-export([become_leader/2, handle_tick/2]).
-export([rpc_delete_metrics/1]).
-export([format/1]).
-export([open_files/1]).
-export([add_member/3]).
-export([delete_member/3]).
-export([requeue/3]).
-export([policy_changed/2]).
-export([cleanup_data_dir/0]).
-export([shrink_all/1,
         grow/4]).

%%-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("rabbit.hrl").
-include_lib("stdlib/include/qlc.hrl").
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
         single_active_consumer_ctag
        ]).

-define(RPC_TIMEOUT, 1000).
-define(TICK_TIMEOUT, 5000). %% the ra server tick time
-define(DELETE_TIMEOUT, 5000).

%%----------------------------------------------------------------------------

-spec init_state(amqqueue:ra_server_id(), rabbit_amqqueue:name()) ->
    rabbit_fifo_client:state().
init_state({Name, _}, QName = #resource{}) ->
    {ok, SoftLimit} = application:get_env(rabbit, quorum_commands_soft_limit),
    %% This lookup could potentially return an {error, not_found}, but we do not
    %% know what to do if the queue has `disappeared`. Let it crash.
    {ok, Q} = rabbit_amqqueue:lookup(QName),
    Leader = amqqueue:get_pid(Q),
    Nodes = amqqueue:get_quorum_nodes(Q),
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
    RaName = qname_to_rname(QName),
    Id = {RaName, node()},
    Nodes = select_quorum_nodes(QuorumSize, rabbit_mnesia:cluster_nodes(all)),
    NewQ0 = amqqueue:set_pid(Q, Id),
    NewQ1 = amqqueue:set_quorum_nodes(NewQ0, Nodes),
    case rabbit_amqqueue:internal_declare(NewQ1, false) of
        {created, NewQ} ->
            RaMachine = ra_machine(NewQ),
            ServerIds = [{RaName, Node} || Node <- Nodes],
            ClusterName = RaName,
            RaConfs = [begin
                           UId = ra:new_uid(ra_lib:to_binary(ClusterName)),
                           FName = rabbit_misc:rs(QName),
                           #{cluster_name => ClusterName,
                             id => ServerId,
                             uid => UId,
                             friendly_name => FName,
                             initial_members => ServerIds,
                             log_init_args => #{uid => UId},
                             tick_timeout => ?TICK_TIMEOUT,
                             machine => RaMachine}
                       end || ServerId <- ServerIds],

            case ra:start_cluster(RaConfs) of
                {ok, _, _} ->
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
    MaxBytes = args_policy_lookup(<<"max-length-bytes">>, fun min/2, Q),
    DeliveryLimit = args_policy_lookup(<<"delivery-limit">>, fun min/2, Q),
    #{name => Name,
      queue_resource => QName,
      dead_letter_handler => dlx_mfa(Q),
      become_leader_handler => {?MODULE, become_leader, [QName]},
      max_length => MaxLength,
      max_bytes => MaxBytes,
      single_active_consumer_on => single_active_consumer_on(Q),
      delivery_limit => DeliveryLimit
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
    local_or_remote_handler(ChPid, rabbit_quorum_queue, cancel_consumer, [QName, ChPid, ConsumerTag]).

cancel_consumer(QName, ChPid, ConsumerTag) ->
    catch rabbit_core_metrics:consumer_deleted(ChPid, ConsumerTag, QName),
    rabbit_event:notify(consumer_deleted,
                        [{consumer_tag, ConsumerTag},
                         {channel,      ChPid},
                         {queue,        QName},
                         {user_who_performed_action, ?INTERNAL_USER}]).

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
                          Nodes = amqqueue:get_quorum_nodes(Q0),
                          [rpc:call(Node, ?MODULE, rpc_delete_metrics,
                                    [QName], ?RPC_TIMEOUT)
                           || Node <- Nodes, Node =/= node()];
                      _ ->
                          ok
                  end
          end).

rpc_delete_metrics(QName) ->
    ets:delete(queue_coarse_metrics, QName),
    ets:delete(queue_metrics, QName),
    ok.

handle_tick(QName, {Name, MR, MU, M, C, MsgBytesReady, MsgBytesUnack}) ->
    %% this makes calls to remote processes so cannot be run inside the
    %% ra server
    _ = spawn(fun() ->
                      R = reductions(Name),
                      rabbit_core_metrics:queue_stats(QName, MR, MU, M, R),
                      Util = case C of
                                 0 -> 0;
                                 _ -> rabbit_fifo:usage(Name)
                             end,
                      Infos = [{consumers, C}, {consumer_utilisation, Util},
                               {message_bytes_ready, MsgBytesReady},
                               {message_bytes_unacknowledged, MsgBytesUnack},
                               {message_bytes, MsgBytesReady + MsgBytesUnack}
                               | infos(QName)],
                      rabbit_core_metrics:queue_stats(QName, Infos),
                      rabbit_event:notify(queue_stats,
                                          Infos ++ [{name, QName},
                                                    {messages, M},
                                                    {messages_ready, MR},
                                                    {messages_unacknowledged, MU},
                                                    {reductions, R}])
              end),
    ok = repair_leader_record(QName),
    ok.

repair_leader_record(QName) ->
    {ok, Q} = rabbit_amqqueue:lookup(QName),
    Node = node(),
    case amqqueue:get_pid(Q) of
        {_, Node} ->
            %% it's ok - we don't need to do anything
            ok;
        _ ->
            rabbit_log:debug("~s: repairing leader record",
                             [rabbit_misc:rs(QName)]),
            {_, Name} = erlang:process_info(self(), registered_name),
            become_leader(QName, Name)
    end,
    ok.



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
         Nodes = amqqueue:get_quorum_nodes(Q0),
         case ra:restart_server({Name, node()}) of
             ok ->
                 % queue was restarted, good
                 ok;
             {error, Err1}
               when Err1 == not_started orelse
                    Err1 == name_not_registered ->
                 % queue was never started on this node
                 % so needs to be started from scratch.
                 Machine = ra_machine(Q0),
                 RaNodes = [{Name, Node} || Node <- Nodes],
                 case ra:start_server(Name, {Name, node()}, Machine, RaNodes) of
                     ok -> ok;
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

delete(Q,
       _IfUnused, _IfEmpty, ActingUser) when ?amqqueue_is_quorum(Q) ->
    {Name, _} = amqqueue:get_pid(Q),
    QName = amqqueue:get_name(Q),
    QNodes = amqqueue:get_quorum_nodes(Q),
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
            Count = maps:get(delivery_count, MsgHeader, 0),
            IsDelivered = Count > 0,
            Msg = rabbit_basic:add_header(<<"x-delivery-count">>, long, Count, Msg0),
            {ok, MsgsReady, {QName, Id, MsgId, IsDelivered, Msg}, QState};
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
    {'ok', rabbit_fifo_client:state()}.

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
    ConsumerMeta = #{ack => not NoAck,
                     prefetch => ConsumerPrefetchCount,
                     args => Args,
                     username => ActingUser},
    {ok, QState} = rabbit_fifo_client:checkout(ConsumerTag,
                                               Prefetch,
                                               ConsumerMeta,
                                               QState0),
    {ok, {_, SacResult}, _} = ra:local_query(QPid,
                                             fun rabbit_fifo:query_single_active_consumer/1),

    SingleActiveConsumerOn = single_active_consumer_on(Q),
    {IsSingleActiveConsumer, ActivityStatus} = case {SingleActiveConsumerOn, SacResult} of
                                                   {false, _} ->
                                                       {true, up};
                                                   {true, {value, {ConsumerTag, ChPid}}} ->
                                                       {true, single_active};
                                                   _ ->
                                                       {false, waiting}
                                               end,

    %% TODO: emit as rabbit_fifo effect
    rabbit_core_metrics:consumer_created(ChPid, ConsumerTag, ExclusiveConsume,
                                         not NoAck, QName,
                                         ConsumerPrefetchCount, IsSingleActiveConsumer,
                                         ActivityStatus, Args),
    {ok, QState}.

-spec basic_cancel(rabbit_types:ctag(), ChPid :: pid(), any(), rabbit_fifo_client:state()) ->
                          {'ok', rabbit_fifo_client:state()}.

basic_cancel(ConsumerTag, ChPid, OkMsg, QState0) ->
    maybe_send_reply(ChPid, OkMsg),
    rabbit_fifo_client:cancel_checkout(quorum_ctag(ConsumerTag), QState0).

-spec stateless_deliver(amqqueue:ra_server_id(), rabbit_types:delivery()) -> 'ok'.

stateless_deliver(ServerId, Delivery) ->
    ok = rabbit_fifo_client:untracked_enqueue([ServerId],
                                              Delivery#delivery.message).

-spec deliver(Confirm :: boolean(), rabbit_types:delivery(),
              rabbit_fifo_client:state()) ->
    {ok | slow, rabbit_fifo_client:state()}.

deliver(false, Delivery, QState0) ->
    rabbit_fifo_client:enqueue(Delivery#delivery.message, QState0);
deliver(true, Delivery, QState0) ->
    rabbit_fifo_client:enqueue(Delivery#delivery.msg_seq_no,
                               Delivery#delivery.message, QState0).

-spec info(amqqueue:amqqueue()) -> rabbit_types:infos().

info(Q) ->
    info(Q, [name, durable, auto_delete, arguments, pid, state, messages,
             messages_ready, messages_unacknowledged]).

-spec infos(rabbit_types:r('queue')) -> rabbit_types:infos().

infos(QName) ->
    case rabbit_amqqueue:lookup(QName) of
        {ok, Q} ->
            info(Q, ?STATISTICS_KEYS);
        {error, not_found} ->
            []
    end.

-spec info(amqqueue:amqqueue(), rabbit_types:info_keys()) -> rabbit_types:infos().

info(Q, Items) ->
    [{Item, i(Item, Q)} || Item <- Items].

-spec stat(amqqueue:amqqueue()) -> {'ok', non_neg_integer(), non_neg_integer()}.

stat(Q) when ?is_amqqueue(Q) ->
    Leader = amqqueue:get_pid(Q),
    try
        {ok, _, _} = rabbit_fifo_client:stat(Leader)
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
             || Q <- rabbit_amqqueue:list_by_type(quorum),
                lists:member(node(), amqqueue:get_quorum_nodes(Q))],
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

policy_changed(QName, Node) ->
    {ok, Q} = rabbit_amqqueue:lookup(QName),
    rabbit_fifo_client:update_machine_state(Node, ra_machine_config(Q)).

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

-spec status(rabbit_types:vhost(), Name :: rabbit_misc:resource_name()) -> rabbit_types:infos() | {error, term()}.

status(Vhost, QueueName) ->
    %% Handle not found queues
    QName = #resource{virtual_host = Vhost, name = QueueName, kind = queue},
    RName = qname_to_rname(QName),
    case rabbit_amqqueue:lookup(QName) of
        {ok, Q} when ?amqqueue_is_classic(Q) ->
            {error, classic_queue_not_supported};
        {ok, Q} when ?amqqueue_is_quorum(Q) ->
            {_, Leader} = amqqueue:get_pid(Q),
            Nodes = amqqueue:get_quorum_nodes(Q),
            Info = [{leader, Leader}, {members, Nodes}],
            case ets:lookup(ra_state, RName) of
                [{_, State}] ->
                    [{local_state, State} | Info];
                [] ->
                    Info
            end;
        {error, not_found} = E ->
            E
    end.

add_member(VHost, Name, Node) ->
    QName = #resource{virtual_host = VHost, name = Name, kind = queue},
    case rabbit_amqqueue:lookup(QName) of
        {ok, Q} when ?amqqueue_is_classic(Q) ->
            {error, classic_queue_not_supported};
        {ok, Q} when ?amqqueue_is_quorum(Q) ->
            QNodes = amqqueue:get_quorum_nodes(Q),
            case lists:member(Node, rabbit_mnesia:cluster_nodes(running)) of
                false ->
                    {error, node_not_running};
                true ->
                    case lists:member(Node, QNodes) of
                        true ->
                            {error, already_a_member};
                        false ->
                            add_member(Q, Node)
                    end
            end;
        {error, not_found} = E ->
                    E
    end.

add_member(Q, Node) when ?amqqueue_is_quorum(Q) ->
    {RaName, _} = ServerRef = amqqueue:get_pid(Q),
    QName = amqqueue:get_name(Q),
    QNodes = amqqueue:get_quorum_nodes(Q),
    %% TODO parallel calls might crash this, or add a duplicate in quorum_nodes
    ServerId = {RaName, Node},
    case ra:start_server(RaName, ServerId, ra_machine(Q),
                         [{RaName, N} || N <- QNodes]) of
        ok ->
            case ra:add_member(ServerRef, ServerId) of
                {ok, _, Leader} ->
                    Fun = fun(Q1) ->
                                  Q2 = amqqueue:set_quorum_nodes(
                                         Q1,
                                         [Node | amqqueue:get_quorum_nodes(Q1)]),
                                  amqqueue:set_pid(Q2, Leader)
                          end,
                    rabbit_misc:execute_mnesia_transaction(
                      fun() -> rabbit_amqqueue:update(QName, Fun) end),
                    ok;
                {timeout, _} ->
                    {error, timeout};
                E ->
                    %% TODO should we stop the ra process here?
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
            QNodes = amqqueue:get_quorum_nodes(Q),
            case lists:member(Node, rabbit_mnesia:cluster_nodes(running)) of
                false ->
                    {error, node_not_running};
                true ->
                    case lists:member(Node, QNodes) of
                        false ->
                            {error, not_a_member};
                        true ->
                            delete_member(Q, Node)
                    end
            end;
        {error, not_found} = E ->
                    E
    end.

delete_member(Q, Node) when ?amqqueue_is_quorum(Q) ->
    QName = amqqueue:get_name(Q),
    {RaName, _} = amqqueue:get_pid(Q),
    ServerId = {RaName, Node},
    case amqqueue:get_quorum_nodes(Q) of
        [Node] ->
            %% deleting the last member is not allowed
            {error, last_node};
        _ ->
            case ra:leave_and_delete_server(ServerId) of
                ok ->
                    Fun = fun(Q1) ->
                                  amqqueue:set_quorum_nodes(
                                    Q1,
                                    lists:delete(Node,
                                                 amqqueue:get_quorum_nodes(Q1)))
                          end,
                    rabbit_misc:execute_mnesia_transaction(
                      fun() -> rabbit_amqqueue:update(QName, Fun) end),
                    ok;
                timeout ->
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
         Size = length(amqqueue:get_quorum_nodes(Q)),
         case delete_member(Q, Node) of
             ok ->
                 {QName, {ok, Size-1}};
             {error, Err} ->
                 rabbit_log:warning("~s: failed to remove member (replica) on node ~w, error: ~w",
                                    [rabbit_misc:rs(QName), Node, Err]),
                 {QName, {error, Size, Err}}
         end
     end || Q <- rabbit_amqqueue:list(),
            amqqueue:get_type(Q) == quorum,
            lists:member(Node, amqqueue:get_quorum_nodes(Q))].

-spec grow(node(), binary(), binary(), all | even) ->
    [{rabbit_amqqueue:name(),
      {ok, pos_integer()} | {error, pos_integer(), term()}}].
grow(Node, VhostSpec, QueueSpec, Strategy) ->
    Running = rabbit_mnesia:cluster_nodes(running),
    [begin
         Size = length(amqqueue:get_quorum_nodes(Q)),
         QName = amqqueue:get_name(Q),
         rabbit_log:info("~s: adding a new member (replica) on node ~w",
                         [rabbit_misc:rs(QName), Node]),
         case add_member(Q, Node) of
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
        amqqueue:get_type(Q) == quorum,
        %% don't add a member if there is already one on the node
        not lists:member(Node, amqqueue:get_quorum_nodes(Q)),
        %% node needs to be running
        lists:member(Node, Running),
        matches_strategy(Strategy, amqqueue:get_quorum_nodes(Q)),
        is_match(amqqueue:get_vhost(Q), VhostSpec) andalso
        is_match(get_resource_name(amqqueue:get_name(Q)), QueueSpec) ].

get_resource_name(#resource{name  = Name}) ->
    Name.

matches_strategy(all, _) -> true;
matches_strategy(even, Members) ->
    length(Members) rem 2 == 0.

is_match(Subj, E) ->
   nomatch /= re:run(Subj, E).


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
qname_to_rname(#resource{virtual_host = <<"/">>, name = Name}) ->
    erlang:binary_to_atom(<<"%2F_", Name/binary>>, utf8);
qname_to_rname(#resource{virtual_host = VHost, name = Name}) ->
    erlang:binary_to_atom(<<VHost/binary, "_", Name/binary>>, utf8).

find_quorum_queues(VHost) ->
    Node = node(),
    mnesia:async_dirty(
      fun () ->
              qlc:e(qlc:q([Q || Q <- mnesia:table(rabbit_durable_queue),
                                ?amqqueue_is_quorum(Q),
                                amqqueue:get_vhost(Q) =:= VHost,
                                amqqueue:qnode(Q) == Node]))
      end).

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
    amqqueue:get_quorum_nodes(Q);
i(online, Q) -> online(Q);
i(leader, Q) -> leader(Q);
i(open_files, Q) when ?is_amqqueue(Q) ->
    {Name, _} = amqqueue:get_pid(Q),
    Nodes = amqqueue:get_quorum_nodes(Q),
    {Data, _} = rpc:multicall(Nodes, rabbit_quorum_queue, open_files, [Name]),
    lists:flatten(Data);
i(single_active_consumer_pid, Q) when ?is_amqqueue(Q) ->
    QPid = amqqueue:get_pid(Q),
    {ok, {_, SacResult}, _} = ra:local_query(QPid,
                                             fun rabbit_fifo:query_single_active_consumer/1),
    case SacResult of
        {value, {_ConsumerTag, ChPid}} ->
            ChPid;
        _ ->
            ''
    end;
i(single_active_consumer_ctag, Q) when ?is_amqqueue(Q) ->
    QPid = amqqueue:get_pid(Q),
    {ok, {_, SacResult}, _} = ra:local_query(QPid,
                                             fun rabbit_fifo:query_single_active_consumer/1),
    case SacResult of
        {value, {ConsumerTag, _ChPid}} ->
            ConsumerTag;
        _ ->
            ''
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

online(Q) when ?is_amqqueue(Q) ->
    Nodes = amqqueue:get_quorum_nodes(Q),
    {Name, _} = amqqueue:get_pid(Q),
    [Node || Node <- Nodes, is_process_alive(Name, Node)].

format(Q) when ?is_amqqueue(Q) ->
    Nodes = amqqueue:get_quorum_nodes(Q),
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
    Keys = [<<"x-expires">>, <<"x-message-ttl">>,
            <<"x-max-priority">>, <<"x-queue-mode">>, <<"x-overflow">>],
    [case rabbit_misc:table_lookup(Args, Key) of
         undefined -> ok;
         _TypeVal   -> rabbit_misc:protocol_error(
                         precondition_failed,
                         "invalid arg '~s' for ~s",
                         [Key, rabbit_misc:rs(QueueName)])
     end || Key <- Keys],
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
        undefined -> application:get_env(rabbit, default_quorum_initial_group_size);
        {_Type, Val} -> Val
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
