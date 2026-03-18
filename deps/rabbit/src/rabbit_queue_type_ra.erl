%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

-module(rabbit_queue_type_ra).

-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("kernel/include/logger.hrl").

-export([status/2,
         add_member/5,
         add_members/5,
         delete_member/3,
         delete_members/4,
         force_shrink_members_to_local_member/2,
         all_members_stable/2,
         drain/2,
         revive/1,
         transfer_leadership/2]).

-export_type([ra_membership/0]).

-type ra_membership() :: voter | non_voter | promotable.

-define(RA_MEMBERS_TIMEOUT, 60_000).

-callback status(amqqueue:amqqueue()) ->
    [[{binary(), term()}]].

-callback add_member(amqqueue:amqqueue(), node(), ra_membership(), timeout()) ->
    ok | {error, term()}.

-callback delete_member(amqqueue:amqqueue(), node()) ->
    ok | {error, term()}.

-callback ra_system() ->
    atom().

-spec status(rabbit_types:vhost(), rabbit_misc:resource_name()) ->
    [[{binary(), term()}]] | {error, term()}.
status(VHost, Name) ->
    with_ra_queue(VHost, Name, fun(Mod, Q) -> Mod:status(Q) end).

-spec add_member(rabbit_types:vhost(), rabbit_misc:resource_name(),
                 node(), ra_membership(), timeout()) ->
    ok | {error, term()}.
add_member(VHost, Name, Node, Membership, Timeout) ->
    Fun = fun(Mod, Q) ->
                  case is_queue_member(Q, Node) of
                      true ->
                          %% idempotent by design
                          ok;
                      false ->
                          Mod:add_member(Q, Node, Membership, Timeout)
                  end
          end,
    with_ra_queue(VHost, Name, Fun).

-spec delete_member(rabbit_types:vhost(), rabbit_misc:resource_name(), node()) ->
    ok | {error, term()}.
delete_member(VHost, Name, Node) ->
    Fun = fun(Mod, Q) ->
                  case is_queue_member(Q, Node) of
                      true ->
                          Mod:delete_member(Q, Node);
                      false ->
                          %% idempotent by design
                          ok
                  end
          end,
    with_ra_queue(VHost, Name, Fun).

is_queue_member(Q, Member) ->
    lists:member(Member, get_nodes(Q)).

-spec force_shrink_members_to_local_member(vhost:name(),
                                           rabbit_misc:resource_name()) ->
    ok | {error, term()}.
force_shrink_members_to_local_member(VHost, Name) ->
    Fun = fun(_Mod, Q) ->
                  case lists:member(node(), get_nodes(Q)) of
                      true ->
                          force_shrink_members_to_local_member(Q);
                      false ->
                          {error, no_local_member}
                  end
          end,
    with_ra_queue(VHost, Name, Fun).

force_shrink_members_to_local_member(Q) ->
    Node = node(),
    Nodes = get_nodes(Q),
    OtherNodes = lists:delete(Node, Nodes),
    Mod = amqqueue:get_type(Q),
    RaSystem = Mod:ra_system(),
    QName = amqqueue:get_name(Q),
    QNameFmt = rabbit_misc:rs(QName),
    {RaName, _} = amqqueue:get_pid(Q),
    ?LOG_WARNING("force shrinking ~ts to a single member on node ~ts ...",
                 [QNameFmt, Node]),
    ok = ra_server_proc:force_shrink_members_to_current_member({RaName, Node}),
    Fun = fun(Q0) ->
                  amqqueue:update_type_state(Q0, fun(TS) ->
                                                         TS#{nodes => [Node]}
                                                 end)
          end,
    _ = rabbit_amqqueue:update(QName, Fun),
    _ = [ra:force_delete_server(RaSystem, {RaName, N}) || N <- OtherNodes],
    ?LOG_WARNING("successfully force shrank ~ts", [QNameFmt]).

with_ra_queue(VHost, Name, Fun) ->
    QName = rabbit_misc:queue_resource(VHost, Name),
    case rabbit_amqqueue:lookup(QName) of
        {ok, Q} ->
            Mod = amqqueue:get_type(Q),
            case is_ra_based(Mod) of
                true ->
                    Fun(Mod, Q);
                false ->
                    {error, {unsupported, Mod}}
            end;
        {error, not_found} = Err ->
            Err
    end.

%% For each Ra-based queue matching VHostSpec and QueueSpec, add a member on Node.
-spec add_members(binary(), binary(), node(), all | even, ra_membership()) ->
    [{rabbit_amqqueue:name(), {ok, pos_integer()} | {error, pos_integer(), term()}}].
add_members(VHostSpec, QueueSpec, Node, Strategy, Membership) ->
    FilterFun = fun(Q) -> not lists:member(Node, get_nodes(Q)) end,
    Fun = fun(Mod, Q, Size) ->
                  case Mod:add_member(Q, Node, Membership, ?RA_MEMBERS_TIMEOUT) of
                      ok ->
                          {ok, Size + 1};
                      {error, Reason} ->
                          {error, Size, Reason}
                  end
          end,
    modify_members_matching(VHostSpec, QueueSpec, Node, Strategy, FilterFun, Fun).

%% For each Ra-based queue matching VHostSpec and QueueSpec, delete a member on Node.
-spec delete_members(binary(), binary(), node(), all | even) ->
    [{rabbit_amqqueue:name(), {ok, pos_integer()} | {error, pos_integer(), term()}}].
delete_members(VHostSpec, QueueSpec, Node, Strategy) ->
    FilterFun = fun(Q) -> lists:member(Node, get_nodes(Q)) end,
    Fun = fun(Mod, Q, Size) ->
                  case Mod:delete_member(Q, Node) of
                      ok ->
                          {ok, Size - 1};
                      {error, Reason} ->
                          {error, Size, Reason}
                  end
          end,
    modify_members_matching(VHostSpec, QueueSpec, Node, Strategy, FilterFun, Fun).

modify_members_matching(VHostSpec, QueueSpec, Node, Strategy, FilterFun, OperationFun) ->
    case lists:member(Node, rabbit_nodes:list_running()) of
        true ->
            [begin
                 Mod = amqqueue:get_type(Q),
                 QName = amqqueue:get_name(Q),
                 QNodes = get_nodes(Q),
                 Size = length(QNodes),
                 {ok, RaName} = rabbit_queue_type_util:qname_to_internal_name(QName),
                 Res = case all_members_stable(RaName, QNodes) of
                           true ->
                               OperationFun(Mod, Q, Size);
                           false ->
                               {error, Size, {error, non_stable_members}}
                       end,
                 {QName, Res}
             end
             || Q <- rabbit_amqqueue:list(),
                FilterFun(Q),
                matches_strategy(Strategy, get_nodes(Q)),
                is_ra_based(amqqueue:get_type(Q)),
                is_match(amqqueue:get_vhost(Q), VHostSpec),
                is_match(get_resource_name(amqqueue:get_name(Q)), QueueSpec)];
        false ->
            {error, {node_not_running, Node}}
    end.

is_ra_based(Mod) ->
    lists:any(fun({behaviour, Bs}) -> lists:member(?MODULE, Bs);
                 ({behavior, Bs}) -> lists:member(?MODULE, Bs);
                 (_) -> false
              end, Mod:module_info(attributes)).

%% Check that all Ra members are stable (voter or non_voter, not promotable).
%% This is used to ensure that we don't add/remove members while another
%% membership change is in progress.
-spec all_members_stable(atom(), [node()]) -> boolean().
all_members_stable(RaName, QNodes) ->
    Result = erpc:multicall(QNodes, ets, lookup, [ra_state, RaName], ?RA_MEMBERS_TIMEOUT),
    lists:all(fun({ok, [{_RaName, _RaState, Membership}]})
                    when Membership =:= voter orelse
                         Membership =:= non_voter ->
                      true;
                 (_) ->
                      false
              end, Result).

matches_strategy(all, _Members) ->
    true;
matches_strategy(even, Members) ->
    length(Members) rem 2 =:= 0.

is_match(Subject, RE) ->
    match =:= re:run(Subject, RE, [{capture, none}]).

get_resource_name(#resource{name = Name}) ->
    Name.

-spec drain(rabbit_queue_type:queue_type(), [node()]) -> ok.
drain(QueueType, []) ->
    ?LOG_INFO("skipping drain for ~s since no other RabbitMQ node is online",
              [QueueType]);
drain(QueueType, _OnlineRemoteNodes) ->
    stop_local_leaders(QueueType),
    stop_local_followers(QueueType),
    ok.

-spec revive(rabbit_queue_type:queue_type()) -> ok.
revive(QueueType) ->
    Queues = list_local_followers(QueueType),
    %% This function ignores the first argument so we can just pass the
    %% empty binary as the vhost name.
    {Recovered, Failed} = QueueType:recover(<<>>, Queues),
    ?LOG_INFO("successfully revived ~b ~s replicas",
              [length(Recovered), QueueType]),
    case length(Failed) of
        0 ->
            ok;
        NumFailed ->
            ?LOG_ERROR("failed to revive ~b ~s replicas",
                       [NumFailed, QueueType]),
            ok
    end.

stop_local_leaders(QueueType) ->
    LocalLeaderQueues = list_local_leaders(QueueType),
    ?LOG_INFO("stopping ~b local ~s leaders...",
              [length(LocalLeaderQueues), QueueType]),
    QueuesChunked = ra_lib:lists_chunk(256, LocalLeaderQueues),
    [begin
         [begin
              %% We trigger an election and exclude this node from the list of
              %% candidates by simply shutting its local replica (Ra server).k
              RaLeader = amqqueue:get_pid(Q),
              RaSystem = QueueType:ra_system(),
              ?LOG_DEBUG("stopping Ra server ~tp in Ra system ~s ...",
                         [RaLeader, RaSystem]),
              case ra:stop_server(RaSystem, RaLeader) of
                  ok ->
                      ?LOG_DEBUG("successfully stopped Ra server ~tp", [RaLeader]);
                  {error, nodedown} ->
                      ?LOG_WARNING("failed to stop Ra server ~tp: target node was reported as down",
                                   [RaLeader])
              end,
              ok
          end || Q <- Queues],
         %% Wait for leader elections before processing next chunk of queues.
         [begin
              {RaName, LeaderNode} = amqqueue:get_pid(Q),
              MemberNodes = lists:delete(LeaderNode, get_nodes(Q)),
              %% We don't do any explicit error handling here as it is more
              %% important to make progress.
              _ = lists:any(fun(N) ->
                                    case ra:members({RaName, N}, ?RA_MEMBERS_TIMEOUT) of
                                        {ok, _, _} ->
                                            true;
                                        Err ->
                                            Name = amqqueue:get_name(Q),
                                            ?LOG_DEBUG("failed to wait for leader election "
                                                       "for ~ts on node ~tp: ~tp",
                                                       [rabbit_misc:rs(Name), N, Err]),
                                            false
                                    end
                            end, MemberNodes),
              ok
          end || Q <- Queues],
         ok
     end || Queues <- QueuesChunked],
    ?LOG_INFO("stopped local ~s leaders", [QueueType]),
    ok.

%% Shut down Ra nodes so that they are not considered for leader election.
stop_local_followers(QueueType) ->
    Queues = list_local_followers(QueueType),
    ?LOG_INFO("stopping ~b local ~s followers...",
              [length(Queues), QueueType]),
    _ = [begin
             {RegisteredName, _LeaderNode} = amqqueue:get_pid(Q),
             RaNode = {RegisteredName, node()},
             RaSystem = QueueType:ra_system(),
             ?LOG_DEBUG("stopping Ra server ~tp in Ra system ~s ...",
                        [RaNode, RaSystem]),
             case ra:stop_server(RaSystem, RaNode) of
                 ok ->
                     ?LOG_DEBUG("successfully stopped Ra server ~tp", [RaNode]);
                 {error, nodedown} ->
                     ?LOG_WARNING("failed to stop Ra server ~tp: target node was reported as down",
                                  [RaNode])
             end
         end || Q <- Queues],
    ?LOG_INFO("stopped local ~s followers", [QueueType]),
    ok.

list_local_leaders(QueueType) ->
    [Q || Q <- rabbit_amqqueue:list(),
          amqqueue:get_type(Q) =:= QueueType,
          amqqueue:get_state(Q) =/= crashed,
          amqqueue:get_leader_node(Q) =:= node()].

list_local_followers(QueueType) ->
    Node = node(),
    [Q || Q <- rabbit_amqqueue:list(),
          amqqueue:get_type(Q) =:= QueueType,
          QueueType:is_recoverable(Q),
          amqqueue:get_leader_node(Q) =/= Node,
          lists:member(Node, get_nodes(Q))].

-spec transfer_leadership(amqqueue:amqqueue(), node()) ->
    {ok, node()} | {error, term()}.
transfer_leadership(Queue, Destination) ->
    {RaName, _} = Leader = amqqueue:get_pid(Queue),
    case ra:transfer_leadership(Leader, {RaName, Destination}) of
        ok ->
            case ra:members(Leader, ?RA_MEMBERS_TIMEOUT) of
                {_, _, {_, NewNode}} ->
                    {ok, NewNode};
                {timeout, _} ->
                    {error, ra_members_timeout}
            end;
        already_leader ->
            {error, already_leader};
        {timeout, _} ->
            {error, timeout};
        {error, _} = Err ->
            Err
    end.

get_nodes(Queue) ->
    rabbit_amqqueue:get_quorum_nodes(Queue).
