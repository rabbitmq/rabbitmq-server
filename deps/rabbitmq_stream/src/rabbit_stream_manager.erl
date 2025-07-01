%% The contents of this file are subject to the Mozilla Public License
%% Version 2.0 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at https://www.mozilla.org/en-US/MPL/2.0/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is Pivotal Software, Inc.
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_stream_manager).

-feature(maybe_expr, enable).

-include_lib("rabbit_common/include/rabbit_framing.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("rabbit/include/amqqueue.hrl").
-include_lib("rabbitmq_stream/src/rabbit_stream_utils.hrl").

%% API
-export([create/4,
         delete/3,
         create_super_stream/6,
         delete_super_stream/3,
         lookup_leader/2,
         lookup_local_member/2,
         lookup_member/2,
         topology/2,
         route/3,
         partitions/2,
         partition_index/3,
         reset_offset/3]).

-spec create(binary(), binary(), #{binary() => binary()}, binary()) ->
    {ok, map()} |
    {error, reference_already_exists} |
    {error, internal_error} |
    {error, validation_failed}.
create(VirtualHost, Reference, Arguments, Username) ->
    StreamQueueArguments = stream_queue_arguments(Arguments),
    maybe
        ok ?= validate_stream_queue_arguments(StreamQueueArguments),
        do_create_stream(VirtualHost, Reference, StreamQueueArguments, Username)
    else
        error ->
            {error, validation_failed};
        {error, _} = Err ->
            Err
    end.

-spec delete(binary(), binary(), binary()) ->
    {ok, deleted} | {error, reference_not_found}.
delete(VirtualHost, Reference, Username) ->
    Name =
    #resource{virtual_host = VirtualHost,
              kind = queue,
              name = Reference},
    rabbit_log:debug("Trying to delete stream ~tp", [Reference]),
    case rabbit_amqqueue:lookup(Name) of
        {ok, Q} ->
            rabbit_log:debug("Found queue record ~tp, checking if it is a stream",
                             [Reference]),
            case is_stream_queue(Q) of
                true ->
                    rabbit_log:debug("Queue record ~tp is a stream, trying to delete it",
                                     [Reference]),
                    {ok, _} =
                    rabbit_stream_queue:delete(Q, false, false, Username),
                    rabbit_log:debug("Stream ~tp deleted", [Reference]),
                    {ok, deleted};
                _ ->
                    rabbit_log:debug("Queue record ~tp is NOT a stream, returning error",
                                     [Reference]),
                    {error, reference_not_found}
            end;
        {error, not_found} ->
            rabbit_log:debug("Stream ~tp not found, cannot delete it",
                             [Reference]),
            {error, reference_not_found}
    end.

-spec create_super_stream(binary(),
                          binary(),
                          [binary()],
                          #{binary() => binary()},
                          [binary()],
                          binary()) ->
    ok | {error, term()}.
create_super_stream(VirtualHost,
                    Name,
                    Partitions,
                    Arguments,
                    BindingKeys,
                    Username) ->
    case validate_super_stream_creation(VirtualHost, Name, Partitions, BindingKeys) of
        {error, Reason} ->
            {error, Reason};
        ok ->
            case declare_super_stream_exchange(VirtualHost, Name, Username) of
                ok ->
                    RollbackOperations =
                    [fun() ->
                             delete_super_stream_exchange(VirtualHost, Name,
                                                          Username)
                     end],
                    QueueCreationsResult =
                    lists:foldl(fun (Partition, {ok, RollbackOps}) ->
                                        Args =
                                        default_super_stream_arguments(Arguments),
                                        case create(VirtualHost,
                                                    Partition,
                                                    Args,
                                                    Username)
                                        of
                                            {ok, _} ->
                                                {ok,
                                                 [fun() ->
                                                          delete(VirtualHost,
                                                                 Partition,
                                                                 Username)
                                                  end]
                                                 ++ RollbackOps};
                                            {error, Reason} ->
                                                {{error, Reason},
                                                 RollbackOps}
                                        end;
                                    (_,
                                     {{error, _Reason}, _RollbackOps} =
                                     Acc) ->
                                        Acc
                                end,
                                {ok, RollbackOperations}, Partitions),
                    case QueueCreationsResult of
                        {ok, RollbackOps} ->
                            BindingsResult =
                            add_super_stream_bindings(VirtualHost,
                                                      Name,
                                                      Partitions,
                                                      BindingKeys,
                                                      Username),
                            case BindingsResult of
                                ok ->
                                    ok;
                                Error ->
                                    _ = [Fun() || Fun <- RollbackOps],
                                    Error
                            end;
                        {{error, Reason}, RollbackOps} ->
                            _ = [Fun() || Fun <- RollbackOps],
                            {error, Reason}
                    end;
                {error, Msg} ->
                    {error, Msg}
            end
    end.

-spec delete_super_stream(binary(), binary(), binary()) ->
    ok | {error, term()}.
delete_super_stream(VirtualHost, SuperStream, Username) ->
    case super_stream_partitions(VirtualHost, SuperStream) of
        {ok, Partitions} ->
            case delete_super_stream_exchange(VirtualHost, SuperStream,
                                              Username)
            of
                ok ->
                    ok;
                {error, Error} ->
                    rabbit_log:warning("Error while deleting super stream exchange ~tp, "
                                       "~tp",
                                       [SuperStream, Error]),
                    ok
            end,
            [begin
                 case delete(VirtualHost, Stream, Username) of
                     {ok, deleted} ->
                         ok;
                     {error, Err} ->
                         rabbit_log:warning("Error while delete partition ~tp of super stream "
                                            "~tp, ~tp",
                                            [Stream, SuperStream, Err]),
                         ok
                 end
             end
             || Stream <- Partitions],
            ok;
        {error, Error} ->
            {error, Error}
    end.

-spec lookup_leader(binary(), binary()) ->
    {ok, pid()} | {error, not_available} |
    {error, not_found}.
lookup_leader(VirtualHost, Stream) ->
    case lookup_stream(VirtualHost, Stream) of
        {ok, Q} ->
            LeaderPid = amqqueue:get_pid(Q),
            case process_alive(LeaderPid) of
                true ->
                    {ok, LeaderPid};
                false ->
                    case leader_from_members(Q) of
                        {ok, Pid} when is_pid(Pid) ->
                            {ok, Pid};
                        _ ->
                            {error, not_available}
                    end
            end;
        R ->
            R
    end.

-spec lookup_local_member(binary(), binary()) ->
    {ok, pid()} | {error, not_found} |
    {error, not_available}.
lookup_local_member(VirtualHost, Stream) ->
    case lookup_stream(VirtualHost, Stream) of
        {ok, Q} ->
            #{name := StreamName} = amqqueue:get_type_state(Q),
            % FIXME check if pid is alive in case of stale information
            case rabbit_stream_coordinator:local_pid(StreamName) of
                {ok, Pid} when is_pid(Pid) ->
                    {ok, Pid};
                {error, timeout} ->
                    {error, not_available};
                _ ->
                    {error, not_available}
            end;
        R ->
            R
    end.

-spec lookup_member(binary(), binary()) ->
    {ok, pid()} | {error, not_found} |
    {error, not_available}.
lookup_member(VirtualHost, Stream) ->
    case lookup_stream(VirtualHost, Stream) of
        {ok, Q} ->
            #{name := StreamName} = amqqueue:get_type_state(Q),
            % FIXME check if pid is alive in case of stale information
            case rabbit_stream_coordinator:local_pid(StreamName) of
                {ok, Pid} when is_pid(Pid) ->
                    {ok, Pid};
                _ ->
                    case rabbit_stream_coordinator:members(StreamName) of
                        {ok, Members} ->
                            case lists:search(fun ({undefined, _Role}) ->
                                                      false;
                                                  ({P, _Role})
                                                    when is_pid(P) ->
                                                      process_alive(P);
                                                  (_) ->
                                                      false
                                              end,
                                              maps:values(Members))
                            of
                                {value, {Pid, _Role}} ->
                                    {ok, Pid};
                                _ ->
                                    {error, not_available}
                            end;
                        _ ->
                            {error, not_available}
                    end
            end;
        R ->
            R
    end.

-spec topology(binary(), binary()) ->
    {ok,
     #{leader_node => undefined | pid(),
       replica_nodes => [pid()]}} |
    {error, stream_not_found} | {error, stream_not_available}.
topology(VirtualHost, Stream) ->
    case lookup_stream(VirtualHost, Stream) of
        {ok, Q} ->
            QState = amqqueue:get_type_state(Q),
            #{name := StreamName} = QState,
            case rabbit_stream_coordinator:members(StreamName) of
                {ok, Members} ->
                    {ok,
                     maps:fold(fun (_Node, {undefined, _Role}, Acc) ->
                                       Acc;
                                   (LeaderNode, {_Pid, writer}, Acc) ->
                                       Acc#{leader_node => LeaderNode};
                                   (ReplicaNode, {_Pid, replica}, Acc) ->
                                       #{replica_nodes := ReplicaNodes} =
                                       Acc,
                                       Acc#{replica_nodes =>
                                            ReplicaNodes
                                            ++ [ReplicaNode]};
                                   (_Node, _, Acc) ->
                                       Acc
                               end,
                               #{leader_node => undefined,
                                 replica_nodes => []},
                               Members)};
                Err ->
                    rabbit_log:info("Error locating ~tp stream members: ~tp",
                                    [StreamName, Err]),
                    {error, stream_not_available}
            end;
        {error, not_found} ->
            {error, stream_not_found};
        {error, not_available} ->
            {error, stream_not_available}
    end.

-spec route(binary(), binary(), binary()) ->
    {ok, [binary()] | no_route} | {error, stream_not_found}.
route(RoutingKey, VirtualHost, SuperStream) ->
    ExchangeName = rabbit_misc:r(VirtualHost, exchange, SuperStream),
    try
        Exchange = rabbit_exchange:lookup_or_die(ExchangeName),
        Content = #content{properties = #'P_basic'{}},
        {ok, DummyMsg} = mc_amqpl:message(ExchangeName,
                                          RoutingKey,
                                          Content),
        case rabbit_exchange:route(Exchange, DummyMsg) of
            [] ->
                {ok, no_route};
            Routes ->
                {ok,
                 [Stream
                  || #resource{name = Stream} = R <- Routes,
                     is_resource_stream_queue(R)]}
        end
    catch
        exit:Error ->
            rabbit_log:warning("Error while looking up exchange ~tp, ~tp",
                               [rabbit_misc:rs(ExchangeName), Error]),
            {error, stream_not_found}
    end.

-spec partitions(binary(), binary()) ->
    {ok, [binary()]} | {error, stream_not_found}.
partitions(VirtualHost, SuperStream) ->
    super_stream_partitions(VirtualHost, SuperStream).

-spec partition_index(binary(), binary(), binary()) ->
    {ok, integer()} | {error, stream_not_found}.
partition_index(VirtualHost, SuperStream, Stream) ->
    ExchangeName = rabbit_misc:r(VirtualHost, exchange, SuperStream),
    rabbit_log:debug("Looking for partition index of stream ~tp in "
                     "super stream ~tp (virtual host ~tp)",
                     [Stream, SuperStream, VirtualHost]),
    try
        _ = rabbit_exchange:lookup_or_die(ExchangeName),
        UnorderedBindings =
        _ = [Binding
             || Binding = #binding{destination = #resource{name = Q} = D}
                <- rabbit_binding:list_for_source(ExchangeName),
                is_resource_stream_queue(D), Q == Stream],
        OrderedBindings =
        rabbit_stream_utils:sort_partitions(UnorderedBindings),
        rabbit_log:debug("Bindings: ~tp", [OrderedBindings]),
        case OrderedBindings of
            [] ->
                {error, stream_not_found};
            Bindings ->
                Binding = lists:nth(1, Bindings),
                #binding{args = Args} = Binding,
                case rabbit_misc:table_lookup(Args,
                                              <<"x-stream-partition-order">>)
                of
                    {_, Order} ->
                        Index = rabbit_data_coercion:to_integer(Order),
                        {ok, Index};
                    _ ->
                        Pattern = <<"-">>,
                        Size = byte_size(Pattern),
                        case string:find(Stream, Pattern, trailing) of
                            nomatch ->
                                {ok, -1};
                            <<Pattern:Size/binary, Rest/binary>> ->
                                try
                                    Index = binary_to_integer(Rest),
                                    {ok, Index}
                                catch
                                    error:_ ->
                                        {ok, -1}
                                end;
                            _ ->
                                {ok, -1}
                        end
                end
        end
    catch
        exit:Error ->
            rabbit_log:error("Error while looking up exchange ~tp, ~tp",
                             [ExchangeName, Error]),
            {error, stream_not_found}
    end.

-spec reset_offset(binary(), binary(), binary()) ->
    ok |
    {error, not_available | not_found | no_reference |
     {validation_failed, term()}}.
reset_offset(_, _, Ref) when ?IS_INVALID_REF(Ref) ->
    {error, {validation_failed,
             rabbit_misc:format("Reference is too long to store offset: ~p",
                                 [byte_size(Ref)])}};
reset_offset(VH, S, Ref) ->
    case lookup_leader(VH, S) of
        {ok, P} ->
            case osiris:read_tracking(P, offset, Ref) of
                undefined ->
                    {error, no_reference};
                {offset, _} ->
                    osiris:write_tracking(P, Ref, {offset, 0})
            end;
        R ->
            R
    end.

stream_queue_arguments(Arguments) ->
    stream_queue_arguments([{<<"x-queue-type">>, longstr, <<"stream">>}],
                           Arguments).

stream_queue_arguments(ArgumentsAcc, Arguments)
  when map_size(Arguments) =:= 0 ->
    ArgumentsAcc;
stream_queue_arguments(ArgumentsAcc,
                       #{<<"max-length-bytes">> := Value} = Arguments) ->
    stream_queue_arguments([{<<"x-max-length-bytes">>, long,
                             rabbit_data_coercion:to_integer(Value)}]
                           ++ ArgumentsAcc,
                           maps:remove(<<"max-length-bytes">>, Arguments));
stream_queue_arguments(ArgumentsAcc,
                       #{<<"max-age">> := Value} = Arguments) ->
    stream_queue_arguments([{<<"x-max-age">>, longstr, Value}]
                           ++ ArgumentsAcc,
                           maps:remove(<<"max-age">>, Arguments));
stream_queue_arguments(ArgumentsAcc,
                       #{<<"stream-max-segment-size-bytes">> := Value} =
                       Arguments) ->
    stream_queue_arguments([{<<"x-stream-max-segment-size-bytes">>, long,
                             rabbit_data_coercion:to_integer(Value)}]
                           ++ ArgumentsAcc,
                           maps:remove(<<"stream-max-segment-size-bytes">>,
                                       Arguments));
stream_queue_arguments(ArgumentsAcc,
                       #{<<"initial-cluster-size">> := Value} = Arguments) ->
    stream_queue_arguments([{<<"x-initial-cluster-size">>, long,
                             rabbit_data_coercion:to_integer(Value)}]
                           ++ ArgumentsAcc,
                           maps:remove(<<"initial-cluster-size">>, Arguments));
stream_queue_arguments(ArgumentsAcc,
                       #{<<"queue-leader-locator">> := Value} = Arguments) ->
    stream_queue_arguments([{<<"x-queue-leader-locator">>, longstr,
                             Value}]
                           ++ ArgumentsAcc,
                           maps:remove(<<"queue-leader-locator">>, Arguments));
stream_queue_arguments(ArgumentsAcc,
                       #{<<"stream-filter-size-bytes">> := Value} = Arguments) ->
    stream_queue_arguments([{<<"x-stream-filter-size-bytes">>, long,
                             rabbit_data_coercion:to_integer(Value)}]
                           ++ ArgumentsAcc,
                           maps:remove(<<"stream-filter-size-bytes">>, Arguments));
stream_queue_arguments(ArgumentsAcc, _Arguments) ->
    ArgumentsAcc.

validate_stream_queue_arguments([]) ->
    ok;
validate_stream_queue_arguments([{<<"x-initial-cluster-size">>, long,
                                  ClusterSize}
                                 | _])
  when ClusterSize =< 0 ->
    error;
validate_stream_queue_arguments([{<<"x-queue-leader-locator">>,
                                  longstr, Locator}
                                 | T]) ->
    case lists:member(Locator,
                      rabbit_queue_location:queue_leader_locators())
    of
        true ->
            validate_stream_queue_arguments(T);
        false ->
            error
    end;
validate_stream_queue_arguments([{<<"x-stream-filter-size-bytes">>, long,
                                  FilterSize}
                                 | _])
  when FilterSize < 16 orelse FilterSize > 255 ->
    error;
validate_stream_queue_arguments([_ | T]) ->
    validate_stream_queue_arguments(T).

default_super_stream_arguments(Arguments) ->
    case Arguments of
        #{<<"queue-leader-locator">> := _} ->
            Arguments;
        _ ->
            Arguments#{<<"queue-leader-locator">> => <<"balanced">>}
    end.

do_create_stream(VirtualHost, Reference, StreamQueueArguments, Username) ->
    Name = #resource{virtual_host = VirtualHost,
                     kind = queue,
                     name = Reference},
    Q0 = amqqueue:new(Name,
                      none,
                      true,
                      false,
                      none,
                      StreamQueueArguments,
                      VirtualHost,
                      #{user => Username},
                      rabbit_stream_queue),
    try
        QueueLookup =
        rabbit_amqqueue:with(Name,
                             fun(Q) ->
                                     ok =
                                     rabbit_amqqueue:assert_equivalence(Q,
                                                                        true,
                                                                        false,
                                                                        StreamQueueArguments,
                                                                        none)
                             end),

        case QueueLookup of
            ok ->
                {error, reference_already_exists};
            {error, not_found} ->
                try
                    case rabbit_queue_type:declare(Q0, node()) of
                        {new, Q} ->
                            {ok, amqqueue:get_type_state(Q)};
                        {existing, _} ->
                            {error, reference_already_exists};
                        {error, Err} ->
                            rabbit_log:warning("Error while creating ~tp stream, ~tp",
                                               [Reference, Err]),
                            {error, internal_error};
                        {error,
                         queue_limit_exceeded, Reason, ReasonArg} ->
                            rabbit_log:warning("Cannot declare stream ~tp because, "
                                               ++ Reason,
                                               [Reference] ++ ReasonArg),
                            {error, validation_failed};
                        {protocol_error,
                         precondition_failed,
                         Msg,
                         Args} ->
                            rabbit_log:warning("Error while creating ~tp stream, "
                                               ++ Msg,
                                               [Reference] ++ Args),
                            {error, validation_failed}
                    end
                catch
                    exit:Error ->
                        rabbit_log:error("Error while creating ~tp stream, ~tp",
                                         [Reference, Error]),
                        {error, internal_error}
                end;
            {error, {absent, _, Reason}} ->
                rabbit_log:error("Error while creating ~tp stream, ~tp",
                                 [Reference, Reason]),
                {error, internal_error}
        end
    catch
        exit:ExitError ->
            case ExitError of
                % likely a problem of inequivalent args on an existing stream
                {amqp_error, precondition_failed, M, _} ->
                    rabbit_log:info("Error while creating ~tp stream, "
                                    ++ M,
                                    [Reference]),
                    {error, validation_failed};
                E ->
                    rabbit_log:warning("Error while creating ~tp stream, ~tp",
                                       [Reference, E]),
                    {error, validation_failed}
            end
    end.

super_stream_partitions(VirtualHost, SuperStream) ->
    ExchangeName = rabbit_misc:r(VirtualHost, exchange, SuperStream),
    try
        _ = rabbit_exchange:lookup_or_die(ExchangeName),
        UnorderedBindings =
        [Binding
         || Binding = #binding{destination = D}
            <- rabbit_binding:list_for_source(ExchangeName),
            is_resource_stream_queue(D)],
        OrderedBindings =
        rabbit_stream_utils:sort_partitions(UnorderedBindings),
        {ok,
         lists:foldl(fun (#binding{destination =
                                   #resource{kind = queue, name = Q}},
                          Acc) ->
                             Acc ++ [Q];
                         (_Binding, Acc) ->
                             Acc
                     end,
                     [], OrderedBindings)}
    catch
        exit:Error ->
            rabbit_log:error("Error while looking up exchange ~tp, ~tp",
                             [ExchangeName, Error]),
            {error, stream_not_found}
    end.

validate_super_stream_creation(_VirtualHost, _Name, Partitions, BindingKeys)
  when length(Partitions) =/= length(BindingKeys) ->
    {error, {validation_failed, "There must be the same number of partitions and binding keys"}};
validate_super_stream_creation(VirtualHost, Name, Partitions, _BindingKeys) ->
    maybe
        ok ?= validate_super_stream_partitions(Partitions),
        ok ?= case rabbit_vhost_limit:would_exceed_queue_limit(length(Partitions), VirtualHost) of
                  false ->
                      ok;
                  {true, Limit, _} ->
                      {error, {validation_failed,
                               rabbit_misc:format("Cannot declare super stream ~tp with ~tp partition(s) "
                                                  "because queue limit ~tp in vhost '~tp' is reached",
                                                  [Name, length(Partitions), Limit, VirtualHost])}}
              end,
        ok ?= case exchange_exists(VirtualHost, Name) of
                  {error, validation_failed} ->
                      {error,
                       {validation_failed,
                        rabbit_misc:format("~ts is not a correct name for a super stream",
                                           [Name])}};
                  {ok, true} ->
                      {error,
                       {reference_already_exists,
                        rabbit_misc:format("there is already an exchange named ~ts",
                                           [Name])}};
                  {ok, false} ->
                      ok
              end,
        ok ?= check_already_existing_queue(VirtualHost, Partitions)
    end.

validate_super_stream_partitions(Partitions) ->
    case erlang:length(Partitions) == sets:size(sets:from_list(Partitions)) of
        true ->
            case lists:dropwhile(fun(Partition) ->
                                         case rabbit_stream_utils:enforce_correct_name(Partition) of
                                             {ok, _} -> true;
                                             _ -> false
                                         end
                                 end, Partitions) of
                [] ->
                    ok;
                InvalidPartitions -> {error, {validation_failed,
                                              {rabbit_misc:format("~ts is not a correct partition names",
                                                                  [InvalidPartitions])}}}
            end;
        _ -> {error, {validation_failed,
                      {rabbit_misc:format("Duplicate partition names found ~ts",
                                          [Partitions])}}}
    end.

exchange_exists(VirtualHost, Name) ->
    case rabbit_stream_utils:enforce_correct_name(Name) of
        {ok, CorrectName} ->
            ExchangeName = rabbit_misc:r(VirtualHost, exchange, CorrectName),
            {ok, rabbit_exchange:exists(ExchangeName)};
        error ->
            {error, validation_failed}
    end.

queue_exists(VirtualHost, Name) ->
    case rabbit_stream_utils:enforce_correct_name(Name) of
        {ok, CorrectName} ->
            QueueName = rabbit_misc:r(VirtualHost, queue, CorrectName),
            {ok, rabbit_amqqueue:exists(QueueName)};
        error ->
            {error, validation_failed}
    end.

check_already_existing_queue(VirtualHost, Queues) ->
    check_already_existing_queue0(VirtualHost, Queues, undefined).

check_already_existing_queue0(_VirtualHost, [], undefined) ->
    ok;
check_already_existing_queue0(VirtualHost, [Q | T], _Error) ->
    case queue_exists(VirtualHost, Q) of
        {ok, false} ->
            check_already_existing_queue0(VirtualHost, T, undefined);
        {ok, true} ->
            {error,
             {reference_already_exists,
              rabbit_misc:format("there is already a queue named ~ts", [Q])}};
        {error, validation_failed} ->
            {error,
             {validation_failed,
              rabbit_misc:format("~ts is not a correct name for a queue", [Q])}}
    end.

declare_super_stream_exchange(VirtualHost, Name, Username) ->
    case rabbit_stream_utils:enforce_correct_name(Name) of
        {ok, CorrectName} ->
            Args =
            rabbit_misc:set_table_value([],
                                        <<"x-super-stream">>,
                                        bool,
                                        true),
            CheckedType = rabbit_exchange:check_type(<<"direct">>),
            ExchangeName = rabbit_misc:r(VirtualHost, exchange, CorrectName),
            XResult = case rabbit_exchange:lookup(ExchangeName) of
                          {ok, FoundX} ->
                              {ok, FoundX};
                          {error, not_found} ->
                              rabbit_exchange:declare(ExchangeName,
                                                      CheckedType,
                                                      true,
                                                      false,
                                                      false,
                                                      Args,
                                                      Username)
                      end,
            case XResult of
                {ok, X} ->
                    try
                        ok =
                        rabbit_exchange:assert_equivalence(X,
                                                           CheckedType,
                                                           true,
                                                           false,
                                                           false,
                                                           Args)
                    catch
                        exit:ExitError ->
                            % likely to be a problem of inequivalent args on an existing stream
                            rabbit_log:error("Error while creating ~tp super stream exchange: "
                                             "~tp",
                                             [Name, ExitError]),
                            {error, validation_failed}
                    end;
                {error, timeout} = Err ->
                    Err
            end;
        error ->
            {error, validation_failed}
    end.

add_super_stream_bindings(VirtualHost,
                          Name,
                          Partitions,
                          BindingKeys,
                          Username) ->
    PartitionsBindingKeys = lists:zip(Partitions, BindingKeys),
    BindingsResult =
    lists:foldl(fun ({Partition, BindingKey}, {ok, Order}) ->
                        case add_super_stream_binding(VirtualHost,
                                                      Name,
                                                      Partition,
                                                      BindingKey,
                                                      Order,
                                                      Username)
                        of
                            ok ->
                                {ok, Order + 1};
                            {error, Reason} ->
                                {{error, Reason}, 0}
                        end;
                    (_, {{error, _Reason}, _Order} = Acc) ->
                        Acc
                end,
                {ok, 0}, PartitionsBindingKeys),
    case BindingsResult of
        {ok, _} ->
            ok;
        {{error, Reason}, _} ->
            {error, Reason}
    end.

add_super_stream_binding(VirtualHost,
                         SuperStream,
                         Partition,
                         BindingKey,
                         Order,
                         Username) ->
    {ok, ExchangeNameBin} =
    rabbit_stream_utils:enforce_correct_name(SuperStream),
    {ok, QueueNameBin} =
    rabbit_stream_utils:enforce_correct_name(Partition),
    ExchangeName = rabbit_misc:r(VirtualHost, exchange, ExchangeNameBin),
    QueueName = rabbit_misc:r(VirtualHost, queue, QueueNameBin),
    Pid = self(),
    Arguments =
    rabbit_misc:set_table_value([],
                                <<"x-stream-partition-order">>,
                                long,
                                Order),
    case rabbit_binding:add(#binding{source = ExchangeName,
                                     destination = QueueName,
                                     key = BindingKey,
                                     args = Arguments},
                            fun (_X, Q) when ?is_amqqueue(Q) ->
                                    try
                                        rabbit_amqqueue:check_exclusive_access(Q,
                                                                               Pid)
                                    catch
                                        exit:Reason ->
                                            {error, Reason}
                                    end;
                                (_X, #exchange{}) ->
                                    ok
                            end,
                            Username)
    of
        {error, {resources_missing, [{not_found, Name} | _]}} ->
            {error,
             {stream_not_found,
              rabbit_misc:format("stream ~ts does not exists", [Name])}};
        {error, {resources_missing, [{absent, Q, _Reason} | _]}} ->
            {error,
             {stream_not_found,
              rabbit_misc:format("stream ~ts does not exists (absent)", [Q])}};
        {error, {binding_invalid, Fmt, Args}} ->
            {error, {binding_invalid, rabbit_misc:format(Fmt, Args)}};
        {error, #amqp_error{} = Error} ->
            {error, {internal_error, rabbit_misc:format("~tp", [Error])}};
        {error, timeout} ->
            {error, {internal_error, "failed to add binding due to a timeout"}};
        ok ->
            ok
    end.

delete_super_stream_exchange(VirtualHost, Name, Username) ->
    case rabbit_stream_utils:enforce_correct_name(Name) of
        {ok, CorrectName} ->
            ExchangeName = rabbit_misc:r(VirtualHost, exchange, CorrectName),
            case rabbit_exchange:ensure_deleted(
                   ExchangeName, false, Username) of
                ok ->
                    ok;
                {error, timeout} = Err ->
                    Err
            end;
        error ->
            {error, validation_failed}
    end.

lookup_stream(VirtualHost, Stream) ->
    Name = #resource{virtual_host = VirtualHost,
                     kind = queue,
                     name = Stream},
    case rabbit_amqqueue:lookup(Name) of
        {ok, Q} ->
            case is_stream_queue(Q) of
                true ->
                    {ok, Q};
                _ ->
                    {error, not_found}
            end;
        {error, not_found} ->
            case rabbit_amqqueue:not_found_or_absent_dirty(Name) of
                not_found ->
                    {error, not_found};
                _ ->
                    {error, not_available}
            end
    end.

leader_from_members(Q) ->
    QState = amqqueue:get_type_state(Q),
    #{name := StreamName} = QState,
    case rabbit_stream_coordinator:members(StreamName) of
        {ok, Members} ->
            maps:fold(fun (_LeaderNode, {Pid, writer}, _Acc) ->
                              {ok, Pid};
                          (_Node, _, Acc) ->
                              Acc
                      end,
                      {error, not_found}, Members);
        _ ->
            {error, not_found}
    end.

process_alive(Pid) when is_pid(Pid) ->
    CurrentNode = node(),
    case node(Pid) of
        nonode@nohost ->
            false;
        CurrentNode ->
            is_process_alive(Pid);
        OtherNode ->
            case rpc:call(OtherNode, erlang, is_process_alive, [Pid], 10000) of
                B when is_boolean(B) ->
                    B;
                _ ->
                    false
            end
    end;
process_alive(_) ->
    false.

is_stream_queue(Q) ->
    case amqqueue:get_type(Q) of
        rabbit_stream_queue ->
            true;
        _ ->
            false
    end.

is_resource_stream_queue(#resource{kind = queue} = Resource) ->
    case rabbit_amqqueue:lookup(Resource) of
        {ok, Q} ->
            is_stream_queue(Q);
        _ ->
            false
    end;
is_resource_stream_queue(_) ->
    false.
