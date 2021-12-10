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
%% Copyright (c) 2020-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_stream_manager).

-behaviour(gen_server).

-include_lib("rabbit_common/include/rabbit.hrl").

%% API
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2]).
-export([start_link/1,
         create/4,
         delete/3,
         lookup_leader/2,
         lookup_local_member/2,
         topology/2,
         route/3,
         partitions/2]).

-record(state, {configuration}).

start_link(Conf) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [Conf], []).

init([Conf]) ->
    {ok, #state{configuration = Conf}}.

-spec create(binary(), binary(), #{binary() => binary()}, binary()) ->
                {ok, map()} |
                {error, reference_already_exists} |
                {error, internal_error} |
                {error, validation_failed}.
create(VirtualHost, Reference, Arguments, Username) ->
    gen_server:call(?MODULE,
                    {create, VirtualHost, Reference, Arguments, Username}).

-spec delete(binary(), binary(), binary()) ->
                {ok, deleted} | {error, reference_not_found}.
delete(VirtualHost, Reference, Username) ->
    gen_server:call(?MODULE, {delete, VirtualHost, Reference, Username}).

-spec lookup_leader(binary(), binary()) ->
    {ok, pid()} | {error, not_available} |
    {error, not_found}.

lookup_leader(VirtualHost, Stream) ->
    gen_server:call(?MODULE, {lookup_leader, VirtualHost, Stream}).

-spec lookup_local_member(binary(), binary()) ->
                             {ok, pid()} | {error, not_found} |
                             {error, not_available}.
lookup_local_member(VirtualHost, Stream) ->
    gen_server:call(?MODULE, {lookup_local_member, VirtualHost, Stream}).

-spec topology(binary(), binary()) ->
                  {ok,
                   #{leader_node => undefined | pid(),
                     replica_nodes => [pid()]}} |
                  {error, stream_not_found} | {error, stream_not_available}.
topology(VirtualHost, Stream) ->
    gen_server:call(?MODULE, {topology, VirtualHost, Stream}).

-spec route(binary(), binary(), binary()) ->
               {ok, [binary()] | no_route} | {error, stream_not_found}.
route(RoutingKey, VirtualHost, SuperStream) ->
    gen_server:call(?MODULE,
                    {route, RoutingKey, VirtualHost, SuperStream}).

-spec partitions(binary(), binary()) ->
                    {ok, [binary()]} | {error, stream_not_found}.
partitions(VirtualHost, SuperStream) ->
    gen_server:call(?MODULE, {partitions, VirtualHost, SuperStream}).

stream_queue_arguments(Arguments) ->
    stream_queue_arguments([{<<"x-queue-type">>, longstr, <<"stream">>}],
                           Arguments).

stream_queue_arguments(ArgumentsAcc, Arguments)
    when map_size(Arguments) =:= 0 ->
    ArgumentsAcc;
stream_queue_arguments(ArgumentsAcc,
                       #{<<"max-length-bytes">> := Value} = Arguments) ->
    stream_queue_arguments([{<<"x-max-length-bytes">>, long,
                             binary_to_integer(Value)}]
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
                             binary_to_integer(Value)}]
                           ++ ArgumentsAcc,
                           maps:remove(<<"stream-max-segment-size-bytes">>,
                                       Arguments));
stream_queue_arguments(ArgumentsAcc,
                       #{<<"initial-cluster-size">> := Value} = Arguments) ->
    stream_queue_arguments([{<<"x-initial-cluster-size">>, long,
                             binary_to_integer(Value)}]
                           ++ ArgumentsAcc,
                           maps:remove(<<"initial-cluster-size">>, Arguments));
stream_queue_arguments(ArgumentsAcc,
                       #{<<"queue-leader-locator">> := Value} = Arguments) ->
    stream_queue_arguments([{<<"x-queue-leader-locator">>, longstr,
                             Value}]
                           ++ ArgumentsAcc,
                           maps:remove(<<"queue-leader-locator">>, Arguments));
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
                      [<<"client-local">>, <<"random">>, <<"least-leaders">>])
    of
        true ->
            validate_stream_queue_arguments(T);
        false ->
            error
    end;
validate_stream_queue_arguments([_ | T]) ->
    validate_stream_queue_arguments(T).

handle_call({create, VirtualHost, Reference, Arguments, Username},
            _From, State) ->
    Name =
        #resource{virtual_host = VirtualHost,
                  kind = queue,
                  name = Reference},
    StreamQueueArguments = stream_queue_arguments(Arguments),
    case validate_stream_queue_arguments(StreamQueueArguments) of
        ok ->
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
                        {reply, {error, reference_already_exists}, State};
                    {error, not_found} ->
                        try
                            case rabbit_queue_type:declare(Q0, node()) of
                                {new, Q} ->
                                    {reply, {ok, amqqueue:get_type_state(Q)},
                                     State};
                                {existing, _} ->
                                    {reply, {error, reference_already_exists},
                                     State};
                                {error, Err} ->
                                    rabbit_log:warning("Error while creating ~p stream, ~p",
                                                       [Reference, Err]),
                                    {reply, {error, internal_error}, State}
                            end
                        catch
                            exit:Error ->
                                rabbit_log:error("Error while creating ~p stream, ~p",
                                                 [Reference, Error]),
                                {reply, {error, internal_error}, State}
                        end;
                    {error, {absent, _, Reason}} ->
                        rabbit_log:error("Error while creating ~p stream, ~p",
                                         [Reference, Reason]),
                        {reply, {error, internal_error}, State}
                end
            catch
                exit:ExitError ->
                    % likely to be a problem of inequivalent args on an existing stream
                    rabbit_log:error("Error while creating ~p stream: ~p",
                                     [Reference, ExitError]),
                    {reply, {error, validation_failed}, State}
            end;
        error ->
            {reply, {error, validation_failed}, State}
    end;
handle_call({delete, VirtualHost, Reference, Username}, _From,
            State) ->
    Name =
        #resource{virtual_host = VirtualHost,
                  kind = queue,
                  name = Reference},
    rabbit_log:debug("Trying to delete stream ~p", [Reference]),
    case rabbit_amqqueue:lookup(Name) of
        {ok, Q} ->
            rabbit_log:debug("Found queue record ~p, checking if it is a stream",
                             [Reference]),
            case is_stream_queue(Q) of
                true ->
                    rabbit_log:debug("Queue record ~p is a stream, trying to delete it",
                                     [Reference]),
                    {ok, _} =
                        rabbit_stream_queue:delete(Q, false, false, Username),
                    rabbit_log:debug("Stream ~p deleted", [Reference]),
                    {reply, {ok, deleted}, State};
                _ ->
                    rabbit_log:debug("Queue record ~p is NOT a stream, returning error",
                                     [Reference]),
                    {reply, {error, reference_not_found}, State}
            end;
        {error, not_found} ->
            rabbit_log:debug("Stream ~p not found, cannot delete it",
                             [Reference]),
            {reply, {error, reference_not_found}, State}
    end;
handle_call({lookup_leader, VirtualHost, Stream}, _From, State) ->
    Name =
        #resource{virtual_host = VirtualHost,
                  kind = queue,
                  name = Stream},
    Res = case rabbit_amqqueue:lookup(Name) of
              {ok, Q} ->
                  case is_stream_queue(Q) of
                      true ->
                          LeaderPid = amqqueue:get_pid(Q),
                          case process_alive(LeaderPid) of
                              true ->
                                  {ok, LeaderPid};
                              false ->
                                  case leader_from_members(Q) of
                                      {ok, Pid} ->
                                          {ok, Pid};
                                      _ ->
                                          {error, not_available}
                                  end
                          end;
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
          end,
    {reply, Res, State};
handle_call({lookup_local_member, VirtualHost, Stream}, _From,
            State) ->
    Name =
        #resource{virtual_host = VirtualHost,
                  kind = queue,
                  name = Stream},
    Res = case rabbit_amqqueue:lookup(Name) of
              {ok, Q} ->
                  case is_stream_queue(Q) of
                      true ->
                          #{name := StreamName} = amqqueue:get_type_state(Q),
                          % FIXME check if pid is alive in case of stale information
                          case rabbit_stream_coordinator:local_pid(StreamName)
                          of
                              {ok, Pid} when is_pid(Pid) ->
                                  {ok, Pid};
                              {error, timeout} ->
                                  {error, not_available};
                              _ ->
                                  {error, not_available}
                          end;
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
          end,
    {reply, Res, State};
handle_call({topology, VirtualHost, Stream}, _From, State) ->
    Name =
        #resource{virtual_host = VirtualHost,
                  kind = queue,
                  name = Stream},
    Res = case rabbit_amqqueue:lookup(Name) of
              {ok, Q} ->
                  case is_stream_queue(Q) of
                      true ->
                          QState = amqqueue:get_type_state(Q),
                          #{name := StreamName} = QState,
                          StreamMembers =
                              case rabbit_stream_coordinator:members(StreamName)
                              of
                                  {ok, Members} ->
                                      maps:fold(fun (_Node, {undefined, _Role},
                                                     Acc) ->
                                                        Acc;
                                                    (LeaderNode, {_Pid, writer},
                                                     Acc) ->
                                                        Acc#{leader_node =>
                                                                 LeaderNode};
                                                    (ReplicaNode,
                                                     {_Pid, replica}, Acc) ->
                                                        #{replica_nodes :=
                                                              ReplicaNodes} =
                                                            Acc,
                                                        Acc#{replica_nodes =>
                                                                 ReplicaNodes
                                                                 ++ [ReplicaNode]};
                                                    (_Node, _, Acc) ->
                                                        Acc
                                                end,
                                                #{leader_node => undefined,
                                                  replica_nodes => []},
                                                Members);
                                  _ ->
                                      {error, stream_not_found}
                              end,
                          {ok, StreamMembers};
                      _ ->
                          {error, stream_not_found}
                  end;
              {error, not_found} ->
                  case rabbit_amqqueue:not_found_or_absent_dirty(Name) of
                      not_found ->
                          {error, stream_not_found};
                      _ ->
                          {error, stream_not_available}
                  end
          end,
    {reply, Res, State};
handle_call({route, RoutingKey, VirtualHost, SuperStream}, _From,
            State) ->
    ExchangeName = rabbit_misc:r(VirtualHost, exchange, SuperStream),
    Res = try
              Exchange = rabbit_exchange:lookup_or_die(ExchangeName),
              Delivery =
                  #delivery{message =
                                #basic_message{routing_keys = [RoutingKey]}},
              case rabbit_exchange:route(Exchange, Delivery) of
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
                  rabbit_log:error("Error while looking up exchange ~p, ~p",
                                   [ExchangeName, Error]),
                  {error, stream_not_found}
          end,
    {reply, Res, State};
handle_call({partitions, VirtualHost, SuperStream}, _From, State) ->
    ExchangeName = rabbit_misc:r(VirtualHost, exchange, SuperStream),
    Res = try
              rabbit_exchange:lookup_or_die(ExchangeName),
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
                  rabbit_log:error("Error while looking up exchange ~p, ~p",
                                   [ExchangeName, Error]),
                  {error, stream_not_found}
          end,
    {reply, Res, State};
handle_call(which_children, _From, State) ->
    {reply, [], State}.

handle_cast(_, State) ->
    {noreply, State}.

handle_info(Info, State) ->
    rabbit_log:info("Received info ~p", [Info]),
    {noreply, State}.

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

process_alive(Pid) ->
    CurrentNode = node(),
    case node(Pid) of
        nonode@nohost ->
            false;
        CurrentNode ->
            is_process_alive(Pid);
        OtherNode ->
            rpc:call(OtherNode, erlang, is_process_alive, [Pid], 10000)
    end.

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
