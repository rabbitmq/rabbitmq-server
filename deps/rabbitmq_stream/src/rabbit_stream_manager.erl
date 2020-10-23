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
%% Copyright (c) 2020 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_stream_manager).
-behaviour(gen_server).

-include_lib("rabbit_common/include/rabbit.hrl").

%% API
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).
-export([start_link/1, create/4, delete/3, lookup_leader/2, lookup_local_member/2, topology/2]).

-record(state, {
    configuration
}).

start_link(Conf) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [Conf], []).

init([Conf]) ->
    {ok, #state{configuration = Conf}}.

-spec create(binary(), binary(), #{binary() => binary()}, binary()) ->
    {ok, map()} | {error, reference_already_exists} | {error, internal_error}.
create(VirtualHost, Reference, Arguments, Username) ->
    gen_server:call(?MODULE, {create, VirtualHost, Reference, Arguments, Username}).

-spec delete(binary(), binary(), binary()) ->
    {ok, deleted} | {error, reference_not_found}.
delete(VirtualHost, Reference, Username) ->
    gen_server:call(?MODULE, {delete, VirtualHost, Reference, Username}).

-spec lookup_leader(binary(), binary()) -> pid() | cluster_not_found.
lookup_leader(VirtualHost, Stream) ->
    gen_server:call(?MODULE, {lookup_leader, VirtualHost, Stream}).

-spec lookup_local_member(binary(), binary()) -> {ok, pid()} | {error, not_found}.
lookup_local_member(VirtualHost, Stream) ->
    gen_server:call(?MODULE, {lookup_local_member, VirtualHost, Stream}).

-spec topology(binary(), binary()) ->
    {ok, #{leader_node => pid(), replica_nodes => [pid()]}} | {error, stream_not_found}.
topology(VirtualHost, Stream) ->
    gen_server:call(?MODULE, {topology, VirtualHost, Stream}).

stream_queue_arguments(Arguments) ->
    stream_queue_arguments([{<<"x-queue-type">>, longstr, <<"stream">>}], Arguments).

stream_queue_arguments(ArgumentsAcc, Arguments) when map_size(Arguments) =:= 0 ->
    ArgumentsAcc;
stream_queue_arguments(ArgumentsAcc, #{<<"max-length-bytes">> := Value} = Arguments) ->
    stream_queue_arguments(
        [{<<"x-max-length-bytes">>, long, binary_to_integer(Value)}] ++ ArgumentsAcc,
        maps:remove(<<"max-length-bytes">>, Arguments)
    );
stream_queue_arguments(ArgumentsAcc, #{<<"max-age">> := Value} = Arguments) ->
    stream_queue_arguments(
        [{<<"x-max-age">>, longstr, Value}] ++ ArgumentsAcc,
        maps:remove(<<"max-age">>, Arguments)
    );
stream_queue_arguments(ArgumentsAcc, #{<<"max-segment-size">> := Value} = Arguments) ->
    stream_queue_arguments(
        [{<<"x-max-segment-size">>, long, binary_to_integer(Value)}] ++ ArgumentsAcc,
        maps:remove(<<"max-segment-size">>, Arguments)
    );
stream_queue_arguments(ArgumentsAcc, #{<<"initial-cluster-size">> := Value} = Arguments) ->
    stream_queue_arguments(
        [{<<"x-initial-cluster-size">>, long, binary_to_integer(Value)}] ++ ArgumentsAcc,
        maps:remove(<<"initial-cluster-size">>, Arguments)
    );
stream_queue_arguments(ArgumentsAcc, #{<<"queue-leader-locator">> := Value} = Arguments) ->
    stream_queue_arguments(
        [{<<"x-queue-leader-locator">>, longstr, Value}] ++ ArgumentsAcc,
        maps:remove(<<"queue-leader-locator">>, Arguments)
    );
stream_queue_arguments(ArgumentsAcc, _Arguments) ->
    ArgumentsAcc.

validate_stream_queue_arguments([]) ->
    ok;
validate_stream_queue_arguments([{<<"x-initial-cluster-size">>, long, ClusterSize} | _]) when ClusterSize =< 0 ->
    error;
validate_stream_queue_arguments([{<<"x-queue-leader-locator">>, longstr, Locator} | T]) ->
    case lists:member(Locator, [<<"client-local">>,
                                <<"random">>,
                                <<"least-leaders">>]) of
        true  ->
            validate_stream_queue_arguments(T);
        false ->
            error
    end;
validate_stream_queue_arguments([_ | T]) ->
    validate_stream_queue_arguments(T).


handle_call({create, VirtualHost, Reference, Arguments, Username}, _From, State) ->
    Name = #resource{virtual_host = VirtualHost, kind = queue, name = Reference},
    StreamQueueArguments = stream_queue_arguments(Arguments),
    case validate_stream_queue_arguments(StreamQueueArguments) of
        ok ->
            Q0 = amqqueue:new(
                Name,
                none, true, false, none, StreamQueueArguments,
                VirtualHost, #{user => Username}, rabbit_stream_queue
            ),
            try
                case rabbit_stream_queue:declare(Q0, node()) of
                    {new, Q} ->
                        {reply, {ok, amqqueue:get_type_state(Q)}, State};
                    {existing, _} ->
                        {reply, {error, reference_already_exists}, State};
                    {error, Err} ->
                        rabbit_log:warn("Error while creating ~p stream, ~p~n", [Reference, Err]),
                        {reply, {error, internal_error}, State}
                end
            catch
                exit:Error ->
                    rabbit_log:info("Error while creating ~p stream, ~p~n", [Reference, Error]),
                    {reply, {error, internal_error}, State}
            end;
        error ->
            {reply, {error, validation_failed}, State}
    end;
handle_call({delete, VirtualHost, Reference, Username}, _From, State) ->
    Name = #resource{virtual_host = VirtualHost, kind = queue, name = Reference},
    rabbit_log:debug("Trying to delete stream ~p~n", [Reference]),
    case rabbit_amqqueue:lookup(Name) of
        {ok, Q} ->
            rabbit_log:debug("Found queue record ~p, checking if it is a stream~n", [Reference]),
            case is_stream_queue(Q) of
                true ->
                    rabbit_log:debug("Queue record ~p is a stream, trying to delete it~n", [Reference]),
                    {ok, _} = rabbit_stream_queue:delete(Q, false, false, Username),
                    rabbit_log:debug("Stream ~p deleted~n", [Reference]),
                    {reply, {ok, deleted}, State};
                _ ->
                    rabbit_log:debug("Queue record ~p is NOT a stream, returning error~n", [Reference]),
                    {reply, {error, reference_not_found}, State}
            end;
        {error, not_found} ->
            rabbit_log:debug("Stream ~p not found, cannot delete it~n", [Reference]),
            {reply, {error, reference_not_found}, State}
    end;
handle_call({lookup_leader, VirtualHost, Stream}, _From, State) ->
    Name = #resource{virtual_host = VirtualHost, kind = queue, name = Stream},
    Res = case rabbit_amqqueue:lookup(Name) of
              {ok, Q} ->
                  case is_stream_queue(Q) of
                      true ->
                          #{leader_pid := LeaderPid} = amqqueue:get_type_state(Q),
                          LeaderPid;
                      _ ->
                          cluster_not_found
                  end;
              _ ->
                  cluster_not_found
          end,
    {reply, Res, State};
handle_call({lookup_local_member, VirtualHost, Stream}, _From, State) ->
    Name = #resource{virtual_host = VirtualHost, kind = queue, name = Stream},
    Res = case rabbit_amqqueue:lookup(Name) of
              {ok, Q} ->
                  case is_stream_queue(Q) of
                      true ->
                          #{leader_pid := LeaderPid, replica_pids := ReplicaPids} = amqqueue:get_type_state(Q),
                          LocalMember = lists:foldl(fun(Pid, Acc) ->
                              case node(Pid) =:= node() of
                                  true ->
                                      Pid;
                                  false ->
                                      Acc
                              end
                                                    end, undefined, [LeaderPid] ++ ReplicaPids),
                          case LocalMember of
                              undefined ->
                                  {error, not_available};
                              Pid ->
                                  {ok, Pid}
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
                  end;
              _ ->
                  {error, not_found}
          end,
    {reply, Res, State};
handle_call({topology, VirtualHost, Stream}, _From, State) ->
    Name = #resource{virtual_host = VirtualHost, kind = queue, name = Stream},
    Res = case rabbit_amqqueue:lookup(Name) of
              {ok, Q} ->
                  case is_stream_queue(Q) of
                      true ->
                          {ok, maps:with([leader_node, replica_nodes], amqqueue:get_type_state(Q))};
                      _ ->
                          {error, stream_not_found}
                  end;
              {error, not_found} ->
                  case rabbit_amqqueue:not_found_or_absent_dirty(Name) of
                      not_found ->
                          {error, stream_not_found};
                      _ ->
                          {error, stream_not_available}
                  end;
              _ ->
                  {error, stream_not_found}
          end,
    {reply, Res, State};
handle_call(which_children, _From, State) ->
    {reply, [], State}.

handle_cast(_, State) ->
    {noreply, State}.

handle_info(Info, State) ->
    rabbit_log:info("Received info ~p~n", [Info]),
    {noreply, State}.

is_stream_queue(Q) ->
    case amqqueue:get_type(Q) of
        rabbit_stream_queue ->
            true;
        _ ->
            false
    end.