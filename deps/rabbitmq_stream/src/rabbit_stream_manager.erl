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
-export([start_link/1, create/4, register/0, delete/3, lookup_leader/2, lookup_local_member/2, topology/2, unregister/0]).

-record(state, {
    configuration, listeners, monitors
}).

start_link(Conf) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [Conf], []).

init([Conf]) ->
    {ok, #state{configuration = Conf, listeners = [], monitors = #{}}}.

create(VirtualHost, Reference, Arguments, Username) ->
    gen_server:call(?MODULE, {create, VirtualHost, Reference, Arguments, Username}).

delete(VirtualHost, Reference, Username) ->
    gen_server:call(?MODULE, {delete, VirtualHost, Reference, Username}).

register() ->
    gen_server:call(?MODULE, {register, self()}).

unregister() ->
    gen_server:call(?MODULE, {unregister, self()}).

lookup_leader(VirtualHost, Stream) ->
    gen_server:call(?MODULE, {lookup_leader, VirtualHost, Stream}).

lookup_local_member(VirtualHost, Stream) ->
    gen_server:call(?MODULE, {lookup_local_member, VirtualHost, Stream}).

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
stream_queue_arguments(ArgumentsAcc, #{<<"max-segment-size">> := Value} = Arguments) ->
    stream_queue_arguments(
        [{<<"x-max-segment-size">>, long, binary_to_integer(Value)}] ++ ArgumentsAcc,
        maps:remove(<<"max-segment-size">>, Arguments)
    );
stream_queue_arguments(ArgumentsAcc, _Arguments) ->
    ArgumentsAcc.

handle_call({create, VirtualHost, Reference, Arguments, Username}, _From, State) ->
    Name = #resource{virtual_host = VirtualHost, kind = queue, name = Reference},
    Q0 = amqqueue:new(
        Name,
        none, true, false, none, stream_queue_arguments(Arguments),
        VirtualHost, #{user => Username}, rabbit_stream_queue
    ),
    try
        case rabbit_stream_queue:declare(Q0, node()) of
            {new, Q} ->
                {reply, {ok, amqqueue:get_type_state(Q)}, State};
            {existing, _} ->
                {reply, {error, reference_already_exists}, State}
        end
    catch
        exit:Error ->
            error_logger:info_msg("Error while creating ~p stream, ~p~n", [Reference, Error]),
            {reply, {error, internal_error}, State}
    end;
handle_call({delete, VirtualHost, Reference, Username}, _From, #state{listeners = Listeners} = State) ->
    Name = #resource{virtual_host = VirtualHost, kind = queue, name = Reference},
    case rabbit_amqqueue:lookup(Name) of
        {ok, Q} ->
            case is_stream_queue(Q) of
                true ->
                    {ok, _} = rabbit_stream_queue:delete(Q, false, false, Username),
                    [Pid ! {stream_manager, cluster_deleted, Reference} || Pid <- Listeners],
                    {reply, {ok, deleted}, State};
                _ ->
                    {reply, {error, reference_not_found}, State}
            end;
        {error, not_found} ->
            {reply, {error, reference_not_found}, State}
    end;
handle_call({register, Pid}, _From, #state{listeners = Listeners, monitors = Monitors} = State) ->
    case lists:member(Pid, Listeners) of
        false ->
            MonitorRef = erlang:monitor(process, Pid),
            {reply, ok, State#state{listeners = [Pid | Listeners], monitors = Monitors#{Pid => MonitorRef}}};
        true ->
            {reply, ok, State}
    end;
handle_call({unregister, Pid}, _From, #state{listeners = Listeners, monitors = Monitors} = State) ->
    Monitors1 = case maps:get(Pid, Monitors, undefined) of
                    undefined ->
                        Monitors;
                    MonitorRef ->
                        erlang:demonitor(MonitorRef, [flush]),
                        maps:remove(Pid, Monitors)
                end,
    {reply, ok, State#state{listeners = lists:delete(Pid, Listeners), monitors = Monitors1}};
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
                                  {error, not_found};
                              Pid ->
                                  {ok, Pid}
                          end;
                      _ ->
                          {error, not_found}
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
              _ ->
                  {error, stream_not_found}
          end,
    {reply, Res, State}.

handle_cast(_, State) ->
    {noreply, State}.

handle_info({'DOWN', _MonitorRef, process, Pid, _Info}, #state{listeners = Listeners, monitors = Monitors} = State) ->
    {noreply, State#state{listeners = lists:delete(Pid, Listeners), monitors = maps:remove(Pid, Monitors)}};
handle_info(Info, State) ->
    error_logger:info_msg("Received info ~p~n", [Info]),
    {noreply, State}.

is_stream_queue(Q) ->
    case amqqueue:get_type(Q) of
        rabbit_stream_queue ->
            true;
        _ ->
            false
    end.