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
%% The Initial Developer of the Original Code is Pivotal Software, Inc.
%% Copyright (c) 2020 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_stream_manager).
-behaviour(gen_server).

-include_lib("rabbit_common/include/rabbit.hrl").

%% API
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).
-export([start_link/1, create/3, register/0, delete/3, lookup/2, unregister/0]).

-record(state, {
    configuration, listeners, monitors
}).

start_link(Conf) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [Conf], []).

init([Conf]) ->
    {ok, #state{configuration = Conf, listeners = [], monitors = #{}}}.

create(VirtualHost, Reference, Username) ->
    gen_server:call(?MODULE, {create, VirtualHost, Reference, Username}).

delete(VirtualHost, Reference, Username) ->
    gen_server:call(?MODULE, {delete, VirtualHost, Reference, Username}).

register() ->
    gen_server:call(?MODULE, {register, self()}).

unregister() ->
    gen_server:call(?MODULE, {unregister, self()}).

lookup(VirtualHost, Stream) ->
    gen_server:call(?MODULE, {lookup, VirtualHost, Stream}).

handle_call({create, VirtualHost, Reference, Username}, _From, State) ->
    Name = #resource{virtual_host = VirtualHost, kind = queue, name = Reference},
    Q0 = amqqueue:new(
        Name,
        none, true, false, none, [{<<"x-queue-type">>, longstr, <<"stream">>}],
        VirtualHost, #{user => Username}, rabbit_stream_queue
    ),
    try
        case rabbit_stream_queue:declare(Q0, node()) of
            {new, Q} ->
                {reply, {ok, amqqueue:get_type_state(Q)}, State};
            _ ->
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
            {reply, {error, reference_not_found}, State};
        Other ->
            error_logger:info_msg("Unexpected result when trying to look up stream ~p for deletion, ~p~n",
                [Reference, Other]),
            {reply, {error, internal_error}, State}
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
handle_call({lookup, VirtualHost, Stream}, _From, State) ->
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