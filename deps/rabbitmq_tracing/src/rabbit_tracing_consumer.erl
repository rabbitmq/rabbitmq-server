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
%% The Original Code is RabbitMQ Federation.
%%
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2007-2011 VMware, Inc.  All rights reserved.
%%

-module(rabbit_tracing_consumer).

-behaviour(gen_server).

-include_lib("amqp_client/include/amqp_client.hrl").

-import(rabbit_misc, [pget/2, pget/3, table_lookup/2]).

-record(state, {conn, ch, vhost, queue, file, filename}).
-define(X, <<"amq.rabbitmq.trace">>).

-export([start_link/1, info_all/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

start_link(Args) ->
    gen_server:start_link(?MODULE, Args, []).

info_all(Pid) ->
    gen_server:call(Pid, info_all, infinity).

%%----------------------------------------------------------------------------

init(Args) ->
    process_flag(trap_exit, true),
    Name = pget(name, Args),
    VHost = pget(vhost, Args),
    {ok, Conn} = amqp_connection:start(
                   #amqp_params_direct{virtual_host = VHost}),
    link(Conn),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    link(Ch),
    #'queue.declare_ok'{queue = Q} =
        amqp_channel:call(Ch, #'queue.declare'{durable   = false,
                                               exclusive = true}),
    #'queue.bind_ok'{} =
    amqp_channel:call(
      Ch, #'queue.bind'{exchange = ?X, queue = Q,
                        routing_key = pget(pattern, Args)}),
    #'basic.qos_ok'{} =
        amqp_channel:call(Ch, #'basic.qos'{prefetch_count = 10}),
    #'basic.consume_ok'{} =
        amqp_channel:subscribe(Ch, #'basic.consume'{queue  = Q,
                                                    no_ack = false}, self()),
    {ok, Dir} = application:get_env(directory),
    Filename = Dir ++ "/" ++ binary_to_list(Name) ++ ".log",
    ok = filelib:ensure_dir(Filename),
    {ok, F} = file:open(Filename, [append]),
    rabbit_tracing_traces:announce(VHost, Name, self()),
    rabbit_log:info("Tracer opened log file ~p~n", [Filename]),
    {ok, #state{conn = Conn, ch = Ch, vhost = VHost, queue = Q,
                file = F, filename = Filename}}.

handle_call(info_all, _From, State = #state{vhost = V, queue = Q}) ->
    [QInfo] = rabbit_mgmt_db:augment_queues(
                [rabbit_mgmt_wm_queue:queue(V, Q)], basic),
    {reply, [{queue, rabbit_mgmt_format:strip_pids(QInfo)}], State};

handle_call(_Req, _From, State) ->
    {reply, unknown_request, State}.

handle_cast(_C, State) ->
    {noreply, State}.

handle_info({#'basic.deliver'{routing_key = Key, delivery_tag = Seq},
             #amqp_msg{props = #'P_basic'{headers = H}, payload = Payload}},
            State = #state{ch = Ch, file = F}) ->
    P = fun(Fmt, Args) -> io:format(F, Fmt, Args) end,
    P("~n~s~n", [string:copies("=", 80)]),
    P("~s: ", [rabbit_mgmt_format:timestamp(os:timestamp())]),
    Q = case Key of
            <<"publish.", _Rest/binary>> ->
                P("Message published~n~n", []),
                none;
            <<"deliver.", Rest/binary>> ->
                P("Message received~n~n", []),
                Rest
        end,
    {longstr, Node} = table_lookup(H, <<"node">>),
    P("Node:         ~s~n", [Node]),
    {longstr, X} = table_lookup(H, <<"exchange_name">>),
    P("Exchange:     ~s~n", [X]),
    case Q of
        none -> ok;
        _    -> P("Queue:        ~s~n", [Q])
    end,
    {array, Keys} = table_lookup(H, <<"routing_keys">>),
    P("Routing keys: ~p~n", [[binary_to_list(K) || {_, K} <- Keys]]),
    {table, Props} = table_lookup(H, <<"properties">>),
    P("Properties:   ~p~n", [Props]),
    P("Payload: ~n~s~n", [Payload]),
    amqp_channel:cast(Ch, #'basic.ack'{delivery_tag = Seq}),
    {noreply, State};

handle_info(_I, State) ->
    {noreply, State}.

terminate(shutdown, #state{conn = Conn, ch = Ch,
                           file = F, filename = Filename}) ->
    catch amqp_channel:close(Ch),
    catch amqp_connection:close(Conn),
    catch file:close(F),
    rabbit_log:info("Tracer closed log file ~p~n", [Filename]),
    ok;

terminate(_Reason, _State) ->
    ok.

code_change(_, State, _) -> {ok, State}.

%%----------------------------------------------------------------------------
