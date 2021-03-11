%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_tracing_consumer).

-behaviour(gen_server).

-include_lib("amqp_client/include/amqp_client.hrl").

-import(rabbit_misc, [pget/2, pget/3, table_lookup/2]).

-record(state, {conn, ch, vhost, queue, file, filename, format, buf, buf_cnt,
                max_payload}).
-record(log_record, {timestamp, type, exchange, queue, node, connection,
                     vhost, username, channel, routing_keys, routed_queues,
                     properties, payload}).

-define(DEFAULT_USERNAME, <<"guest">>).
-define(DEFAULT_PASSWORD, <<"guest">>).

-define(X, <<"amq.rabbitmq.trace">>).
-define(MAX_BUF, 100).

-export([start_link/1, info_all/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

start_link(Args) ->
    gen_server:start_link(?MODULE, Args, []).

info_all(Pid) ->
    gen_server:call(Pid, info_all, infinity).

%%----------------------------------------------------------------------------

init(Args0) ->
    process_flag(trap_exit, true),
    Args = filter_optional_user_pass(Args0),
    Name = pget(name, Args),
    VHost = pget(vhost, Args),
    Username = pget(tracer_connection_username, Args,
                    rabbit_misc:get_env(rabbitmq_tracing, username, ?DEFAULT_USERNAME)),
    Password = pget(tracer_connection_password, Args,
                    rabbit_misc:get_env(rabbitmq_tracing, password, ?DEFAULT_PASSWORD)),
    Username = rabbit_tracing_util:coerce_env_value(username, Username),
    Password = rabbit_tracing_util:coerce_env_value(password, Password),
    MaxPayload = pget(max_payload_bytes, Args, unlimited),
    {ok, Conn} = amqp_connection:start(
                   #amqp_params_direct{virtual_host = VHost,
                                       username = Username,
                                       password = Password}),
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
    amqp_channel:enable_delivery_flow_control(Ch),
    #'basic.consume_ok'{} =
        amqp_channel:subscribe(Ch, #'basic.consume'{queue  = Q,
                                                    no_ack = true}, self()),
    {ok, Dir} = application:get_env(directory),
    Filename = Dir ++ "/" ++ binary_to_list(Name) ++ ".log",
    case filelib:ensure_dir(Filename) of
        ok ->
            case prim_file:open(Filename, [append]) of
                {ok, F} ->
                    rabbit_tracing_traces:announce(VHost, Name, self()),
                    Format = list_to_atom(binary_to_list(pget(format, Args))),
                    rabbit_log:info("Tracer opened log file ~p with "
                                    "format ~p", [Filename, Format]),
                    {ok, #state{conn = Conn, ch = Ch, vhost = VHost, queue = Q,
                                file = F, filename = Filename,
                                format = Format, buf = [], buf_cnt = 0,
                                max_payload = MaxPayload}};
                {error, E} ->
                    {stop, {could_not_open, Filename, E}}
            end;
        {error, E} ->
            {stop, {could_not_create_dir, Dir, E}}
    end.

handle_call(info_all, _From, State = #state{vhost = V, queue = Q}) ->
    [QInfo] = rabbit_mgmt_db:augment_queues(
                [rabbit_mgmt_wm_queue:queue(V, Q)],
                rabbit_mgmt_util:no_range(), basic),
    {reply, [{queue, rabbit_mgmt_format:strip_pids(QInfo)}], State};

handle_call(_Req, _From, State) ->
    {reply, unknown_request, State}.

handle_cast(_C, State) ->
    {noreply, State}.

handle_info({BasicDeliver, Msg, DeliveryCtx},
            State = #state{format = Format}) ->
    amqp_channel:notify_received(DeliveryCtx),
    {noreply, log(Format, delivery_to_log_record({BasicDeliver, Msg}, State),
                  State),
     0};

handle_info(timeout, State) ->
    {noreply, flush(State)};

handle_info(_I, State) ->
    {noreply, State}.

terminate(shutdown, State = #state{conn = Conn, ch = Ch,
                                   file = F, filename = Filename}) ->
    flush(State),
    catch amqp_channel:close(Ch),
    catch amqp_connection:close(Conn),
    catch prim_file:close(F),
    rabbit_log:info("Tracer closed log file ~p", [Filename]),
    ok;

terminate(_Reason, _State) ->
    ok.

code_change(_, State, _) -> {ok, State}.

%%----------------------------------------------------------------------------

delivery_to_log_record({#'basic.deliver'{routing_key = Key},
                        #amqp_msg{props   = #'P_basic'{headers = H},
                                  payload = Payload}}, State) ->
    {Type, Q, RQs} = case Key of
                         <<"publish.", _Rest/binary>> ->
                             {array, Qs} = table_lookup(H, <<"routed_queues">>),
                             {published, none, [Q || {_, Q} <- Qs]};
                         <<"deliver.", Rest/binary>> ->
                             {received,  Rest, none}
                     end,
    {longstr, Node}   = table_lookup(H, <<"node">>),
    {longstr, X}      = table_lookup(H, <<"exchange_name">>),
    {array, Keys}     = table_lookup(H, <<"routing_keys">>),
    {table, Props}    = table_lookup(H, <<"properties">>),
    {longstr, Conn}   = table_lookup(H, <<"connection">>),
    {longstr, VHost}  = table_lookup(H, <<"vhost">>),
    {longstr, User}   = table_lookup(H, <<"user">>),
    {signedint, Chan} = table_lookup(H, <<"channel">>),
    #log_record{timestamp    = rabbit_mgmt_format:now_to_str_ms(
                                 os:system_time(milli_seconds)),
                type         = Type,
                exchange     = X,
                queue        = Q,
                node         = Node,
                connection   = Conn,
                vhost        = VHost,
                username     = User,
                channel      = Chan,
                routing_keys = [K || {_, K} <- Keys],
                routed_queues= RQs,
                properties   = Props,
                payload      = truncate(Payload, State)}.

log(text, Record, State) ->
    Fmt = "~n========================================"
        "========================================~n~s: Message ~s~n~n"
        "Node:         ~s~nConnection:   ~s~n"
        "Virtual host: ~s~nUser:         ~s~n"
        "Channel:      ~p~nExchange:     ~s~n"
        "Routing keys: ~p~n" ++
        case Record#log_record.queue of
            none -> "";
            _    -> "Queue:        ~s~n"
        end ++
        case Record#log_record.routed_queues of
            none -> "";
            _    -> "Routed queues: ~p~n"
        end ++
        "Properties:   ~p~nPayload: ~n~s~n",
    Args =
        [Record#log_record.timestamp,
         Record#log_record.type,
         Record#log_record.node,    Record#log_record.connection,
         Record#log_record.vhost,   Record#log_record.username,
         Record#log_record.channel, Record#log_record.exchange,
         Record#log_record.routing_keys] ++
        case Record#log_record.queue of
            none -> [];
            Q    -> [Q]
        end ++
        case Record#log_record.routed_queues of
            none -> [];
            RQs  -> [RQs]
        end ++
        [Record#log_record.properties, Record#log_record.payload],
    print_log(io_lib:format(Fmt, Args), State);

log(json, Record, State) ->
    print_log([rabbit_json:encode(
                #{timestamp     => Record#log_record.timestamp,
                  type          => Record#log_record.type,
                  node          => Record#log_record.node,
                  connection    => Record#log_record.connection,
                  vhost         => Record#log_record.vhost,
                  user          => Record#log_record.username,
                  channel       => Record#log_record.channel,
                  exchange      => Record#log_record.exchange,
                  queue         => Record#log_record.queue,
                  routed_queues => Record#log_record.routed_queues,
                  routing_keys  => Record#log_record.routing_keys,
                  properties    => rabbit_mgmt_format:amqp_table(
                                     Record#log_record.properties),
                  payload       => base64:encode(Record#log_record.payload)}),
              "\n"],
              State).

print_log(LogMsg, State = #state{buf = Buf, buf_cnt = BufCnt}) ->
    maybe_flush(State#state{buf = [LogMsg | Buf], buf_cnt = BufCnt + 1}).

maybe_flush(State = #state{buf_cnt = ?MAX_BUF}) ->
    flush(State);
maybe_flush(State) ->
    State.

flush(State = #state{file = F, buf = Buf}) ->
    prim_file:write(F, lists:reverse(Buf)),
    State#state{buf = [], buf_cnt = 0}.

truncate(Payload, #state{max_payload = Max}) ->
    case Max =:= unlimited orelse size(Payload) =< Max of
        true  -> Payload;
        false -> <<Trunc:Max/binary, _/binary>> = Payload,
                 Trunc
    end.

filter_optional_user_pass(Args) ->
    case lists:member({tracer_connection_username,<<>>}, Args) of
        true ->
            [{K, V} || {K, V} <- Args,
                       not lists:member(K, [tracer_connection_username,
                                            tracer_connection_password,
                                            <<"tracer_connection_username">>,
                                            <<"tracer_connection_password">>])];
        _ ->
            Args
    end.
