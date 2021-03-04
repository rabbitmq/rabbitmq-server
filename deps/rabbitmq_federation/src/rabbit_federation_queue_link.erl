%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_federation_queue_link).

-include_lib("rabbit/include/amqqueue.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include("rabbit_federation.hrl").

-behaviour(gen_server2).

-export([start_link/1, go/0, run/1, pause/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-import(rabbit_misc, [pget/2]).
-import(rabbit_federation_util, [name/1, pgname/1]).

-record(not_started, {queue, run, upstream, upstream_params}).
-record(state, {queue, run, conn, ch, dconn, dch, upstream, upstream_params,
                unacked}).

start_link(Args) ->
    gen_server2:start_link(?MODULE, Args, [{timeout, infinity}]).

run(QName)   -> cast(QName, run).
pause(QName) -> cast(QName, pause).
go()         ->
    rabbit_federation_pg:start_scope(),
    cast(go).

%%----------------------------------------------------------------------------
%%call(QName, Msg) -> [gen_server2:call(Pid, Msg, infinity) || Pid <- q(QName)].
cast(Msg)        -> [gen_server2:cast(Pid, Msg) || Pid <- all()].
cast(QName, Msg) -> [gen_server2:cast(Pid, Msg) || Pid <- q(QName)].

join(Name) ->
    ok = pg:join(?FEDERATION_PG_SCOPE, pgname(Name), self()).

all() ->
    pg:get_members(?FEDERATION_PG_SCOPE, pgname(rabbit_federation_queues)).

q(QName) ->
    case pg:get_members(?FEDERATION_PG_SCOPE, pgname({rabbit_federation_queue, QName})) of
        {error, {no_such_group, _}} ->
            [];
        Members ->
            Members
    end.

%%----------------------------------------------------------------------------

federation_up() ->
    is_pid(whereis(rabbit_federation_app)).

init({Upstream, Queue}) when ?is_amqqueue(Queue) ->
    QName = amqqueue:get_name(Queue),
    case rabbit_amqqueue:lookup(QName) of
        {ok, Q} ->
            DeobfuscatedUpstream = rabbit_federation_util:deobfuscate_upstream(Upstream),
            DeobfuscatedUParams = rabbit_federation_upstream:to_params(DeobfuscatedUpstream, Queue),
            UParams = rabbit_federation_util:obfuscate_upstream_params(DeobfuscatedUParams),
            rabbit_federation_status:report(Upstream, UParams, QName, starting),
            join(rabbit_federation_queues),
            join({rabbit_federation_queue, QName}),
            gen_server2:cast(self(), maybe_go),
            rabbit_amqqueue:notify_decorators(Q),
            {ok, #not_started{queue           = Queue,
                              run             = false,
                              upstream        = Upstream,
                              upstream_params = UParams}};
        {error, not_found} ->
            rabbit_federation_link_util:log_warning(QName, "not found, stopping link~n", []),
            {stop, gone}
    end.

handle_call(Msg, _From, State) ->
    {stop, {unexpected_call, Msg}, State}.

handle_cast(maybe_go, State) ->
    go(State);

handle_cast(go, State = #not_started{}) ->
    go(State);

handle_cast(go, State) ->
    {noreply, State};

handle_cast(run, State = #state{upstream        = Upstream,
                                upstream_params = UParams,
                                ch              = Ch,
                                run             = false}) ->
    consume(Ch, Upstream, UParams#upstream_params.x_or_q),
    {noreply, State#state{run = true}};

handle_cast(run, State = #not_started{}) ->
    {noreply, State#not_started{run = true}};

handle_cast(run, State) ->
    %% Already started
    {noreply, State};

handle_cast(pause, State = #state{run = false}) ->
    %% Already paused
    {noreply, State};

handle_cast(pause, State = #not_started{}) ->
    {noreply, State#not_started{run = false}};

handle_cast(pause, State = #state{ch = Ch, upstream = Upstream}) ->
    cancel(Ch, Upstream),
    {noreply, State#state{run = false}};

handle_cast(Msg, State) ->
    {stop, {unexpected_cast, Msg}, State}.

handle_info(#'basic.consume_ok'{}, State) ->
    {noreply, State};

handle_info(#'basic.ack'{} = Ack, State = #state{ch      = Ch,
                                                 unacked = Unacked}) ->
    Unacked1 = rabbit_federation_link_util:ack(Ack, Ch, Unacked),
    {noreply, State#state{unacked = Unacked1}};

handle_info(#'basic.nack'{} = Nack, State = #state{ch      = Ch,
                                                   unacked = Unacked}) ->
    Unacked1 = rabbit_federation_link_util:nack(Nack, Ch, Unacked),
    {noreply, State#state{unacked = Unacked1}};

handle_info({#'basic.deliver'{redelivered = Redelivered,
                              exchange    = X,
                              routing_key = K} = DeliverMethod, Msg},
            State = #state{queue           = Q,
                           upstream        = Upstream,
                           upstream_params = UParams,
                           ch              = Ch,
                           dch             = DCh,
                           unacked         = Unacked}) when ?is_amqqueue(Q) ->
    QName = amqqueue:get_name(Q),
    PublishMethod = #'basic.publish'{exchange    = <<"">>,
                                     routing_key = QName#resource.name},
    HeadersFun = fun (H) -> update_headers(UParams, Redelivered, X, K, H) end,
    ForwardFun = fun (_H) -> true end,
    Unacked1 = rabbit_federation_link_util:forward(
                 Upstream, DeliverMethod, Ch, DCh, PublishMethod,
                 HeadersFun, ForwardFun, Msg, Unacked),
    %% TODO actually we could reject when 'stopped'
    {noreply, State#state{unacked = Unacked1}};

handle_info(#'basic.cancel'{},
            State = #state{queue           = Q,
                           upstream        = Upstream,
                           upstream_params = UParams}) when ?is_amqqueue(Q) ->
    QName = amqqueue:get_name(Q),
    rabbit_federation_link_util:connection_error(
      local, basic_cancel, Upstream, UParams, QName, State);

handle_info({'DOWN', _Ref, process, Pid, Reason},
            State = #state{dch             = DCh,
                           ch              = Ch,
                           upstream        = Upstream,
                           upstream_params = UParams,
                           queue           = Q}) when ?is_amqqueue(Q) ->
    QName = amqqueue:get_name(Q),
    handle_down(Pid, Reason, Ch, DCh, {Upstream, UParams, QName}, State);

handle_info(Msg, State) ->
    {stop, {unexpected_info, Msg}, State}.

terminate(Reason, #not_started{upstream        = Upstream,
                               upstream_params = UParams,
                               queue           = Q}) when ?is_amqqueue(Q) ->
    QName = amqqueue:get_name(Q),
    rabbit_federation_link_util:log_terminate(Reason, Upstream, UParams, QName),
    _ = pg:leave(?FEDERATION_PG_SCOPE, pgname({rabbit_federation_queue, QName}), self()),
    ok;

terminate(Reason, #state{dconn           = DConn,
                         conn            = Conn,
                         upstream        = Upstream,
                         upstream_params = UParams,
                         queue           = Q}) when ?is_amqqueue(Q) ->
    QName = amqqueue:get_name(Q),
    rabbit_federation_link_util:ensure_connection_closed(DConn),
    rabbit_federation_link_util:ensure_connection_closed(Conn),
    rabbit_federation_link_util:log_terminate(Reason, Upstream, UParams, QName),
    _ = pg:leave(?FEDERATION_PG_SCOPE, pgname({rabbit_federation_queue, QName}), self()),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%----------------------------------------------------------------------------

go(S0 = #not_started{run             = Run,
                     upstream        = Upstream = #upstream{
                                         prefetch_count = Prefetch},
                     upstream_params = UParams,
                     queue           = Queue}) when ?is_amqqueue(Queue) ->
    QName = amqqueue:get_name(Queue),
    #upstream_params{x_or_q = UQueue} = UParams,
    Durable = amqqueue:is_durable(UQueue),
    AutoDelete = amqqueue:is_auto_delete(UQueue),
    Args = amqqueue:get_arguments(UQueue),
    Unacked = rabbit_federation_link_util:unacked_new(),
    rabbit_federation_link_util:start_conn_ch(
      fun (Conn, Ch, DConn, DCh) ->
              check_upstream_suitable(Conn),
              amqp_channel:call(Ch, #'queue.declare'{queue       = name(UQueue),
                                                     durable     = Durable,
                                                     auto_delete = AutoDelete,
                                                     arguments   = Args}),
              case Upstream#upstream.ack_mode of
                  'no-ack' -> ok;
                  _        -> amqp_channel:call(
                                Ch, #'basic.qos'{prefetch_count = Prefetch})
              end,
              amqp_selective_consumer:register_default_consumer(Ch, self()),
              case Run of
                  true  -> consume(Ch, Upstream, UQueue);
                  false -> ok
              end,
              {noreply, #state{queue           = Queue,
                               run             = Run,
                               conn            = Conn,
                               ch              = Ch,
                               dconn           = DConn,
                               dch             = DCh,
                               upstream        = Upstream,
                               upstream_params = UParams,
                               unacked         = Unacked}}
      end, Upstream, UParams, QName, S0).

check_upstream_suitable(Conn) ->
    Props = pget(server_properties,
                 amqp_connection:info(Conn, [server_properties])),
    {table, Caps} = rabbit_misc:table_lookup(Props, <<"capabilities">>),
    case rabbit_misc:table_lookup(Caps, <<"consumer_priorities">>) of
        {bool, true} -> ok;
        _            -> exit({error, upstream_lacks_consumer_priorities})
    end.

update_headers(UParams, Redelivered, X, K, undefined) ->
    update_headers(UParams, Redelivered, X, K, []);

update_headers(#upstream_params{table = Table}, Redelivered, X, K, Headers) ->
    {Headers1, Count} =
        case rabbit_misc:table_lookup(Headers, ?ROUTING_HEADER) of
            undefined ->
                %% We only want to record the original exchange and
                %% routing key the first time a message gets
                %% forwarded; after that it's known that they were
                %% <<>> and QueueName respectively.
                {init_x_original_source_headers(Headers, X, K), 0};
            {array, Been} ->
                update_visit_count(Table, Been, Headers);
            %% this means the header comes from the client
            %% which re-published the message, most likely unintentionally.
            %% We can't assume much about the value, so we simply ignore it.
            _Other ->
                {init_x_original_source_headers(Headers, X, K), 0}
        end,
    rabbit_basic:prepend_table_header(
      ?ROUTING_HEADER, Table ++ [{<<"redelivered">>, bool, Redelivered},
                                 {<<"visit-count">>, long, Count + 1}],
      swap_cc_header(Headers1)).

init_x_original_source_headers(Headers, X, K) ->
    rabbit_misc:set_table_value(
        rabbit_misc:set_table_value(
            Headers, <<"x-original-exchange">>, longstr, X),
        <<"x-original-routing-key">>, longstr, K).

update_visit_count(Table, Been, Headers) ->
    {Found, Been1} = lists:partition(
        fun(I) -> visit_match(I, Table) end,
        Been),
    C = case Found of
            [] -> 0;
            [{table, T}] -> case rabbit_misc:table_lookup(
                T, <<"visit-count">>) of
                                {_, I} when is_number(I) -> I;
                                _ -> 0
                            end
        end,
    {rabbit_misc:set_table_value(
        Headers, ?ROUTING_HEADER, array, Been1), C}.

swap_cc_header(Table) ->
    [{case K of
          <<"CC">> -> <<"x-original-cc">>;
          _        -> K
      end, T, V} || {K, T, V} <- Table].

visit_match({table, T}, Info) ->
    lists:all(fun (K) ->
                      rabbit_misc:table_lookup(T, K) =:=
                          rabbit_misc:table_lookup(Info, K)
              end, [<<"uri">>, <<"virtual_host">>, <<"queue">>]);
visit_match(_ ,_) ->
    false.

consumer_tag(#upstream{consumer_tag = ConsumerTag}) ->
    ConsumerTag.

consume(Ch, Upstream, UQueue) ->
    ConsumerTag = consumer_tag(Upstream),
    NoAck = Upstream#upstream.ack_mode =:= 'no-ack',
    amqp_channel:cast(
      Ch, #'basic.consume'{queue        = name(UQueue),
                           no_ack       = NoAck,
                           nowait       = true,
                           consumer_tag = ConsumerTag,
                           arguments    = [{<<"x-priority">>, long, -1}]}).

cancel(Ch, Upstream) ->
    ConsumerTag = consumer_tag(Upstream),
    amqp_channel:cast(Ch, #'basic.cancel'{nowait       = true,
                                          consumer_tag = ConsumerTag}).

handle_down(DCh, Reason, _Ch, DCh, Args, State) ->
    rabbit_federation_link_util:handle_downstream_down(Reason, Args, State);
handle_down(Ch, Reason, Ch, _DCh, Args, State) ->
    rabbit_federation_link_util:handle_upstream_down(Reason, Args, State).
