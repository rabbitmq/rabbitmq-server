%%   The contents of this file are subject to the Mozilla Public License
%%   Version 1.1 (the "License"); you may not use this file except in
%%   compliance with the License. You may obtain a copy of the License at
%%   http://www.mozilla.org/MPL/
%%
%%   Software distributed under the License is distributed on an "AS IS"
%%   basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%%   License for the specific language governing rights and limitations
%%   under the License.
%%
%%   The Original Code is RabbitMQ-shovel.
%%
%%   The Initial Developers of the Original Code are LShift Ltd.
%%
%%   Copyright (C) 2010 LShift Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%
-module(rabbit_shovel_worker).
-behaviour(gen_server).

-export([start_link/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-include("rabbit_shovel.hrl").

-record(state, {inbound_conn, inbound_ch, outbound_conn, outbound_ch,
                tx_counter, name, config}).

start_link(Name, Config) ->
    rabbit_shovel_status:report(Name, starting),
    gen_server:start_link(?MODULE, [Name, Config], []).

%---------------------------
% Gen Server Implementation
%---------------------------

init([Name, Config]) ->
    process_flag(trap_exit, true),
    gen_server:cast(self(), init),
    {ok, #state { name = Name, config = Config }}.

handle_call(_Msg, _From, State) ->
    {noreply, State}.

handle_cast(init, State = #state { name = Name, config = Config }) ->
    random:seed(now()),

    {InboundConn, InboundChan, InboundParams} =
        make_conn_and_chan((Config #shovel.sources) #endpoint.amqp_params),
    {OutboundConn, OutboundChan, OutboundParams} =
        make_conn_and_chan((Config #shovel.destinations) #endpoint.amqp_params),

    create_resources(OutboundChan, (Config #shovel.destinations)
                     #endpoint.resource_declarations),

    create_resources(InboundChan, (Config #shovel.sources)
                     #endpoint.resource_declarations),

    #'basic.qos_ok'{} =
        amqp_channel:call(InboundChan,
                          #'basic.qos'{ prefetch_count = Config #shovel.qos }),

    ok = case Config #shovel.tx_size of
                 0 -> ok;
                 _ -> #'tx.select_ok'{} =
                          amqp_channel:call(OutboundChan, #'tx.select'{}),
                      ok
         end,

    #'basic.consume_ok'{} =
        amqp_channel:subscribe(
          InboundChan,
          #'basic.consume'{queue = Config #shovel.queue,
                           no_ack = Config #shovel.auto_ack},
          self()),

    rabbit_shovel_status:report(Name, {running, {source, InboundParams},
                                                {destination, OutboundParams}}),
    {noreply,
     State #state { inbound_conn = InboundConn, inbound_ch = InboundChan,
                    outbound_conn = OutboundConn, outbound_ch = OutboundChan,
                    tx_counter = 0 }}.

handle_info(#'basic.consume_ok'{}, State) ->
    {noreply, State};

handle_info({#'basic.deliver'{ delivery_tag = Tag, routing_key = RoutingKey },
             Msg = #amqp_msg{ props = Props = #'P_basic'{} }},
            State = #state{ inbound_ch = InboundChan, outbound_ch = OutboundChan,
                            config = Config, tx_counter = TxCounter }) ->
    Props1 = case Config #shovel.delivery_mode of
                 keep -> Props;
                 Mode -> Props #'P_basic'{ delivery_mode = Mode }
             end,
    ok = amqp_channel:call(OutboundChan,
                           (Config #shovel.publish_fields)(
                             #'basic.publish'{ routing_key = RoutingKey }),
                           Msg #amqp_msg{ props = Props1 }),
    {Ack, AckMulti, TxCounter1} =
        case {Config #shovel.tx_size, TxCounter} of
            {0, _}            -> {true,  false, TxCounter};
            {N, N}            -> #'tx.commit_ok'{} =
                                     amqp_channel:call(OutboundChan,
                                                       #'tx.commit'{}),
                                 {true,  true,  0};
            {N, M} when N > M -> {false, false, M + 1}
        end,
    case Ack andalso not (Config #shovel.auto_ack) of
        true -> amqp_channel:cast(InboundChan,
                                  #'basic.ack'{ delivery_tag = Tag,
                                                multiple = AckMulti });
        _    -> ok
    end,
    {noreply, State #state { tx_counter = TxCounter1 }}.

terminate(Reason,
          #state { inbound_conn = undefined, inbound_ch = undefined,
                   outbound_conn = undefined, outbound_ch = undefined,
                   name = Name }) ->
    rabbit_shovel_status:report(Name, {terminated, Reason}),
    ok;
terminate(Reason, State) ->
    amqp_channel:close(State #state.inbound_ch),
    amqp_connection:close(State #state.inbound_conn),
    amqp_channel:close(State #state.outbound_ch),
    amqp_connection:close(State #state.outbound_conn),
    rabbit_shovel_status:report(State #state.name, {terminated, Reason}),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%---------------------------
% Helpers
%---------------------------

make_conn_and_chan(AmqpParams) ->
    AmqpParam = lists:nth(random:uniform(length(AmqpParams)), AmqpParams),
    Conn = case AmqpParam #amqp_params.host of
               undefined -> amqp_connection:start_direct_link(AmqpParam);
               _         -> amqp_connection:start_network_link(AmqpParam)
           end,
    Chan = amqp_connection:open_channel(Conn),
    {Conn, Chan, AmqpParam}.

create_resources(Chan, Declarations) ->
    [amqp_channel:call(Chan, Method) || Method <- Declarations].
