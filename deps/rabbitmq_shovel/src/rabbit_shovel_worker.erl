%%  The contents of this file are subject to the Mozilla Public License
%%  Version 1.1 (the "License"); you may not use this file except in
%%  compliance with the License. You may obtain a copy of the License
%%  at http://www.mozilla.org/MPL/
%%
%%  Software distributed under the License is distributed on an "AS IS"
%%  basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%%  the License for the specific language governing rights and
%%  limitations under the License.
%%
%%  The Original Code is RabbitMQ.
%%
%%  The Initial Developer of the Original Code is VMware, Inc.
%%  Copyright (c) 2007-2011 VMware, Inc.  All rights reserved.
%%

-module(rabbit_shovel_worker).
-behaviour(gen_server2).

-export([start_link/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-include_lib("amqp_client/include/amqp_client.hrl").
-include("rabbit_shovel.hrl").

-record(state, {inbound_conn, inbound_ch, outbound_conn, outbound_ch,
                name, config, blocked, msg_buf, inbound_params,
                outbound_params}).

start_link(Name, Config) ->
    ok = rabbit_shovel_status:report(Name, starting),
    gen_server2:start_link(?MODULE, [Name, Config], []).

%%---------------------------
%% Gen Server Implementation
%%---------------------------

init([Name, Config]) ->
    gen_server2:cast(self(), init),
    {ok, #state{name = Name, config = Config}}.

handle_call(_Msg, _From, State) ->
    {noreply, State}.

handle_cast(init, State = #state{config = Config}) ->
    process_flag(trap_exit, true),
    %% TODO when we move to minimum R13B01:
    %% random:seed(now()),
    {A, B, C} = now(),
    random:seed(A, B, C),
    #shovel{sources = Sources, destinations = Destinations} = Config,
    {InboundConn, InboundChan, InboundParams} =
        make_conn_and_chan(Sources#endpoint.amqp_params),
    {OutboundConn, OutboundChan, OutboundParams} =
        make_conn_and_chan(Destinations#endpoint.amqp_params),

    create_resources(InboundChan,
                     Sources#endpoint.resource_declarations),

    create_resources(OutboundChan,
                     Destinations#endpoint.resource_declarations),

    #'basic.qos_ok'{} =
        amqp_channel:call(InboundChan,
                          #'basic.qos'{
                            prefetch_count = Config#shovel.prefetch_count}),

    ok = amqp_channel:register_flow_handler(OutboundChan, self()),

    #'basic.consume_ok'{} =
        amqp_channel:subscribe(
          InboundChan,
          #'basic.consume'{queue  = Config#shovel.queue,
                           no_ack = Config#shovel.auto_ack},
          self()),

    State1 =
        State#state{inbound_conn = InboundConn, inbound_ch = InboundChan,
                    outbound_conn = OutboundConn, outbound_ch = OutboundChan,
                    blocked = false, msg_buf = queue:new(),
                    inbound_params = InboundParams,
                    outbound_params = OutboundParams},
    ok = report_status(running, State1),
    {noreply, State1}.

handle_info(#'basic.consume_ok'{}, State) ->
    {noreply, State};

handle_info({#'basic.deliver'{delivery_tag = Tag,
                              exchange = Exchange, routing_key = RoutingKey},
             Msg = #amqp_msg{props = Props = #'P_basic'{}}},
            State = #state{config = Config}) ->
    Method = #'basic.publish'{exchange = Exchange, routing_key = RoutingKey},
    Method1 = (Config#shovel.publish_fields)(Method),
    Msg1 = Msg#amqp_msg{props = (Config#shovel.publish_properties)(Props)},
    {noreply, publish(Tag, Method1, Msg1, State)};

handle_info(#'channel.flow'{active = true},
            State = #state{inbound_ch = InboundChan}) ->
    ok = report_status(running, State),
    State1 = drain_buffer(State#state{blocked = false}),
    ok = case State1#state.blocked of
             true  -> report_status(blocked, State);
             false -> channel_flow(InboundChan, true)
         end,
    {noreply, State1};

handle_info(#'channel.flow'{active = false},
            State = #state{inbound_ch = InboundChan}) ->
    ok = report_status(blocked, State),
    ok = channel_flow(InboundChan, false),
    {noreply, State#state{blocked = true}};

handle_info({'EXIT', InboundConn, Reason},
            State = #state{inbound_conn = InboundConn}) ->
    {stop, {inbound_conn_died, Reason}, State};

handle_info({'EXIT', OutboundConn, Reason},
            State = #state{outbound_conn = OutboundConn}) ->
    {stop, {outbound_conn_died, Reason}, State}.

terminate(Reason, #state{inbound_conn = undefined, inbound_ch = undefined,
                         outbound_conn = undefined, outbound_ch = undefined,
                         name = Name}) ->
    rabbit_shovel_status:report(Name, {terminated, Reason}),
    ok;
terminate(Reason, State) ->
    catch amqp_channel:close(State#state.inbound_ch),
    catch amqp_connection:close(State#state.inbound_conn),
    catch amqp_channel:close(State#state.outbound_ch),
    catch amqp_connection:close(State#state.outbound_conn),
    rabbit_shovel_status:report(State#state.name, {terminated, Reason}),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%---------------------------
%% Helpers
%%---------------------------

report_status(Verb, State) ->
    rabbit_shovel_status:report(
      State#state.name, {Verb, {source, State#state.inbound_params},
                         {destination, State#state.outbound_params}}).

publish(Tag, Method, Msg,
        State = #state{inbound_ch = InboundChan, outbound_ch = OutboundChan,
                       config = Config, blocked = false, msg_buf = MsgBuf}) ->
    case amqp_channel:call(OutboundChan, Method, Msg) of
        ok ->
            case Config#shovel.auto_ack of
                true  -> ok;
                false -> amqp_channel:cast(InboundChan,
                                           #'basic.ack'{delivery_tag = Tag})
            end;
        blocked ->
            ok = report_status(blocked, State),
            ok = channel_flow(InboundChan, false),
            State#state{blocked = true,
                        msg_buf = queue:in_r({Tag, Method, Msg}, MsgBuf)}
    end;
publish(Tag, Method, Msg, State = #state{blocked = true, msg_buf = MsgBuf}) ->
    State#state{msg_buf = queue:in({Tag, Method, Msg}, MsgBuf)}.

drain_buffer(State = #state{blocked = true}) ->
    State;
drain_buffer(State = #state{blocked = false, msg_buf = MsgBuf}) ->
    case queue:out(MsgBuf) of
        {empty, _MsgBuf} ->
            State;
        {{value, {Tag, Method, Msg}}, MsgBuf1} ->
            drain_buffer(publish(Tag, Method, Msg,
                                 State#state{msg_buf = MsgBuf1}))
    end.

channel_flow(Chan, Active) ->
    #'channel.flow_ok'{active = Active} =
        amqp_channel:call(Chan, #'channel.flow'{active = Active}),
    ok.

make_conn_and_chan(AmqpParams) ->
    AmqpParam = lists:nth(random:uniform(length(AmqpParams)), AmqpParams),
    {ok, Conn} = amqp_connection:start(AmqpParam),
    link(Conn),
    {ok, Chan} = amqp_connection:open_channel(Conn),
    {Conn, Chan, AmqpParam}.

create_resources(Chan, Declarations) ->
    [amqp_channel:call(Chan, Method) || Method <- Declarations].
