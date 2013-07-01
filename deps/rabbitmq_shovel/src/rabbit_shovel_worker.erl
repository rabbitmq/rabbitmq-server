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
%%  The Initial Developer of the Original Code is GoPivotal, Inc.
%%  Copyright (c) 2007-2013 GoPivotal, Inc.  All rights reserved.
%%

-module(rabbit_shovel_worker).
-behaviour(gen_server2).

-export([start_link/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-include_lib("amqp_client/include/amqp_client.hrl").
-include("rabbit_shovel.hrl").

-define(MAX_CONNECTION_CLOSE_TIMEOUT, 10000).

-record(state, {inbound_conn, inbound_ch, outbound_conn, outbound_ch,
                name, config, blocked, msg_buf, inbound_params,
                outbound_params, unacked}).

start_link(Name, Config) ->
    ok = rabbit_shovel_status:report(Name, starting),
    gen_server2:start_link(?MODULE, [Name, Config], []).

%%---------------------------
%% Gen Server Implementation
%%---------------------------

init([Name, Config]) ->
    gen_server2:cast(self(), init),
    {ok, Shovel} = rabbit_shovel_config:parse(Name, Config),
    {ok, #state{name = Name, config = Shovel}}.

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

    case Config#shovel.ack_mode of
        on_confirm ->
            #'confirm.select_ok'{} =
                amqp_channel:call(OutboundChan, #'confirm.select'{}),
            ok = amqp_channel:register_confirm_handler(OutboundChan, self());
        _ ->
            ok
    end,

    #'basic.consume_ok'{} =
        amqp_channel:subscribe(
          InboundChan,
          #'basic.consume'{queue  = Config#shovel.queue,
                           no_ack = Config#shovel.ack_mode =:= no_ack},
          self()),

    State1 =
        State#state{inbound_conn = InboundConn, inbound_ch = InboundChan,
                    outbound_conn = OutboundConn, outbound_ch = OutboundChan,
                    blocked = false, msg_buf = queue:new(),
                    inbound_params = InboundParams,
                    outbound_params = OutboundParams,
                    unacked = gb_trees:empty()},
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

handle_info(#'basic.ack'{delivery_tag = Seq, multiple = Multiple},
            State = #state{config = #shovel{ack_mode = on_confirm}}) ->
    {noreply, confirm_to_inbound(
                fun (DTag, Multiple) ->
                        #'basic.ack'{delivery_tag = DTag, multiple = Multiple}
                end, Seq, Multiple, State)};

handle_info(#'basic.nack'{delivery_tag = Seq, multiple = Multiple},
            State = #state{config = #shovel{ack_mode = on_confirm}}) ->
    {noreply, confirm_to_inbound(
                fun (DTag, Multiple) ->
                        #'basic.nack'{delivery_tag = DTag, multiple = Multiple}
                end, Seq, Multiple, State)};

handle_info(#'basic.cancel'{}, State = #state{name = Name}) ->
    rabbit_log:warning("Shovel ~p received 'basic.cancel' from the broker~n",
                       [Name]),
    {stop, {shutdown, restart}, State};

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
    catch amqp_connection:close(State#state.inbound_conn,
                                ?MAX_CONNECTION_CLOSE_TIMEOUT),
    catch amqp_connection:close(State#state.outbound_conn,
                                ?MAX_CONNECTION_CLOSE_TIMEOUT),
    rabbit_shovel_status:report(State#state.name, {terminated, Reason}),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%---------------------------
%% Helpers
%%---------------------------

confirm_to_inbound(MsgCtr, Seq, Multiple, State =
                       #state{inbound_ch = InboundChan, unacked = Unacked}) ->
    ok = amqp_channel:cast(
           InboundChan, MsgCtr(gb_trees:get(Seq, Unacked), Multiple)),
    Unacked1 = remove_delivery_tags(Seq, Multiple, Unacked),
    State#state{unacked = Unacked1}.

remove_delivery_tags(Seq, false, Unacked) ->
    gb_trees:delete(Seq, Unacked);
remove_delivery_tags(Seq, true, Unacked) ->
    case gb_trees:is_empty(Unacked) of
        true  -> Unacked;
        false -> {Smallest, _Val, Unacked1} = gb_trees:take_smallest(Unacked),
                 case Smallest > Seq of
                     true  -> Unacked;
                     false -> remove_delivery_tags(Seq, true, Unacked1)
                 end
    end.

report_status(Verb, State) ->
    rabbit_shovel_status:report(
      State#state.name, {Verb, {source, State#state.inbound_params},
                         {destination, State#state.outbound_params}}).

publish(Tag, Method, Msg,
        State = #state{inbound_ch = InboundChan, outbound_ch = OutboundChan,
                       config = Config, blocked = false, msg_buf = MsgBuf,
                       unacked = Unacked}) ->
    Seq = case Config#shovel.ack_mode of
              on_confirm  -> amqp_channel:next_publish_seqno(OutboundChan);
              _           -> undefined
          end,
    case amqp_channel:call(OutboundChan, Method, Msg) of
        ok ->
            case Config#shovel.ack_mode of
                no_ack ->
                    State;
                on_confirm ->
                    State#state{unacked = gb_trees:insert(Seq, Tag, Unacked)};
                on_publish ->
                    ok = amqp_channel:cast(
                           InboundChan, #'basic.ack'{delivery_tag = Tag}),
                    State
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
