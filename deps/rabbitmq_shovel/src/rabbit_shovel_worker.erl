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
%%  Copyright (c) 2007-2014 GoPivotal, Inc.  All rights reserved.
%%

-module(rabbit_shovel_worker).
-behaviour(gen_server2).

-export([start_link/3]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-include_lib("amqp_client/include/amqp_client.hrl").
-include("rabbit_shovel.hrl").

-define(MAX_CONNECTION_CLOSE_TIMEOUT, 10000).

-record(state, {inbound_conn, inbound_ch, outbound_conn, outbound_ch,
                name, type, config, inbound_uri, outbound_uri, unacked,
                remaining, %% [1]
                remaining_unacked}). %% [2]

%% [1] Counts down until we shut down in all modes
%% [2] Counts down until we stop publishing in on-confirm mode

start_link(Type, Name, Config) ->
    ok = rabbit_shovel_status:report(Name, Type, starting),
    gen_server2:start_link(?MODULE, [Type, Name, Config], []).

%%---------------------------
%% Gen Server Implementation
%%---------------------------

init([Type, Name, Config]) ->
    gen_server2:cast(self(), init),
    {ok, Shovel} = parse(Type, Name, Config),
    {ok, #state{name = Name, type = Type, config = Shovel}}.

parse(static,  Name, Config) -> rabbit_shovel_config:parse(Name, Config);
parse(dynamic, Name, Config) -> rabbit_shovel_parameters:parse(Name, Config).

handle_call(_Msg, _From, State) ->
    {noreply, State}.

handle_cast(init, State = #state{config = Config}) ->
    random:seed(now()),
    #shovel{sources = Sources, destinations = Destinations} = Config,
    {InboundConn, InboundChan, InboundURI} =
        make_conn_and_chan(Sources#endpoint.uris),
    {OutboundConn, OutboundChan, OutboundURI} =
        make_conn_and_chan(Destinations#endpoint.uris),

    %% Don't trap exits until we have established connections so that
    %% if we try to shut down while waiting for a connection to be
    %% established then we don't block
    process_flag(trap_exit, true),

    (Sources#endpoint.resource_declaration)(InboundConn, InboundChan),
    (Destinations#endpoint.resource_declaration)(OutboundConn, OutboundChan),

    NoAck = Config#shovel.ack_mode =:= no_ack,
    case NoAck of
        false -> Prefetch = Config#shovel.prefetch_count,
                 #'basic.qos_ok'{} =
                     amqp_channel:call(
                       InboundChan, #'basic.qos'{prefetch_count = Prefetch});
        true  -> ok
    end,

    case Config#shovel.ack_mode of
        on_confirm ->
            #'confirm.select_ok'{} =
                amqp_channel:call(OutboundChan, #'confirm.select'{}),
            ok = amqp_channel:register_confirm_handler(OutboundChan, self());
        _ ->
            ok
    end,

    Remaining = remaining(InboundChan, Config),
    case Remaining of
        0 -> exit({shutdown, autodelete});
        _ -> ok
    end,

    #'basic.consume_ok'{} =
        amqp_channel:subscribe(
          InboundChan, #'basic.consume'{queue  = Config#shovel.queue,
                                        no_ack = NoAck},
          self()),

    State1 =
        State#state{inbound_conn = InboundConn, inbound_ch = InboundChan,
                    outbound_conn = OutboundConn, outbound_ch = OutboundChan,
                    inbound_uri = InboundURI,
                    outbound_uri = OutboundURI,
                    remaining = Remaining,
                    remaining_unacked = Remaining,
                    unacked = gb_trees:empty()},
    ok = report_running(State1),
    {noreply, State1}.

handle_info(#'basic.consume_ok'{}, State) ->
    {noreply, State};

handle_info({#'basic.deliver'{delivery_tag = Tag,
                              exchange = Exchange, routing_key = RoutingKey},
             Msg = #amqp_msg{props = Props = #'P_basic'{}}},
            State = #state{inbound_uri  = InboundURI,
                           outbound_uri = OutboundURI,
                           config = #shovel{publish_properties = PropsFun,
                                            publish_fields     = FieldsFun}}) ->
    Method = #'basic.publish'{exchange = Exchange, routing_key = RoutingKey},
    Method1 = FieldsFun(InboundURI, OutboundURI, Method),
    Msg1 = Msg#amqp_msg{props = PropsFun(InboundURI, OutboundURI, Props)},
    {noreply, publish(Tag, Method1, Msg1, State)};

handle_info(#'basic.ack'{delivery_tag = Seq, multiple = Multiple},
            State = #state{config = #shovel{ack_mode = on_confirm}}) ->
    {noreply, confirm_to_inbound(
                fun (DTag, Multi) ->
                        #'basic.ack'{delivery_tag = DTag, multiple = Multi}
                end, Seq, Multiple, State)};

handle_info(#'basic.nack'{delivery_tag = Seq, multiple = Multiple},
            State = #state{config = #shovel{ack_mode = on_confirm}}) ->
    {noreply, confirm_to_inbound(
                fun (DTag, Multi) ->
                        #'basic.nack'{delivery_tag = DTag, multiple = Multi}
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
                         name = Name, type = Type}) ->
    rabbit_shovel_status:report(Name, Type, {terminated, Reason}),
    ok;
terminate({shutdown, autodelete}, State = #state{name = {VHost, Name},
                                                 type = dynamic}) ->
    close_connections(State),
    %% See rabbit_shovel_dyn_worker_sup_sup:stop_child/1
    put(shovel_worker_autodelete, true),
    rabbit_runtime_parameters:clear(VHost, <<"shovel">>, Name),
    rabbit_shovel_status:remove({VHost, Name}),
    ok;
terminate(Reason, State) ->
    close_connections(State),
    rabbit_shovel_status:report(State#state.name, State#state.type,
                                {terminated, Reason}),
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
    {Unacked1, Removed} = remove_delivery_tags(Seq, Multiple, Unacked, 0),
    decr_remaining(Removed, State#state{unacked = Unacked1}).

remove_delivery_tags(Seq, false, Unacked, 0) ->
    {gb_trees:delete(Seq, Unacked), 1};
remove_delivery_tags(Seq, true, Unacked, Count) ->
    case gb_trees:is_empty(Unacked) of
        true  -> {Unacked, Count};
        false -> {Smallest, _Val, Unacked1} = gb_trees:take_smallest(Unacked),
                 case Smallest > Seq of
                     true  -> {Unacked, Count};
                     false -> remove_delivery_tags(Seq, true, Unacked1, Count+1)
                 end
    end.

report_running(State) ->
    rabbit_shovel_status:report(
      State#state.name, State#state.type,
      {running, [{src_uri,  State#state.inbound_uri},
                 {dest_uri, State#state.outbound_uri}]}).

publish(_Tag, _Method, _Msg, State = #state{remaining_unacked = 0}) ->
    %% We are in on-confirm mode, and are autodelete. We have
    %% published all the messages we need to; we just wait for acks to
    %% come back. So drop subsequent messages on the floor to be
    %% requeued later.
    State;

publish(Tag, Method, Msg,
        State = #state{inbound_ch = InboundChan, outbound_ch = OutboundChan,
                       config = Config, unacked = Unacked}) ->
    Seq = case Config#shovel.ack_mode of
              on_confirm  -> amqp_channel:next_publish_seqno(OutboundChan);
              _           -> undefined
          end,
    ok = amqp_channel:call(OutboundChan, Method, Msg),
    decr_remaining_unacked(
      case Config#shovel.ack_mode of
          no_ack     -> decr_remaining(1, State);
          on_confirm -> State#state{unacked = gb_trees:insert(
                                                Seq, Tag, Unacked)};
          on_publish -> ok = amqp_channel:cast(
                               InboundChan, #'basic.ack'{delivery_tag = Tag}),
                        decr_remaining(1, State)
      end).

make_conn_and_chan(URIs) ->
    URI = lists:nth(random:uniform(length(URIs)), URIs),
    {ok, AmqpParam} = amqp_uri:parse(URI),
    {ok, Conn} = amqp_connection:start(AmqpParam),
    link(Conn),
    {ok, Chan} = amqp_connection:open_channel(Conn),
    {Conn, Chan, list_to_binary(amqp_uri:remove_credentials(URI))}.

remaining(_Ch, #shovel{delete_after = never}) ->
    unlimited;
remaining(Ch, #shovel{delete_after = 'queue-length', queue = Queue}) ->
    #'queue.declare_ok'{message_count = N} =
        amqp_channel:call(Ch, #'queue.declare'{queue   = Queue,
                                               passive = true}),
    N;
remaining(_Ch, #shovel{delete_after = Count}) ->
    Count.

decr_remaining(_N, State = #state{remaining = unlimited}) ->
    State;
decr_remaining(N,  State = #state{remaining = M}) ->
    case M > N of
        true  -> State#state{remaining = M - N};
        false -> exit({shutdown, autodelete})
    end.

decr_remaining_unacked(State = #state{remaining_unacked = unlimited}) ->
    State;
decr_remaining_unacked(State = #state{remaining_unacked = 0}) ->
    State;
decr_remaining_unacked(State = #state{remaining_unacked = N}) ->
    State#state{remaining_unacked = N - 1}.

close_connections(State) ->
    catch amqp_connection:close(State#state.inbound_conn,
                                ?MAX_CONNECTION_CLOSE_TIMEOUT),
    catch amqp_connection:close(State#state.outbound_conn,
                                ?MAX_CONNECTION_CLOSE_TIMEOUT).
