%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_amqp1_0_outgoing_link).

-export([attach/3, detach/3, delivery/6, transferred/3, credit_drained/3, flow/3]).

-include_lib("amqp_client/include/amqp_client.hrl").
-include("rabbit_amqp1_0.hrl").

-import(rabbit_amqp1_0_util, [protocol_error/3, serial_add/2]).
-import(rabbit_amqp1_0_link_util, [handle_to_ctag/1]).

-define(INIT_TXFR_COUNT, 0).
-define(DEFAULT_SEND_SETTLED, false).

-record(outgoing_link, {queue,
                        delivery_count = 0,
                        send_settled,
                        default_outcome,
                        route_state}).

detach(#'v1_0.detach'{handle = Handle}, BCh,_Link) ->
    CTag = handle_to_ctag(Handle),
    rabbit_amqp1_0_channel:call(BCh, #'basic.cancel'{consumer_tag = CTag}),
    ok.

attach(#'v1_0.attach'{name = Name,
                      handle = Handle,
                      source = Source,
                      snd_settle_mode = SndSettleMode,
                      rcv_settle_mode = RcvSettleMode}, BCh, DCh) ->
    {DefaultOutcome, Outcomes} = rabbit_amqp1_0_link_util:outcomes(Source),
    SndSettled =
        case SndSettleMode of
            ?V_1_0_SENDER_SETTLE_MODE_SETTLED   -> true;
            ?V_1_0_SENDER_SETTLE_MODE_UNSETTLED -> false;
            _                                   -> ?DEFAULT_SEND_SETTLED
        end,
    DOSym = amqp10_framing:symbol_for(DefaultOutcome),
    case ensure_source(Source,
                       #outgoing_link{delivery_count  = ?INIT_TXFR_COUNT,
                                      send_settled    = SndSettled,
                                      default_outcome = DOSym,
                                      route_state     =
                                        rabbit_routing_util:init_state()},
                       DCh) of
        {ok, Source1, OutgoingLink = #outgoing_link{queue = QueueName}} ->
            CTag = handle_to_ctag(Handle),
            Args = source_filters_to_consumer_args(Source1),

            case rabbit_amqp1_0_channel:subscribe(
                   BCh, #'basic.consume'{
                     queue = QueueName,
                     consumer_tag = CTag,
                     %% we will ack when we've transferred
                     %% a message, or when we get an ack
                     %% from the client.
                     no_ack = false,
                     %% TODO exclusive?
                     exclusive = false,
                     arguments = Args ++
                                  [{<<"x-credit">>, table,
                                   [{<<"credit">>, long,    0},
                                    {<<"drain">>,  bool, false}]}]},
                   self()) of
                #'basic.consume_ok'{} ->
                    %% TODO we should avoid the race by getting the queue to send
                    %% attach back, but a.t.m. it would use the wrong codec.
                    {ok, [#'v1_0.attach'{
                       name = Name,
                       handle = Handle,
                       initial_delivery_count = {uint, ?INIT_TXFR_COUNT},
                       snd_settle_mode =
                           case SndSettled of
                               true  -> ?V_1_0_SENDER_SETTLE_MODE_SETTLED;
                               false -> ?V_1_0_SENDER_SETTLE_MODE_UNSETTLED
                           end,
                       rcv_settle_mode = RcvSettleMode,
                       source = Source1#'v1_0.source'{
                                  default_outcome = DefaultOutcome,
                                  outcomes        = Outcomes
                                 },
                       role = ?SEND_ROLE}], OutgoingLink};
                Fail ->
                    protocol_error(?V_1_0_AMQP_ERROR_INTERNAL_ERROR,
                                   "Consume failed: ~p", [Fail])
            end;
        {error, Reason} ->
            %% TODO proper link establishment protocol here?
            protocol_error(?V_1_0_AMQP_ERROR_INVALID_FIELD,
                               "Attach rejected: ~p", [Reason])
    end.

credit_drained(#'basic.credit_drained'{credit_drained = CreditDrained},
               Handle, Link = #outgoing_link{delivery_count = Count0}) ->
    Count = Count0 + CreditDrained,
    %% The transfer count that is given by the queue should be at
    %% least that we have locally, since we will either have received
    %% all the deliveries and transferred them, or the queue will have
    %% advanced it due to drain. So we adopt the queue's idea of the
    %% count.
    %% TODO account for it not being there any more
    F = #'v1_0.flow'{ handle      = Handle,
                      delivery_count = {uint, Count},
                      link_credit = {uint, 0},
                      available   = {uint, 0},
                      drain       = true },
    {F, Link#outgoing_link{delivery_count = Count}}.

flow(#outgoing_link{delivery_count = LocalCount},
     #'v1_0.flow'{handle         = Handle,
                  delivery_count = Count0,
                  link_credit    = {uint, RemoteCredit},
                  drain          = Drain0}, BCh) ->
    {uint, RemoteCount} = default(Count0, {uint, LocalCount}),
    Drain = default(Drain0, false),
    %% See section 2.6.7
    LocalCredit = RemoteCount + RemoteCredit - LocalCount,
    CTag = handle_to_ctag(Handle),
    #'basic.credit_ok'{available = Available} =
        rabbit_amqp1_0_channel:call(
          BCh, #'basic.credit'{consumer_tag = CTag,
                               credit       = LocalCredit,
                               drain        = Drain}),
    case Available of
        -1 ->
            {ok, []};
        %% We don't know - probably because this flow relates
        %% to a handle that does not yet exist
        %% TODO is this an error?
        _  ->
            {ok, [#'v1_0.flow'{
                    handle         = Handle,
                    delivery_count = {uint, LocalCount},
                    link_credit    = {uint, LocalCredit},
                    available      = {uint, Available},
                    drain          = Drain}]}
    end.

default(undefined, Default) -> Default;
default(Thing,    _Default) -> Thing.

ensure_source(Source = #'v1_0.source'{address       = Address,
                                      dynamic       = Dynamic,
                                      durable       = Durable,
                                      filter = _Filters,
                                      %% TODO
                                      expiry_policy = _ExpiryPolicy,
                                      %% TODO
                                      timeout       = _Timeout},
              Link = #outgoing_link{ route_state = RouteState }, DCh) ->
    DeclareParams = [{durable, rabbit_amqp1_0_link_util:durable(Durable)},
                     {exclusive, false},
                     {auto_delete, false},
                     {check_exchange, true},
                     {nowait, false}],
    case Dynamic of
        true -> protocol_error(?V_1_0_AMQP_ERROR_NOT_IMPLEMENTED,
                               "Dynamic sources not supported", []);
        _    -> ok
    end,
    case Address of
        {utf8, Destination} ->
            case rabbit_routing_util:parse_endpoint(Destination, false) of
                {ok, Dest} ->
                    {ok, Queue, RouteState1} =
                        rabbit_amqp1_0_channel:convert_error(
                          fun() ->
                                  rabbit_routing_util:ensure_endpoint(
                                    source, DCh, Dest, DeclareParams,
                                    RouteState)
                          end),
                    ER = rabbit_routing_util:parse_routing(Dest),
                    ok = rabbit_routing_util:ensure_binding(Queue, ER, DCh),
                    {ok, Source, Link#outgoing_link{route_state = RouteState1,
                                                    queue       = Queue}};
                {error, _} = E ->
                    E
            end;
        _ ->
            {error, {address_not_utf8_string, Address}}
    end.

delivery(Deliver = #'basic.deliver'{delivery_tag = DeliveryTag,
                                    routing_key  = RKey},
                Msg, FrameMax, Handle, Session,
                #outgoing_link{send_settled = SendSettled,
                               default_outcome = DefaultOutcome}) ->
    DeliveryId = rabbit_amqp1_0_session:next_delivery_id(Session),
    Session1 = rabbit_amqp1_0_session:record_outgoing(
                 DeliveryTag, SendSettled, DefaultOutcome, Session),
    Txfr = #'v1_0.transfer'{handle = Handle,
                            delivery_tag = {binary, <<DeliveryTag:64>>},
                            delivery_id = {uint, DeliveryId},
                            %% The only one in AMQP 1-0
                            message_format = {uint, 0},
                            settled = SendSettled,
                            resume = false,
                            more = false,
                            aborted = false,
                            %% TODO: actually batchable would be fine,
                            %% but in any case it's only a hint
                            batchable = false},
    Msg1_0 = rabbit_amqp1_0_message:annotated_message(
               RKey, Deliver, Msg),
    ?DEBUG("Outbound content:~n  ~p",
           [[amqp10_framing:pprint(Section) ||
                Section <- amqp10_framing:decode_bin(
                             iolist_to_binary(Msg1_0))]]),
    %% TODO Ugh
    TLen = iolist_size(amqp10_framing:encode_bin(Txfr)),
    Frames = case FrameMax of
                 unlimited ->
                     [[Txfr, Msg1_0]];
                 _ ->
                     encode_frames(Txfr, Msg1_0, FrameMax - TLen, [])
             end,
    {ok, Frames, Session1}.

encode_frames(_T, _Msg, MaxContentLen, _Transfers) when MaxContentLen =< 0 ->
    protocol_error(?V_1_0_AMQP_ERROR_FRAME_SIZE_TOO_SMALL,
                   "Frame size is too small by ~p bytes", [-MaxContentLen]);
encode_frames(T, Msg, MaxContentLen, Transfers) ->
    case iolist_size(Msg) > MaxContentLen of
        true  ->
            <<Chunk:MaxContentLen/binary, Rest/binary>> =
                iolist_to_binary(Msg),
            T1 = T#'v1_0.transfer'{more = true},
            encode_frames(T, Rest, MaxContentLen,
                          [[T1, Chunk] | Transfers]);
        false ->
            lists:reverse([[T, Msg] | Transfers])
    end.

transferred(DeliveryTag, Channel,
            Link = #outgoing_link{ delivery_count = Count,
                                   send_settled   = SendSettled }) ->
    if SendSettled ->
            rabbit_amqp1_0_channel:cast(
              Channel, #'basic.ack'{ delivery_tag = DeliveryTag });
       true ->
            ok
    end,
    Link#outgoing_link{delivery_count = serial_add(Count, 1)}.

source_filters_to_consumer_args(#'v1_0.source'{filter = {map, KVList}}) ->
    Key = {symbol, <<"rabbitmq:stream-offset-spec">>},
    case lists:keyfind(Key, 1, KVList) of
        {_, {timestamp, Ts}} ->
            [{<<"x-stream-offset">>, timestamp, Ts div 1000}]; %% 0.9.1 uses second based timestamps
        {_, {utf8, Spec}} ->
            [{<<"x-stream-offset">>, longstr, Spec}]; %% next, last, first and "10m" etc
        {_, {_, Offset}} when is_integer(Offset) ->
            [{<<"x-stream-offset">>, long, Offset}]; %% integer offset
        _ ->
            []
    end;
source_filters_to_consumer_args(_Source) ->
    [].
