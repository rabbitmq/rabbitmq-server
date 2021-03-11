%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_amqp1_0_incoming_link).

-export([attach/3, transfer/4]).

-include_lib("amqp_client/include/amqp_client.hrl").
-include("rabbit_amqp1_0.hrl").

-import(rabbit_amqp1_0_util, [protocol_error/3]).

%% Just make these constant for the time being.
-define(INCOMING_CREDIT, 65536).

-record(incoming_link, {name, exchange, routing_key,
                        delivery_id = undefined,
                        delivery_count = 0,
                        send_settle_mode = undefined,
                        recv_settle_mode = undefined,
                        credit_used = ?INCOMING_CREDIT div 2,
                        msg_acc = [],
                        route_state}).

attach(#'v1_0.attach'{name = Name,
                      handle = Handle,
                      source = Source,
                      snd_settle_mode = SndSettleMode,
                      rcv_settle_mode = RcvSettleMode,
                      target = Target,
                      initial_delivery_count = {uint, InitTransfer}},
       BCh, DCh) ->
    %% TODO associate link name with target
    case ensure_target(Target,
                       #incoming_link{
                         name        = Name,
                         route_state = rabbit_routing_util:init_state(),
                         delivery_count = InitTransfer },
                       DCh) of
        {ok, ServerTarget, IncomingLink} ->
            {_, _Outcomes} = rabbit_amqp1_0_link_util:outcomes(Source),
            %% Default is mixed
            Confirm =
                case SndSettleMode of
                    ?V_1_0_SENDER_SETTLE_MODE_SETTLED ->
                        false;
                    _ when SndSettleMode == undefined;
                           SndSettleMode == ?V_1_0_SENDER_SETTLE_MODE_UNSETTLED;
                           SndSettleMode == ?V_1_0_SENDER_SETTLE_MODE_MIXED ->
                        amqp_channel:register_confirm_handler(BCh, self()),
                        rabbit_amqp1_0_channel:call(BCh, #'confirm.select'{}),
                        true
                end,
            Flow = #'v1_0.flow'{ handle = Handle,
                                 link_credit = {uint, ?INCOMING_CREDIT},
                                 drain = false,
                                 echo = false },
            Attach = #'v1_0.attach'{
              name = Name,
              handle = Handle,
              source = Source,
              snd_settle_mode = SndSettleMode,
              rcv_settle_mode = RcvSettleMode,
              target = ServerTarget,
              initial_delivery_count = undefined, % must be, I am the receiver
              role = ?RECV_ROLE}, %% server is receiver
            IncomingLink1 =
                IncomingLink#incoming_link{recv_settle_mode = RcvSettleMode},
            {ok, [Attach, Flow], IncomingLink1, Confirm};
        {error, Reason} ->
            %% TODO proper link establishment protocol here?
            protocol_error(?V_1_0_AMQP_ERROR_INVALID_FIELD,
                               "Attach rejected: ~p", [Reason])
    end.

set_delivery_id({uint, D},
                #incoming_link{delivery_id = undefined} = Link) ->
    Link#incoming_link{delivery_id = D};
set_delivery_id(DeliveryId,
                #incoming_link{delivery_id = D} = Link)
  when DeliveryId == {uint, D} orelse DeliveryId == undefined ->
    Link.

effective_send_settle_mode(undefined, undefined) ->
    false;
effective_send_settle_mode(undefined, SettleMode)
  when is_boolean(SettleMode) ->
    SettleMode;
effective_send_settle_mode(SettleMode, undefined)
  when is_boolean(SettleMode) ->
    SettleMode;
effective_send_settle_mode(SettleMode, SettleMode)
  when is_boolean(SettleMode) ->
    SettleMode.

effective_recv_settle_mode(undefined, undefined) ->
    ?V_1_0_RECEIVER_SETTLE_MODE_FIRST;
effective_recv_settle_mode(undefined, Mode) ->
    Mode;
effective_recv_settle_mode(Mode, _) ->
    Mode.

% TODO: validate effective settle modes against
%       those declared during attach

% TODO: handle aborted transfers

transfer(#'v1_0.transfer'{delivery_id = DeliveryId,
                          more        = true,
                          settled     = Settled}, MsgPart,
         #incoming_link{msg_acc = MsgAcc,
                        send_settle_mode = SSM} = Link, _BCh) ->
    {ok, set_delivery_id(
           DeliveryId,
           Link#incoming_link{msg_acc = [MsgPart | MsgAcc],
                              send_settle_mode =
                                effective_send_settle_mode(Settled, SSM)})};
transfer(#'v1_0.transfer'{delivery_id     = DeliveryId0,
                          settled         = Settled,
                          rcv_settle_mode = RcvSettleMode,
                          handle          = Handle},
         MsgPart,
         #incoming_link{exchange         = X,
                        routing_key      = LinkRKey,
                        delivery_count   = Count,
                        credit_used      = CreditUsed,
                        msg_acc          = MsgAcc,
                        send_settle_mode = SSM,
                        recv_settle_mode = RSM} = Link, BCh) ->
    MsgBin = iolist_to_binary(lists:reverse([MsgPart | MsgAcc])),
    ?DEBUG("Inbound content:~n  ~p",
           [[amqp10_framing:pprint(Section) ||
                Section <- amqp10_framing:decode_bin(MsgBin)]]),
    {MsgRKey, Msg} = rabbit_amqp1_0_message:assemble(MsgBin),
    RKey = case LinkRKey of
               undefined -> MsgRKey;
               _         -> LinkRKey
           end,
    rabbit_amqp1_0_channel:cast_flow(
      BCh, #'basic.publish'{exchange    = X,
                            routing_key = RKey}, Msg),
    {SendFlow, CreditUsed1} = case CreditUsed - 1 of
                                  C when C =< 0 ->
                                      {true,  ?INCOMING_CREDIT div 2};
                                  D ->
                                      {false, D}
                              end,
    #incoming_link{delivery_id = DeliveryId} =
      set_delivery_id(DeliveryId0, Link),
    NewLink = Link#incoming_link{
                delivery_id      = undefined,
                send_settle_mode = undefined,
                delivery_count   = rabbit_amqp1_0_util:serial_add(Count, 1),
                credit_used      = CreditUsed1,
                msg_acc          = []},
    Reply = case SendFlow of
                true  -> ?DEBUG("sending flow for incoming ~p", [NewLink]),
                         [incoming_flow(NewLink, Handle)];
                false -> []
            end,
    EffectiveSendSettleMode = effective_send_settle_mode(Settled, SSM),
    EffectiveRecvSettleMode = effective_recv_settle_mode(RcvSettleMode, RSM),
    case not EffectiveSendSettleMode andalso
         EffectiveRecvSettleMode =:= ?V_1_0_RECEIVER_SETTLE_MODE_SECOND of
        false -> ok;
        true  -> protocol_error(?V_1_0_AMQP_ERROR_NOT_IMPLEMENTED,
                                "rcv-settle-mode second not supported", [])
    end,
    {message, Reply, NewLink, DeliveryId,
     EffectiveSendSettleMode}.

%% TODO default-outcome and outcomes, dynamic lifetimes

ensure_target(Target = #'v1_0.target'{address       = Address,
                                      dynamic       = Dynamic,
                                      durable       = Durable,
                                      %% TODO
                                      expiry_policy = _ExpiryPolicy,
                                      %% TODO
                                      timeout       = _Timeout},
              Link = #incoming_link{ route_state = RouteState }, DCh) ->
    DeclareParams = [{durable, rabbit_amqp1_0_link_util:durable(Durable)},
                     {exclusive, false},
                     {auto_delete, false},
                     {check_exchange, true},
                     {nowait, false}],
    case Dynamic of
        true ->
            protocol_error(?V_1_0_AMQP_ERROR_NOT_IMPLEMENTED,
                           "Dynamic targets not supported", []);
        _ ->
            ok
    end,
    case Address of
        {utf8, Destination} ->
            case rabbit_routing_util:parse_endpoint(Destination, true) of
                {ok, Dest} ->
                    {ok, _Queue, RouteState1} =
                        rabbit_amqp1_0_channel:convert_error(
                          fun () ->
                                  rabbit_routing_util:ensure_endpoint(
                                    dest, DCh, Dest, DeclareParams,
                                    RouteState)
                          end),
                    {XName, RK} = rabbit_routing_util:parse_routing(Dest),
                    {ok, Target, Link#incoming_link{
                                   route_state = RouteState1,
                                   exchange    = list_to_binary(XName),
                                   routing_key = case RK of
                                                     undefined -> undefined;
                                                     []        -> undefined;
                                                     _         -> list_to_binary(RK)
                                                 end}};
                {error, _} = E ->
                    E
            end;
        _Else ->
            {error, {address_not_utf8_string, Address}}
    end.

incoming_flow(#incoming_link{ delivery_count = Count }, Handle) ->
    #'v1_0.flow'{handle         = Handle,
                 delivery_count = {uint, Count},
                 link_credit    = {uint, ?INCOMING_CREDIT}}.
