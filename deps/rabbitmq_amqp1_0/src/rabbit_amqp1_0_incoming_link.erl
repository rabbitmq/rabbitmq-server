%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_amqp1_0_incoming_link).

-export([attach/3, transfer/4, ack/2]).

-include_lib("amqp_client/include/amqp_client.hrl").
-include("rabbit_amqp1_0.hrl").

-import(rabbit_amqp1_0_util, [protocol_error/3]).

-define(INCOMING_CREDIT, application:get_env(rabbitmq_amqp1_0, maximum_incoming_credit, 65536)).
-define(INCOMING_CREDIT_MODE, application:get_env(rabbitmq_amqp1_0, grant_credit, on_confirm)).

-record(incoming_link, {name, exchange, routing_key,
                        delivery_id = undefined,
                        delivery_count = 0,
                        send_settle_mode = undefined,
                        recv_settle_mode = undefined,
                        credit_remaining = 0,
                        msg_acc = [],
                        route_state,
                        in_confirm_mode = false
                    }).

attach(#'v1_0.attach'{name = Name,
                      handle = Handle,
                      source = Source,
                      snd_settle_mode = SndSettleMode,
                      rcv_settle_mode = RcvSettleMode,
                      target = Target,
                      initial_delivery_count = {uint, InitTransfer}},
       BCh, DCh) ->
    %% TODO associate link name with target
    InitialCreditRemaining = max_incoming_credit() div 2,
    case ensure_target(Target,
                       #incoming_link{
                         name        = Name,
                         route_state = rabbit_routing_util:init_state(),
                         delivery_count = InitTransfer,
                         credit_remaining = InitialCreditRemaining 
                        },
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
                                 link_credit = {uint, max_incoming_credit()},
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
                IncomingLink#incoming_link{
                    recv_settle_mode = RcvSettleMode,
                    in_confirm_mode = Confirm
                },
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
                        credit_remaining = CreditRemaining,
                        msg_acc          = MsgAcc,
                        send_settle_mode = SSM,
                        recv_settle_mode = RSM,
                        in_confirm_mode = InConfirmMode
                    } = Link, BCh) ->
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

    MessageSettled = effective_send_settle_mode(Settled, SSM),
    EffectiveRecvSettleMode = effective_recv_settle_mode(RcvSettleMode, RSM),
    case not MessageSettled andalso
            EffectiveRecvSettleMode =:= ?V_1_0_RECEIVER_SETTLE_MODE_SECOND of
        false -> ok;
        true  -> protocol_error(?V_1_0_AMQP_ERROR_NOT_IMPLEMENTED,
                                "rcv-settle-mode second not supported", [])
    end,
    case MessageSettled of 
        true -> ok;
        false -> 
            case InConfirmMode of 
                true -> ok;
                false -> 
                    protocol_error(?V_1_0_AMQP_ERROR_NOT_IMPLEMENTED,
                                "Publishing an unsettled message on a link set as 'settled' is not supported ", [])
            end
    end,


    {SendFlow, CreditRemaining1} = adjust_credits(transfer, CreditRemaining - 1, MessageSettled),
    #incoming_link{delivery_id = DeliveryId} =
      set_delivery_id(DeliveryId0, Link),
    
    NewLink = Link#incoming_link{
                delivery_id      = undefined,
                send_settle_mode = undefined,
                delivery_count   = rabbit_amqp1_0_util:serial_add(Count, 1),
                credit_remaining      = CreditRemaining1,
                msg_acc          = []
            },
    Reply = case SendFlow of
                true  -> ?DEBUG("sending flow for incoming ~p", [NewLink]),
                         [incoming_flow(NewLink, Handle)];
                false -> []
            end,
    {message, Reply, NewLink, DeliveryId, MessageSettled}.


ack(Handle, #incoming_link{
        credit_remaining = CreditRemaining
    } = Link) ->
    {SendFlow, CreditRemaining1} = adjust_credits(ack, CreditRemaining, true),
    Link2 = Link#incoming_link{
        credit_remaining = CreditRemaining1
    },
    Replies = case SendFlow of 
        true -> [incoming_flow(Link2, Handle)];
        false -> []
    end,
    {ok, Replies, Link2}.

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
                     {nowait, false},
                     {arguments, rabbit_amqp1_0_link_util:queue_declare_arguments()}],
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
                 link_credit    = {uint, max_incoming_credit()}}.

max_incoming_credit() -> 
    ?INCOMING_CREDIT.


-spec adjust_credits(Method :: ack | transfer, 
                     CreditRemaining :: integer(), 
                     MsgSettled :: boolean()) -> 
    {boolean(), integer()}.
% there are credits remaining
adjust_credits(_Method, CreditRemaining, _MsgSettled) when CreditRemaining > 0 -> 
    {false, CreditRemaining};
% if message was settled on transfer, we give more credits on transfer
adjust_credits(transfer, CreditRemaining, true) when CreditRemaining =< 0 -> 
    {true, max_incoming_credit() div 2};
% we received an ack and have no credit
adjust_credits(ack, CreditRemaining,_) when CreditRemaining =< 0 -> 
    {true, max_incoming_credit() div 2};
% message is not settled, depending on configuration we either grant or not
adjust_credits(Method, CreditRemaining, false) when CreditRemaining =< 0 -> 
    GrantCredit = ?INCOMING_CREDIT_MODE,
    case GrantCredit of 
        on_publish -> 
            {true, max_incoming_credit() div 2};
        on_confirm -> 
            case Method of
                ack -> 
                    {true, max_incoming_credit() div 2};
                transfer ->
                    {false, CreditRemaining}
            end
    end.
            
