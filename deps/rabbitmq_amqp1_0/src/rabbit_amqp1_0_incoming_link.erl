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
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2007-2013 VMware, Inc.  All rights reserved.
%%

-module(rabbit_amqp1_0_incoming_link).

-export([attach/3, transfer/4]).

-include_lib("amqp_client/include/amqp_client.hrl").
-include("rabbit_amqp1_0.hrl").

-import(rabbit_amqp1_0_link_util, [protocol_error/3]).

%% Just make these constant for the time being.
-define(INCOMING_CREDIT, 65536).

-record(incoming_link, {name, exchange, routing_key,
                        delivery_id = undefined,
                        delivery_count = 0,
                        send_settle_mode = undefined,
                        recv_settle_mode = undefined,
                        credit_used = ?INCOMING_CREDIT div 2,
                        msg_acc = []}).

attach(#'v1_0.attach'{name = Name,
                      handle = Handle,
                      source = Source,
                      snd_settle_mode = SndSettleMode,
                      rcv_settle_mode = RcvSettleMode,
                      target = Target,
                      initial_delivery_count = {uint, InitTransfer}},
       BCh, DCh) ->
    %% TODO associate link name with target
    case ensure_target(Target, #incoming_link{ name = Name }, DCh) of
        {ok, ServerTarget,
         IncomingLink = #incoming_link{ delivery_count = InitTransfer }} ->
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
            rabbit_log:warning("AMQP 1.0 attach rejected ~p~n", [Reason]),
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
    ?DEBUG("Inbound content:~n  ~p~n",
           [[rabbit_amqp1_0_framing:pprint(Section) ||
                Section <- rabbit_amqp1_0_framing:decode_bin(MsgBin)]]),
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
                delivery_count   = rabbit_misc:serial_add(Count, 1),
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

%% There are a few things that influence what source and target
%% definitions mean for our purposes.
%%
%% Addresses: we artificially segregate exchanges and queues, since
%% they have different namespaces. However, we allow both incoming and
%% outgoing links to exchanges: outgoing links from an exchange
%% involve an anonymous queue.
%%
%% For targets, addresses are
%% Address = "/exchange/" Name "/" RoutingKey
%%         | "/exchange/" Name
%%         | "/queue"
%%         | "/queue/" Name
%%
%% For sources, addresses are
%% Address = "/exchange/" Name "/" RoutingKey
%%         | "/queue/" Name
%%
%% We use the message property "Subject" as the equivalent of the
%% routing key.  In AMQP 0-9-1 terms, a target of /queue is equivalent
%% to the default exchange; that is, the message is routed to the
%% queue named by the subject.  A target of "/queue/Name" ignores the
%% subject.  The reason for both varieties is that a
%% dynamically-created queue must be fully addressable as a target,
%% while a service may wish to use /queue and route each message to
%% its reply-to queue name (as it is done in 0-9-1).
%%
%% A dynamic source or target only ever creates a queue, and the
%% address is returned in full; e.g., "/queue/amq.gen.123456".
%% However, that cannot be used as a reply-to, since a 0-9-1 client
%% will use it unaltered as the routing key naming the queue.
%% Therefore, we rewrite reply-to from 1.0 clients to be just the
%% queue name, and expect replying clients to use /queue and the
%% subject field.
%%
%% For a source queue, the distribution-mode is always move.  For a
%% source exchange, it is always copy. Anything else should be
%% refused.
%%
%% TODO default-outcome and outcomes, dynamic lifetimes

%% TODO this looks to have a lot in common with ensure_source
ensure_target(Target = #'v1_0.target'{address       = Address,
                                      dynamic       = Dynamic,
                                      durable       = Durable,
                                      %% TODO expiry_policy = ExpiryPolicy,
                                      timeout       = Timeout},
              Link = #incoming_link{}, DCh) ->
    case Dynamic of
        true ->
            case Address of
                undefined ->
                    {ok, QueueName} = rabbit_amqp1_0_link_util:create_queue(Timeout, DCh, Durable),
                    {ok,
                     Target#'v1_0.target'{address = {utf8, rabbit_amqp1_0_link_util:queue_address(QueueName)}},
                     Link#incoming_link{exchange = <<"">>,
                                        routing_key = QueueName}};
                _Else ->
                    {error, {both_dynamic_and_address_supplied,
                             Dynamic, Address}}
            end;
        _ ->
            case Address of
                {Enc, Destination}
                  when Enc =:= utf8 ->
                    case rabbit_amqp1_0_link_util:parse_destination(Destination, Enc) of
                        ["queue", Name] ->
                            case rabbit_amqp1_0_link_util:create_queue(Name, Timeout, DCh, Durable) of
                                {ok, QueueName} ->
                                    {ok, Target,
                                     Link#incoming_link{exchange = <<"">>,
                                                        routing_key = QueueName}};
                                _ ->
                                    {error, "Declaring queue " ++ Name ++ " failed."}
                            end;
                        ["queue"] ->
                            %% Rely on the Subject being set
                            {ok, Target, Link#incoming_link{exchange = <<"">>}};
                        ["exchange", Name] ->
                            case rabbit_amqp1_0_link_util:check_exchange(Name, DCh) of
                                {ok, ExchangeName} ->
                                    {ok, Target,
                                     Link#incoming_link{exchange = ExchangeName}};
                                {error, Reason} ->
                                    {error, Reason}
                            end;
                        ["exchange", Name, RKey] ->
                            case rabbit_amqp1_0_link_util:check_exchange(Name, DCh) of
                                {ok, ExchangeName} ->
                                    {ok, Target,
                                     Link#incoming_link{exchange = ExchangeName,
                                                        routing_key = list_to_binary(RKey)}};
                                {error, Reason} ->
                                    {error, Reason}
                            end;
                        _Otherwise ->
                            {error, {unknown_address, Address}}
                    end;
                _Else ->
                    {error, {unknown_address, Address}}
            end
    end.

incoming_flow(#incoming_link{ delivery_count = Count }, Handle) ->
    #'v1_0.flow'{handle         = Handle,
                 delivery_count = {uint, Count},
                 link_credit    = {uint, ?INCOMING_CREDIT}}.
