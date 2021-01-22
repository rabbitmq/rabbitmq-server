%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%
-module(rabbit_ha_test_producer).

-export([await_response/1, start/6, create/5, create/6]).

-include_lib("amqp_client/include/amqp_client.hrl").

await_response(ProducerPid) ->
    error_logger:info_msg("waiting for producer pid ~p~n", [ProducerPid]),
    case receive {ProducerPid, Response} -> Response end of
        ok                -> ok;
        {error, _} = Else -> exit(Else);
        Else              -> exit({weird_response, Else})
    end.

create(Channel, Queue, TestPid, Confirm, MsgsToSend) ->
    create(Channel, Queue, TestPid, Confirm, MsgsToSend, acks).

create(Channel, Queue, TestPid, Confirm, MsgsToSend, Mode) ->
    AckNackMsgs = case Mode of
        acks  -> {ok, {error, received_nacks}};
        nacks -> {{error, received_acks}, ok}
    end,
    ProducerPid = spawn_link(?MODULE, start, [Channel, Queue, TestPid,
                                              Confirm, MsgsToSend, AckNackMsgs]),
    receive
        {ProducerPid, started} -> ProducerPid
    end.

start(Channel, Queue, TestPid, Confirm, MsgsToSend, AckNackMsgs) ->
    ConfirmState =
        case Confirm of
            true  -> amqp_channel:register_confirm_handler(Channel, self()),
                     #'confirm.select_ok'{} =
                         amqp_channel:call(Channel, #'confirm.select'{}),
                     gb_trees:empty();
            false -> none
        end,
    TestPid ! {self(), started},
    error_logger:info_msg("publishing ~w msgs on ~p~n", [MsgsToSend, Channel]),
    producer(Channel, Queue, TestPid, ConfirmState, MsgsToSend, AckNackMsgs).

%%
%% Private API
%%

producer(_Channel, _Queue, TestPid, none, 0, _AckNackMsgs) ->
    TestPid ! {self(), ok};
producer(Channel, _Queue, TestPid, ConfirmState, 0, {AckMsg, NackMsg}) ->
    error_logger:info_msg("awaiting confirms on channel ~p~n", [Channel]),
    Msg = case drain_confirms(none, ConfirmState) of
              %% No acks or nacks
              acks        -> AckMsg;
              nacks       -> NackMsg;
              mix         -> {error, received_both_acks_and_nacks};
              {Nacks, CS} -> {error, {missing_confirms, Nacks,
                                      lists:sort(gb_trees:keys(CS))}}
          end,
    TestPid ! {self(), Msg};

producer(Channel, Queue, TestPid, ConfirmState, MsgsToSend, AckNackMsgs) ->
    Method = #'basic.publish'{exchange    = <<"">>,
                              routing_key = Queue,
                              mandatory   = false,
                              immediate   = false},

    ConfirmState1 = maybe_record_confirm(ConfirmState, Channel, MsgsToSend),

    amqp_channel:call(Channel, Method,
                      #amqp_msg{props = #'P_basic'{delivery_mode = 2},
                                payload = list_to_binary(
                                            integer_to_list(MsgsToSend))}),

    producer(Channel, Queue, TestPid, ConfirmState1, MsgsToSend - 1, AckNackMsgs).

maybe_record_confirm(none, _, _) ->
    none;
maybe_record_confirm(ConfirmState, Channel, MsgsToSend) ->
    SeqNo = amqp_channel:next_publish_seqno(Channel),
    gb_trees:insert(SeqNo, MsgsToSend, ConfirmState).

drain_confirms(Collected, ConfirmState) ->
    case gb_trees:is_empty(ConfirmState) of
        true  -> Collected;
        false -> receive
                     #'basic.ack'{delivery_tag = DeliveryTag,
                                  multiple     = IsMulti} ->
                         Collected1 = case Collected of
                            none  -> acks;
                            acks  -> acks;
                            nacks -> mix;
                            mix   -> mix
                         end,
                         drain_confirms(Collected1,
                                        delete_confirms(DeliveryTag, IsMulti,
                                                        ConfirmState));
                     #'basic.nack'{delivery_tag = DeliveryTag,
                                   multiple     = IsMulti} ->
                         Collected1 = case Collected of
                            none  -> nacks;
                            nacks -> nacks;
                            acks  -> mix;
                            mix   -> mix
                         end,
                         drain_confirms(Collected1,
                                        delete_confirms(DeliveryTag, IsMulti,
                                                        ConfirmState))
                 after
                     60000 -> {Collected, ConfirmState}
                 end
    end.

delete_confirms(DeliveryTag, false, ConfirmState) ->
    gb_trees:delete(DeliveryTag, ConfirmState);
delete_confirms(DeliveryTag, true, ConfirmState) ->
    multi_confirm(DeliveryTag, ConfirmState).

multi_confirm(DeliveryTag, ConfirmState) ->
    case gb_trees:is_empty(ConfirmState) of
        true  -> ConfirmState;
        false -> {Key, _, ConfirmState1} = gb_trees:take_smallest(ConfirmState),
                 case Key =< DeliveryTag of
                     true  -> multi_confirm(DeliveryTag, ConfirmState1);
                     false -> ConfirmState
                 end
    end.
