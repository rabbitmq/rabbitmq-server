%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(unit_ack_SUITE).
-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

all() ->
    [
     individual_ack_keeps_older_deliveries,
     individual_ack_preserves_order,
     multi_ack_prunes_older_deliveries,
     multi_ack_prunes_across_subscriptions,
     other_subscription_delivery_tag_is_not_found,
     unknown_delivery_tag_is_not_found,
     empty_queue_is_not_found,
     settlement_outside_a_transaction_is_not_found,
     multi_ack_pruned_delivery_is_absorbed,
     named_delivery_is_not_absorbed,
     individual_ack_records_nothing
    ].

individual_ack_keeps_older_deliveries(_Config) ->
    [PA1, PA2] = Ds = [pa(<<"ctag">>, 1, false), pa(<<"ctag">>, 2, false)],
    {Acked, Remaining} = collect(Ds, <<"ctag">>, 2),
    ?assertEqual([PA2], Acked),
    ?assertEqual([PA1], lqueue:to_list(Remaining)).

individual_ack_preserves_order(_Config) ->
    [PA1, PA2, PA3, PA4] = Ds = [pa(<<"ctag">>, N, false) || N <- [1, 2, 3, 4]],
    {Acked, Remaining} = collect(Ds, <<"ctag">>, 3),
    ?assertEqual([PA3], Acked),
    ?assertEqual([PA1, PA2, PA4], lqueue:to_list(Remaining)).

multi_ack_prunes_older_deliveries(_Config) ->
    [PA1, PA2, PA3] = Ds = [pa(<<"ctag">>, N, true) || N <- [1, 2, 3]],
    {Acked, Remaining} = collect(Ds, <<"ctag">>, 2),
    ?assertEqual([PA2, PA1], Acked),
    ?assertEqual([PA3], lqueue:to_list(Remaining)).

multi_ack_prunes_across_subscriptions(_Config) ->
    [PA1, PA2] = Ds = [pa(<<"other">>, 1, false), pa(<<"ctag">>, 2, true)],
    {Acked, Remaining} = collect(Ds, <<"ctag">>, 2),
    ?assertEqual([PA2, PA1], Acked),
    ?assertEqual([], lqueue:to_list(Remaining)).

other_subscription_delivery_tag_is_not_found(_Config) ->
    ?assertEqual({error, not_found},
                 collect([pa(<<"other">>, 1, false)], <<"ctag">>, 1)).

unknown_delivery_tag_is_not_found(_Config) ->
    ?assertEqual({error, not_found},
                 collect([pa(<<"ctag">>, 1, false)], <<"ctag">>, 7)).

empty_queue_is_not_found(_Config) ->
    ?assertEqual({error, not_found}, collect([], <<"ctag">>, 1)).

settlement_outside_a_transaction_is_not_found(_Config) ->
    erase(tx_settled_delivery_tags),
    ?assertMatch({error, "Message not found", _, state},
                 settle_again({<<"ctag">>, 1})).

multi_ack_pruned_delivery_is_absorbed(_Config) ->
    in_transaction(
      fun() ->
              record([pa(<<"ctag">>, 2, true), pa(<<"ctag">>, 1, true)]),
              ?assertEqual(state, settle_again({<<"ctag">>, 1}))
      end).

named_delivery_is_not_absorbed(_Config) ->
    in_transaction(
      fun() ->
              record([pa(<<"ctag">>, 2, true), pa(<<"ctag">>, 1, true)]),
              ?assertMatch({error, "Message not found", _, state},
                           settle_again({<<"ctag">>, 2}))
      end).

individual_ack_records_nothing(_Config) ->
    in_transaction(
      fun() ->
              record([pa(<<"ctag">>, 1, false)]),
              ?assertMatch({error, "Message not found", _, state},
                           settle_again({<<"ctag">>, 1}))
      end).

in_transaction(Fun) ->
    put(tx_settled_delivery_tags, sets:new([{version, 2}])),
    try Fun()
    after
        erase(tx_settled_delivery_tags)
    end.

record(Acked) ->
    rabbit_stomp_processor:record_transaction_settlements(Acked).

settle_again(Ack) ->
    rabbit_stomp_processor:unknown_delivery_tag(Ack, state).

collect(Deliveries, ConsumerTag, DeliveryTag) ->
    rabbit_stomp_processor:collect_acks(
      lqueue:from_list(Deliveries), ConsumerTag, DeliveryTag).

pa(ConsumerTag, DeliveryTag, MultiAck) ->
    rabbit_stomp_processor:pending_ack(ConsumerTag, DeliveryTag, MultiAck).
