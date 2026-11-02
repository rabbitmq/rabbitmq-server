%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(unit_SUITE).

-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile([export_all, nowarn_export_all]).

all() ->
    [
     {group, unit_tests},
     {group, property_tests}
    ].

groups() ->
    [
     {unit_tests, [parallel], [
         parse_ackmode_valid_values,
         parse_ackmode_rejects_unknown,
         decode_large_string_body,
         decode_body_exceeding_heap_limit
     ]},
     {property_tests, [parallel], [
         parse_ackmode_never_creates_atoms
     ]}
    ].

init_per_suite(Config) -> Config.
end_per_suite(_Config) -> ok.
init_per_group(_, Config) -> Config.
end_per_group(_, _Config) -> ok.

%%
%% parse_ackmode/1
%%

parse_ackmode_valid_values(_Config) ->
    ack_requeue_true     = rabbit_mgmt_wm_queue_get:parse_ackmode(<<"ack_requeue_true">>),
    ack_requeue_false    = rabbit_mgmt_wm_queue_get:parse_ackmode(<<"ack_requeue_false">>),
    reject_requeue_true  = rabbit_mgmt_wm_queue_get:parse_ackmode(<<"reject_requeue_true">>),
    reject_requeue_false = rabbit_mgmt_wm_queue_get:parse_ackmode(<<"reject_requeue_false">>),
    ok.

parse_ackmode_rejects_unknown(_Config) ->
    lists:foreach(
      fun(Val) ->
              ?assertThrow({error, _}, rabbit_mgmt_wm_queue_get:parse_ackmode(Val))
      end,
      [<<"">>, <<"nack">>, <<"ack">>, <<"true">>, <<"false">>,
       <<"ACK_REQUEUE_TRUE">>, <<"ack_requeue_true ">>,
       <<"reject_requeue">>, <<"unknown_mode">>]),
    ok.

parse_ackmode_never_creates_atoms(_Config) ->
    Valid = [<<"ack_requeue_true">>, <<"ack_requeue_false">>,
             <<"reject_requeue_true">>, <<"reject_requeue_false">>],
    Prop = ?FORALL(
        Bin,
        proper_types:binary(),
        case lists:member(Bin, Valid) of
            true ->
                is_atom(rabbit_mgmt_wm_queue_get:parse_ackmode(Bin));
            false ->
                try
                    rabbit_mgmt_wm_queue_get:parse_ackmode(Bin),
                    false
                catch throw:{error, _} ->
                    true
                end
        end),
    ?assert(proper:quickcheck(Prop, [quiet, {numtests, 500}])).

%%
%% rabbit_mgmt_util:decode/1
%%

decode_large_string_body(_Config) ->
    Payload = binary:copy(<<"a">>, 5_000_000),
    Body = rabbit_json:encode(#{<<"payload">> => Payload}),
    {Pid, MRef} = spawn_monitor(
                    fun() ->
                            {ok, #{<<"payload">> := Payload}} = rabbit_mgmt_util:decode(Body)
                    end),
    receive {'DOWN', MRef, process, Pid, Reason} -> ?assertEqual(normal, Reason) end.

decode_body_exceeding_heap_limit(_Config) ->
    Body = rabbit_json:encode(#{<<"padding">> => lists:duplicate(2_000_000, 1)}),
    {Pid, MRef} = spawn_monitor(fun() -> rabbit_mgmt_util:decode(Body) end),
    receive {'DOWN', MRef, process, Pid, Reason} -> ?assertEqual(killed, Reason) end.
