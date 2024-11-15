%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%
-module(amqp10_client_types).

-include_lib("amqp10_common/include/amqp10_framing.hrl").

-export([unpack/1,
         infer/1,
         utf8/1,
         uint/1,
         make_properties/1]).

-type amqp10_performative() :: #'v1_0.open'{} | #'v1_0.begin'{} | #'v1_0.attach'{} |
                               #'v1_0.flow'{} | #'v1_0.transfer'{} |
                               #'v1_0.disposition'{} | #'v1_0.detach'{} |
                               #'v1_0.end'{} | #'v1_0.close'{}.

-type amqp10_msg_record() :: #'v1_0.transfer'{} | #'v1_0.header'{} |
                             #'v1_0.delivery_annotations'{} |
                             #'v1_0.message_annotations'{} |
                             #'v1_0.properties'{} |
                             #'v1_0.application_properties'{} |
                             #'v1_0.data'{} | #'v1_0.amqp_sequence'{} |
                             #'v1_0.amqp_value'{} | #'v1_0.footer'{}.

-type channel() :: non_neg_integer().

-type source() :: #'v1_0.source'{}.
-type target() :: #'v1_0.target'{}.

-type delivery_state() :: accepted |
                          rejected |
                          modified |
                          %% the "full" modified outcome
                          {modified,
                           DeliveryFailed :: boolean(),
                           UndeliverableHere :: boolean(),
                           MessageAnnotations :: #{amqp10_msg:annotations_key() => term()}
                           } |
                          received |
                          released.

-type amqp_error() :: internal_error | not_found | unauthorized_access |
                      decode_error | resource_limit_exceeded |
                      not_allowed | invalid_field | not_implemented |
                      resource_locked | precondition_failed | resource_deleted |
                      illegal_state | frame_size_too_small.

-type connection_error()  :: connection_forced | framing_error | redirect.
-type session_error() :: atom(). % TODO
-type link_error() :: atom(). % TODO

-type connection_event_detail() :: opened |
                                   {closed, Reason::any()} |
                                   {error, {connection_error(), any()}}.
-type session_event_detail() :: begun | ended | {error, {session_error(), any()}}.
-type link_event_detail() :: attached | detached | {error, {link_error(), any()}}.
-type amqp10_event_detail() :: {connection, pid(), connection_event_detail()} |
                               {session, pid(), session_event_detail()} |
                               {link, {sender | receiver, Name :: binary()},
                                link_event_detail()}.
-type amqp10_event() :: {amqp10_event, amqp10_event_detail()}.

-type properties() :: #{binary() => amqp10_binary_generator:amqp10_prim()}.

-export_type([amqp10_performative/0, channel/0,
              source/0, target/0, amqp10_msg_record/0,
              delivery_state/0, amqp_error/0, connection_error/0,
              amqp10_event_detail/0, amqp10_event/0,
              properties/0]).


unpack({_, Value}) ->
    Value;
unpack(Value) ->
    Value.

infer(V) when is_integer(V) ->
    {long, V};
infer(V) when is_number(V) ->
    %% AMQP double and Erlang float are both 64-bit.
    {double, V};
infer(V) when is_boolean(V) ->
    {boolean, V};
infer(V) when is_atom(V) ->
    {utf8, atom_to_binary(V, utf8)};
infer(TaggedValue) when is_atom(element(1, TaggedValue)) ->
    TaggedValue;
infer(V) ->
    utf8(V).

utf8(V) when is_binary(V) ->
    {utf8, V};
utf8(V) when is_list(V) ->
    {utf8, unicode:characters_to_binary(V)}.

uint(N) ->
    {uint, N}.

make_properties(#{properties := Props})
  when map_size(Props) > 0 ->
    {map, maps:fold(fun(K, V, L) ->
                            [{{symbol, K}, V} | L]
                    end, [], Props)};
make_properties(_) ->
    undefined.
