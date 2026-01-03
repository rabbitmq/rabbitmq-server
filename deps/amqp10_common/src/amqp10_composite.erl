%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(amqp10_composite).

-include("amqp10_framing.hrl").

-export([flow/2,
         transfer/2,
         disposition/2,
         header/2,
         properties/2]).

-spec flow(#'v1_0.flow'{}, nonempty_list()) ->
    #'v1_0.flow'{}.
flow(F, [F1, F2, F3, F4]) ->
    F#'v1_0.flow'{next_incoming_id = ntu(F1),
                  incoming_window = ntu(F2),
                  next_outgoing_id = ntu(F3),
                  outgoing_window = ntu(F4)};
flow(F, [F1, F2, F3, F4, F5]) ->
    F#'v1_0.flow'{next_incoming_id = ntu(F1),
                  incoming_window = ntu(F2),
                  next_outgoing_id = ntu(F3),
                  outgoing_window = ntu(F4),
                  handle = ntu(F5)};
flow(F, [F1, F2, F3, F4, F5, F6]) ->
    F#'v1_0.flow'{next_incoming_id = ntu(F1),
                  incoming_window = ntu(F2),
                  next_outgoing_id = ntu(F3),
                  outgoing_window = ntu(F4),
                  handle = ntu(F5),
                  delivery_count = ntu(F6)};
flow(F, [F1, F2, F3, F4, F5, F6, F7]) ->
    F#'v1_0.flow'{next_incoming_id = ntu(F1),
                  incoming_window = ntu(F2),
                  next_outgoing_id = ntu(F3),
                  outgoing_window = ntu(F4),
                  handle = ntu(F5),
                  delivery_count = ntu(F6),
                  link_credit = ntu(F7)};
flow(F, [F1, F2, F3, F4, F5, F6, F7, F8]) ->
    F#'v1_0.flow'{next_incoming_id = ntu(F1),
                  incoming_window = ntu(F2),
                  next_outgoing_id = ntu(F3),
                  outgoing_window = ntu(F4),
                  handle = ntu(F5),
                  delivery_count = ntu(F6),
                  link_credit = ntu(F7),
                  available = ntu(F8)};
flow(F, [F1, F2, F3, F4, F5, F6, F7, F8, F9]) ->
    F#'v1_0.flow'{next_incoming_id = ntu(F1),
                  incoming_window = ntu(F2),
                  next_outgoing_id = ntu(F3),
                  outgoing_window = ntu(F4),
                  handle = ntu(F5),
                  delivery_count = ntu(F6),
                  link_credit = ntu(F7),
                  available = ntu(F8),
                  drain = ntu(F9)};
flow(F, [F1, F2, F3, F4, F5, F6, F7, F8, F9, F10]) ->
    F#'v1_0.flow'{next_incoming_id = ntu(F1),
                  incoming_window = ntu(F2),
                  next_outgoing_id = ntu(F3),
                  outgoing_window = ntu(F4),
                  handle = ntu(F5),
                  delivery_count = ntu(F6),
                  link_credit = ntu(F7),
                  available = ntu(F8),
                  drain = ntu(F9),
                  echo = ntu(F10)};
flow(F, [F1, F2, F3, F4, F5, F6, F7, F8, F9, F10, F11]) ->
    F#'v1_0.flow'{next_incoming_id = ntu(F1),
                  incoming_window = ntu(F2),
                  next_outgoing_id = ntu(F3),
                  outgoing_window = ntu(F4),
                  handle = ntu(F5),
                  delivery_count = ntu(F6),
                  link_credit = ntu(F7),
                  available = ntu(F8),
                  drain = ntu(F9),
                  echo = ntu(F10),
                  properties = amqp10_framing:decode(F11)}.

-spec transfer(#'v1_0.transfer'{}, nonempty_list()) ->
    #'v1_0.transfer'{}.
transfer(T, [F1]) ->
    T#'v1_0.transfer'{handle = ntu(F1)};
transfer(T, [F1, F2]) ->
    T#'v1_0.transfer'{handle = ntu(F1),
                      delivery_id = ntu(F2)};
transfer(T, [F1, F2, F3]) ->
    T#'v1_0.transfer'{handle = ntu(F1),
                      delivery_id = ntu(F2),
                      delivery_tag = ntu(F3)};
transfer(T, [F1, F2, F3, F4]) ->
    T#'v1_0.transfer'{handle = ntu(F1),
                      delivery_id = ntu(F2),
                      delivery_tag = ntu(F3),
                      message_format = ntu(F4)};
transfer(T, [F1, F2, F3, F4, F5]) ->
    T#'v1_0.transfer'{handle = ntu(F1),
                      delivery_id = ntu(F2),
                      delivery_tag = ntu(F3),
                      message_format = ntu(F4),
                      settled = ntu(F5)};
transfer(T, [F1, F2, F3, F4, F5, F6]) ->
    T#'v1_0.transfer'{handle = ntu(F1),
                      delivery_id = ntu(F2),
                      delivery_tag = ntu(F3),
                      message_format = ntu(F4),
                      settled = ntu(F5),
                      more = ntu(F6)};
transfer(T, [F1, F2, F3, F4, F5, F6, F7]) ->
    T#'v1_0.transfer'{handle = ntu(F1),
                      delivery_id = ntu(F2),
                      delivery_tag = ntu(F3),
                      message_format = ntu(F4),
                      settled = ntu(F5),
                      more = ntu(F6),
                      rcv_settle_mode = ntu(F7)};
transfer(T, [F1, F2, F3, F4, F5, F6, F7, F8]) ->
    T#'v1_0.transfer'{handle = ntu(F1),
                      delivery_id = ntu(F2),
                      delivery_tag = ntu(F3),
                      message_format = ntu(F4),
                      settled = ntu(F5),
                      more = ntu(F6),
                      rcv_settle_mode = ntu(F7),
                      state = amqp10_framing:decode(F8)};
transfer(T, [F1, F2, F3, F4, F5, F6, F7, F8, F9]) ->
    T#'v1_0.transfer'{handle = ntu(F1),
                      delivery_id = ntu(F2),
                      delivery_tag = ntu(F3),
                      message_format = ntu(F4),
                      settled = ntu(F5),
                      more = ntu(F6),
                      rcv_settle_mode = ntu(F7),
                      state = amqp10_framing:decode(F8),
                      resume = ntu(F9)};
transfer(T, [F1, F2, F3, F4, F5, F6, F7, F8, F9, F10]) ->
    T#'v1_0.transfer'{handle = ntu(F1),
                      delivery_id = ntu(F2),
                      delivery_tag = ntu(F3),
                      message_format = ntu(F4),
                      settled = ntu(F5),
                      more = ntu(F6),
                      rcv_settle_mode = ntu(F7),
                      state = amqp10_framing:decode(F8),
                      resume = ntu(F9),
                      aborted = ntu(F10)};
transfer(T, [F1, F2, F3, F4, F5, F6, F7, F8, F9, F10, F11]) ->
    T#'v1_0.transfer'{handle = ntu(F1),
                      delivery_id = ntu(F2),
                      delivery_tag = ntu(F3),
                      message_format = ntu(F4),
                      settled = ntu(F5),
                      more = ntu(F6),
                      rcv_settle_mode = ntu(F7),
                      state = amqp10_framing:decode(F8),
                      resume = ntu(F9),
                      aborted = ntu(F10),
                      batchable = ntu(F11)}.

-spec disposition(#'v1_0.disposition'{}, nonempty_list()) ->
    #'v1_0.disposition'{}.
disposition(D, [F1, F2]) ->
    D#'v1_0.disposition'{role = ntu(F1),
                         first = ntu(F2)};
disposition(D, [F1, F2, F3]) ->
    D#'v1_0.disposition'{role = ntu(F1),
                         first = ntu(F2),
                         last = ntu(F3)};
disposition(D, [F1, F2, F3, F4]) ->
    D#'v1_0.disposition'{role = ntu(F1),
                         first = ntu(F2),
                         last = ntu(F3),
                         settled = ntu(F4)};
disposition(D, [F1, F2, F3, F4, F5]) ->
    D#'v1_0.disposition'{role = ntu(F1),
                         first = ntu(F2),
                         last = ntu(F3),
                         settled = ntu(F4),
                         state = amqp10_framing:decode(F5)};
disposition(D, [F1, F2, F3, F4, F5, F6]) ->
    D#'v1_0.disposition'{role = ntu(F1),
                         first = ntu(F2),
                         last = ntu(F3),
                         settled = ntu(F4),
                         state = amqp10_framing:decode(F5),
                         batchable = ntu(F6)}.

-spec header(#'v1_0.header'{}, list()) ->
    #'v1_0.header'{}.
header(H, []) ->
    H;
header(H, [F1]) ->
    H#'v1_0.header'{durable = ntu(F1)};
header(H, [F1, F2]) ->
    H#'v1_0.header'{durable = ntu(F1),
                    priority = ntu(F2)};
header(H, [F1, F2, F3]) ->
    H#'v1_0.header'{durable = ntu(F1),
                    priority = ntu(F2),
                    ttl = ntu(F3)};
header(H, [F1, F2, F3, F4]) ->
    H#'v1_0.header'{durable = ntu(F1),
                    priority = ntu(F2),
                    ttl = ntu(F3),
                    first_acquirer = ntu(F4)};
header(H, [F1, F2, F3, F4, F5]) ->
    H#'v1_0.header'{durable = ntu(F1),
                    priority = ntu(F2),
                    ttl = ntu(F3),
                    first_acquirer = ntu(F4),
                    delivery_count = ntu(F5)}.

-spec properties(#'v1_0.properties'{}, list()) ->
    #'v1_0.properties'{}.
properties(P, []) ->
    P;
properties(P, [F1]) ->
    P#'v1_0.properties'{message_id = ntu(F1)};
properties(P, [F1, F2]) ->
    P#'v1_0.properties'{message_id = ntu(F1),
                        user_id = ntu(F2)};
properties(P, [F1, F2, F3]) ->
    P#'v1_0.properties'{message_id = ntu(F1),
                        user_id = ntu(F2),
                        to = ntu(F3)};
properties(P, [F1, F2, F3, F4]) ->
    P#'v1_0.properties'{message_id = ntu(F1),
                        user_id = ntu(F2),
                        to = ntu(F3),
                        subject = ntu(F4)};
properties(P, [F1, F2, F3, F4, F5]) ->
    P#'v1_0.properties'{message_id = ntu(F1),
                        user_id = ntu(F2),
                        to = ntu(F3),
                        subject = ntu(F4),
                        reply_to = ntu(F5)};
properties(P, [F1, F2, F3, F4, F5, F6]) ->
    P#'v1_0.properties'{message_id = ntu(F1),
                        user_id = ntu(F2),
                        to = ntu(F3),
                        subject = ntu(F4),
                        reply_to = ntu(F5),
                        correlation_id = ntu(F6)};
properties(P, [F1, F2, F3, F4, F5, F6, F7]) ->
    P#'v1_0.properties'{message_id = ntu(F1),
                        user_id = ntu(F2),
                        to = ntu(F3),
                        subject = ntu(F4),
                        reply_to = ntu(F5),
                        correlation_id = ntu(F6),
                        content_type = ntu(F7)};
properties(P, [F1, F2, F3, F4, F5, F6, F7, F8]) ->
    P#'v1_0.properties'{message_id = ntu(F1),
                        user_id = ntu(F2),
                        to = ntu(F3),
                        subject = ntu(F4),
                        reply_to = ntu(F5),
                        correlation_id = ntu(F6),
                        content_type = ntu(F7),
                        content_encoding = ntu(F8)};
properties(P, [F1, F2, F3, F4, F5, F6, F7, F8, F9]) ->
    P#'v1_0.properties'{message_id = ntu(F1),
                        user_id = ntu(F2),
                        to = ntu(F3),
                        subject = ntu(F4),
                        reply_to = ntu(F5),
                        correlation_id = ntu(F6),
                        content_type = ntu(F7),
                        content_encoding = ntu(F8),
                        absolute_expiry_time = ntu(F9)};
properties(P, [F1, F2, F3, F4, F5, F6, F7, F8, F9, F10]) ->
    P#'v1_0.properties'{message_id = ntu(F1),
                        user_id = ntu(F2),
                        to = ntu(F3),
                        subject = ntu(F4),
                        reply_to = ntu(F5),
                        correlation_id = ntu(F6),
                        content_type = ntu(F7),
                        content_encoding = ntu(F8),
                        absolute_expiry_time = ntu(F9),
                        creation_time = ntu(F10)};
properties(P, [F1, F2, F3, F4, F5, F6, F7, F8, F9, F10, F11]) ->
    P#'v1_0.properties'{message_id = ntu(F1),
                        user_id = ntu(F2),
                        to = ntu(F3),
                        subject = ntu(F4),
                        reply_to = ntu(F5),
                        correlation_id = ntu(F6),
                        content_type = ntu(F7),
                        content_encoding = ntu(F8),
                        absolute_expiry_time = ntu(F9),
                        creation_time = ntu(F10),
                        group_id = ntu(F11)};
properties(P, [F1, F2, F3, F4, F5, F6, F7, F8, F9, F10, F11, F12]) ->
    P#'v1_0.properties'{message_id = ntu(F1),
                        user_id = ntu(F2),
                        to = ntu(F3),
                        subject = ntu(F4),
                        reply_to = ntu(F5),
                        correlation_id = ntu(F6),
                        content_type = ntu(F7),
                        content_encoding = ntu(F8),
                        absolute_expiry_time = ntu(F9),
                        creation_time = ntu(F10),
                        group_id = ntu(F11),
                        group_sequence = ntu(F12)};
properties(P, [F1, F2, F3, F4, F5, F6, F7, F8, F9, F10, F11, F12, F13]) ->
    P#'v1_0.properties'{message_id = ntu(F1),
                        user_id = ntu(F2),
                        to = ntu(F3),
                        subject = ntu(F4),
                        reply_to = ntu(F5),
                        correlation_id = ntu(F6),
                        content_type = ntu(F7),
                        content_encoding = ntu(F8),
                        absolute_expiry_time = ntu(F9),
                        creation_time = ntu(F10),
                        group_id = ntu(F11),
                        group_sequence = ntu(F12),
                        reply_to_group_id = ntu(F13)}.

%% null to undefined
-compile({inline, [ntu/1]}).
ntu(null) ->
    undefined;
ntu(Other) ->
    Other.
