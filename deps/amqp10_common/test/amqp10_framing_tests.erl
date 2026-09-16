%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(amqp10_framing_tests).

-include_lib("eunit/include/eunit.hrl").
-include("amqp10_framing.hrl").

encode_decode_test_() ->
    Data = [{{symbol, <<"x-my key">>}, {binary, <<"my value">>}}],
    Test = fun(M) -> [M] = amqp10_framing:decode_bin(iolist_to_binary(amqp10_framing:encode_bin(M))) end,
    [
     fun() -> Test(#'v1_0.application_properties'{content = Data}) end,
     fun() -> Test(#'v1_0.delivery_annotations'{content = Data}) end,
     fun() -> Test(#'v1_0.message_annotations'{content = Data}) end,
     fun() -> Test(#'v1_0.footer'{content = Data}) end
    ].

encode_decode_amqp_sequence_test() ->
    L = [{utf8, <<"k">>},
         {binary, <<"v">>}],
    F = #'v1_0.amqp_sequence'{content = L},
    [F] = amqp10_framing:decode_bin(iolist_to_binary(amqp10_framing:encode_bin(F))),
    ok.
