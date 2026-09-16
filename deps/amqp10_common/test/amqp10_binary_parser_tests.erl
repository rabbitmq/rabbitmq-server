%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(amqp10_binary_parser_tests).

-include_lib("eunit/include/eunit.hrl").

-import(amqp10_binary_parser, [peek_value_size/1]).

peek_value_size_fixed_test() ->
    %% 1-byte primitives (type code only)
    ?assertEqual(1, peek_value_size(<<16#40, 0>>)),
    ?assertEqual(1, peek_value_size(<<16#41, 0>>)),
    ?assertEqual(1, peek_value_size(<<16#45, 0>>)),
    %% 2-byte (type + 1 byte)
    ?assertEqual(2, peek_value_size(<<16#50, 42>>)),
    ?assertEqual(2, peek_value_size(<<16#53, 16#75>>)),
    %% 3-byte (type + 2 bytes)
    ?assertEqual(3, peek_value_size(<<16#60, 0, 1>>)),
    %% 5-byte (type + 4 bytes)
    ?assertEqual(5, peek_value_size(<<16#70, 0, 0, 0, 0>>)),
    %% 9-byte (type + 8 bytes)
    ?assertEqual(9, peek_value_size(<<16#80, 0:64>>)),
    %% 17-byte (uuid)
    ?assertEqual(17, peek_value_size(<<16#98, 0:128>>)).

peek_value_size_variable_test() ->
    %% Binary: 0xa0 + size (1 byte) + payload -> 2 + S
    ?assertEqual(5, peek_value_size(<<16#a0, 3, "foo">>)),
    %% UTF8: 0xa1 + size + payload
    ?assertEqual(6, peek_value_size(<<16#a1, 4, "test">>)),
    %% Symbol (CODE_SYM_8 = 0xa3)
    ?assertEqual(7, peek_value_size(<<16#a3, 5, "hello">>)),
    %% List: 0xc0 + size byte -> 2 + Size
    ?assertEqual(4, peek_value_size(<<16#c0, 2, 0, 16#40>>)).
