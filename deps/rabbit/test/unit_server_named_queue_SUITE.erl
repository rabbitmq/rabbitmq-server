%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(unit_server_named_queue_SUITE).

-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

all() ->
    [
     default_when_absent,
     custom_prefix_from_args,
     custom_prefix_with_other_args,
     default_when_empty_binary,
     default_when_wrong_type,
     default_with_unrelated_args,
     error_when_prefix_too_long,
     byte_limit_counts_bytes_not_chars
    ].

%% No `x-name-prefix` arg: falls back to the default <<"amq.gen">>.
default_when_absent(_Config) ->
    ?assertEqual({ok, <<"amq.gen">>},
                 rabbit_amqqueue:server_named_queue_prefix([])).

%% A valid `x-name-prefix` is returned as-is.
custom_prefix_from_args(_Config) ->
    ?assertEqual({ok, <<"myapp">>},
                 rabbit_amqqueue:server_named_queue_prefix(
                   [{<<"x-name-prefix">>, longstr, <<"myapp">>}])),
    ?assertEqual({ok, <<"acme.corp">>},
                 rabbit_amqqueue:server_named_queue_prefix(
                   [{<<"x-name-prefix">>, longstr, <<"acme.corp">>}])).

%% `x-name-prefix` is read correctly even when other args are present.
custom_prefix_with_other_args(_Config) ->
    Args = [
        {<<"x-queue-type">>, longstr, <<"classic">>},
        {<<"x-name-prefix">>, longstr, <<"myapp">>},
        {<<"x-max-length">>, signedint, 1000}
    ],
    ?assertEqual({ok, <<"myapp">>},
                 rabbit_amqqueue:server_named_queue_prefix(Args)).

%% An empty binary falls back to the default.
default_when_empty_binary(_Config) ->
    ?assertEqual({ok, <<"amq.gen">>},
                 rabbit_amqqueue:server_named_queue_prefix(
                   [{<<"x-name-prefix">>, longstr, <<>>}])).

%% A non-longstr type falls back to the default.
default_when_wrong_type(_Config) ->
    ?assertEqual({ok, <<"amq.gen">>},
                 rabbit_amqqueue:server_named_queue_prefix(
                   [{<<"x-name-prefix">>, signedint, 42}])).

%% Unrelated args without `x-name-prefix` return the default.
default_with_unrelated_args(_Config) ->
    Args = [
        {<<"x-queue-type">>, longstr, <<"classic">>},
        {<<"x-max-length">>, signedint, 1000}
    ],
    ?assertEqual({ok, <<"amq.gen">>},
                 rabbit_amqqueue:server_named_queue_prefix(Args)).

%% A prefix longer than 64 bytes returns an error.
error_when_prefix_too_long(_Config) ->
    TooLong = binary:copy(<<"x">>, 65),
    ?assertEqual({error, {prefix_too_long, TooLong}},
                 rabbit_amqqueue:server_named_queue_prefix(
                   [{<<"x-name-prefix">>, longstr, TooLong}])),
    %% Exactly 64 bytes is still valid.
    AtLimit = binary:copy(<<"x">>, 64),
    ?assertEqual({ok, AtLimit},
                 rabbit_amqqueue:server_named_queue_prefix(
                   [{<<"x-name-prefix">>, longstr, AtLimit}])).

%% The limit is in bytes, not Unicode code points.
byte_limit_counts_bytes_not_chars(_Config) ->
    %% "é" is 2 bytes in UTF-8: 32 copies = 64 bytes, at the limit.
    AtLimit = binary:copy(<<"é"/utf8>>, 32),
    ?assertEqual(64, byte_size(AtLimit)),
    ?assertEqual({ok, AtLimit},
                 rabbit_amqqueue:server_named_queue_prefix(
                   [{<<"x-name-prefix">>, longstr, AtLimit}])),
    %% 33 copies = 66 bytes, over the limit.
    TooLong = binary:copy(<<"é"/utf8>>, 33),
    ?assertEqual({error, {prefix_too_long, TooLong}},
                 rabbit_amqqueue:server_named_queue_prefix(
                   [{<<"x-name-prefix">>, longstr, TooLong}])).
