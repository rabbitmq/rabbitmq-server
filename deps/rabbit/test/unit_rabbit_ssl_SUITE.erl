%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(unit_rabbit_ssl_SUITE).

-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

all() ->
    [
      {group, parallel_tests}
    ].

groups() ->
    [
      {parallel_tests, [], [
          wrap_tls_opts_with_binary_password,
          wrap_tls_opts_with_function_password
        ]}
    ].


wrap_tls_opts_with_binary_password(_Config) ->
    Path = "/tmp/path/to/private_key.pem",
    Bin = <<"s3krE7">>,
    Opts0 = [
      {keyfile, Path},
      {password, Bin}
    ],

    Opts = rabbit_ssl_options:wrap_password_opt(Opts0),
    M = maps:from_list(Opts),

    ?assertEqual(Path, maps:get(keyfile, M)),
    ?assert(is_function(maps:get(password, M))),

    F = maps:get(password, M),
    ?assertEqual(Bin, F()),

    passed.

wrap_tls_opts_with_function_password(_Config) ->
  Path = "/tmp/path/to/private_key.pem",
  Bin = <<"s3krE7">>,
  Fun = fun() -> Bin end,
  Opts0 = [
    {keyfile, Path},
    {password, Fun}
  ],

  Opts = rabbit_ssl_options:wrap_password_opt(Opts0),
  M = maps:from_list(Opts),

  ?assertEqual(Path, maps:get(keyfile, M)),
  ?assert(is_function(maps:get(password, M))),
  ?assertEqual(Fun, maps:get(password, M)),

  F = maps:get(password, M),
  ?assertEqual(Bin, F()),

  passed.