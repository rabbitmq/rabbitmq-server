%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(unit_definitions_import_https_SUITE).

-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

all() ->
    [
      {group, parallel_tests}
    ].

groups() ->
    [
      {parallel_tests, [parallel], [
          tls_options_explicit_ssl_options,
          tls_options_default_when_absent,
          tls_options_default_when_empty_proplist,
          tls_options_single_override,
          tls_options_map_input
        ]}
    ].


tls_options_explicit_ssl_options(_Config) ->
    MyOpts = [{verify, verify_none}, {versions, ['tlsv1.3']}],
    Proplist = [{url, "https://example.com/defs.json"},
                {ssl_options, MyOpts}],
    MyOpts = rabbit_definitions_import_https:tls_options_or_default(Proplist),
    passed.

tls_options_default_when_absent(_Config) ->
    Proplist = [{url, "https://example.com/defs.json"}],
    Result = rabbit_definitions_import_https:tls_options_or_default(Proplist),
    ?assertEqual(verify_peer, proplists:get_value(verify, Result)),
    ?assertEqual(2, proplists:get_value(depth, Result)),
    ?assertEqual(error, proplists:get_value(log_level, Result)),
    ?assertEqual(['tlsv1.2'], proplists:get_value(versions, Result)),
    ?assert(is_list(proplists:get_value(cacerts, Result, []))),
    passed.

tls_options_default_when_empty_proplist(_Config) ->
    Result = rabbit_definitions_import_https:tls_options_or_default([]),
    ?assertEqual(verify_peer, proplists:get_value(verify, Result)),
    ?assertEqual(2, proplists:get_value(depth, Result)),
    ?assertEqual(error, proplists:get_value(log_level, Result)),
    passed.

tls_options_single_override(_Config) ->
    MyOpts = [{verify, verify_none}],
    Proplist = [{url, "https://example.com/defs.json"},
                {ssl_options, MyOpts}],
    Result = rabbit_definitions_import_https:tls_options_or_default(Proplist),
    ?assertEqual(verify_none, proplists:get_value(verify, Result)),
    %% Other defaults should still be present since MyOpts replaces all defaults
    passed.

tls_options_map_input(_Config) ->
    MyOpts = #{verify => verify_none, versions => ['tlsv1.3']},
    Proplist = #{url => "https://example.com/defs.json",
                 ssl_options => MyOpts},
    MyOpts = rabbit_definitions_import_https:tls_options_or_default(Proplist),
    passed.
