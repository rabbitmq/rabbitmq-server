%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(unit_networking_ssl_options_overrides_SUITE).

-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

all() ->
    [
      {group, tests}
    ].

%% Every case sets and clears the same `rabbit` app env key, so they
%% cannot run in parallel.
groups() ->
    [
      {tests, [], [
          no_overrides_is_a_no_op,
          override_is_added_when_absent,
          override_replaces_listener_value,
          listener_value_is_preserved_when_not_overridden,
          bare_atom_socket_option_is_preserved,
          override_replaces_every_duplicate_listener_value
        ]}
    ].

init_per_testcase(_Testcase, Config) ->
    ok = application:unset_env(rabbit, ssl_options_overrides),
    Config.

end_per_testcase(_Testcase, _Config) ->
    ok = application:unset_env(rabbit, ssl_options_overrides).

%% -------------------------------------------------------------------
%% Testsuite cases
%% -------------------------------------------------------------------

no_overrides_is_a_no_op(_Config) ->
    Listener = [{port, 15676}, {certfile, "/path/to/cert.pem"}],
    Fixed = rabbit_networking:fix_ssl_options(Listener),
    ?assertEqual("/path/to/cert.pem", proplists:get_value(certfile, Fixed)).

override_is_added_when_absent(_Config) ->
    ok = application:set_env(rabbit, ssl_options_overrides, [{verify, verify_peer}]),
    Fixed = rabbit_networking:fix_ssl_options([{port, 15676}]),
    ?assertEqual(verify_peer, proplists:get_value(verify, Fixed)).

override_replaces_listener_value(_Config) ->
    ok = application:set_env(rabbit, ssl_options_overrides, [{verify, verify_peer}]),
    Fixed = rabbit_networking:fix_ssl_options([{verify, verify_none}]),
    ?assertEqual(verify_peer, proplists:get_value(verify, Fixed)).

listener_value_is_preserved_when_not_overridden(_Config) ->
    ok = application:set_env(rabbit, ssl_options_overrides, [{verify, verify_peer}]),
    Fixed = rabbit_networking:fix_ssl_options([{certfile, "/path/to/cert.pem"}]),
    ?assertEqual("/path/to/cert.pem", proplists:get_value(certfile, Fixed)).

%% `ssl_config` proplists legally carry bare atoms (e.g. `inet6`); the merge
%% must not assume every entry is a `{Key, Value}` pair.
bare_atom_socket_option_is_preserved(_Config) ->
    ok = application:set_env(rabbit, ssl_options_overrides, [{verify, verify_peer}]),
    Fixed = rabbit_networking:fix_ssl_options([inet6, {port, 15676}]),
    ?assert(lists:member(inet6, Fixed)),
    ?assertEqual(verify_peer, proplists:get_value(verify, Fixed)).

override_replaces_every_duplicate_listener_value(_Config) ->
    ok = application:set_env(rabbit, ssl_options_overrides, [{verify, verify_peer}]),
    Fixed = rabbit_networking:fix_ssl_options([{verify, verify_none}, {verify, verify_none}]),
    ?assertEqual([verify_peer], [V || {verify, V} <- Fixed]).
