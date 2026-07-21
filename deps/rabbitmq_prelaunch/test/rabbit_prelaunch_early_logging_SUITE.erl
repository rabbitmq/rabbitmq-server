%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_prelaunch_early_logging_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

%%%===================================================================
%%% Common Test callbacks
%%%===================================================================

all() ->
    [{group, tests}].

all_tests() ->
    [generic_conf_explicit_depth,
     generic_conf_unlimited_falls_back_to_error_logger,
     generic_conf_missing_depth_falls_back_to_error_logger].

groups() ->
    [{tests, [], all_tests()}].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(_Group, Config) ->
    Config.

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    Saved = application:get_env(kernel, error_logger_format_depth),
    [{saved_format_depth, Saved} | Config].

end_per_testcase(_TestCase, Config) ->
    case ?config(saved_format_depth, Config) of
        {ok, V}   -> application:set_env(kernel, error_logger_format_depth, V);
        undefined -> application:unset_env(kernel, error_logger_format_depth)
    end,
    ok.

%%%===================================================================
%%% Helpers
%%%===================================================================

%% A minimal plaintext-formatter cuttlefish conf. `Extra' entries are
%% appended so individual tests can add or omit the `depth' key.
base_conf(Extra) ->
    [{["log", "file"], plaintext},
     {["log", "file", "time_format"], rfc3339_space},
     {["log", "file", "level_format"], lc},
     {["log", "file", "single_line"], false}
     | Extra].

depth_of(Conf) ->
    Generic = rabbit_prelaunch_early_logging:translate_generic_conf(
                "log.file", Conf),
    maps:get(depth, Generic).

%%%===================================================================
%%% Test cases
%%%===================================================================

%% An explicit integer `depth' is carried into the generic formatter
%% config.
generic_conf_explicit_depth(_Config) ->
    Conf = base_conf([{["log", "file", "depth"], 20}]),
    ?assertEqual(20, depth_of(Conf)).

%% `depth = unlimited' means "no explicit depth"; fall back to the
%% deprecated `error_logger_format_depth' kernel variable, matching
%% OTP's `logger_formatter:get_depth/1'.
generic_conf_unlimited_falls_back_to_error_logger(_Config) ->
    application:set_env(kernel, error_logger_format_depth, 10),
    Conf = base_conf([{["log", "file", "depth"], unlimited}]),
    ?assertEqual(10, depth_of(Conf)).

%% A missing `depth' key behaves the same as `unlimited': fall back to
%% the kernel variable.
generic_conf_missing_depth_falls_back_to_error_logger(_Config) ->
    application:set_env(kernel, error_logger_format_depth, 10),
    Conf = base_conf([]),
    ?assertEqual(10, depth_of(Conf)).
