%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_logger_json_fmt_SUITE).

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
    [msg_args_depth_truncates,
     msg_args_depth_unlimited_is_full,
     msg_report_depth_truncates].

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
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

%%%===================================================================
%%% Helpers
%%%===================================================================

deep_term() ->
    [{level1, [{level2, [{level3, [{level4, [{level5, secret}]}]}]}]}].

config(Depth) ->
    #{single_line => false,
      depth => Depth,
      time_format => {rfc3339, $\s, ""},
      level_format => lc}.

format(Msg, Config) ->
    Event = #{msg => Msg, level => error, meta => #{time => 0}},
    iolist_to_binary(rabbit_logger_json_fmt:format(Event, Config)).

%%%===================================================================
%%% Test cases
%%%===================================================================

%% The JSON formatter renders the `msg' field through the shared
%% `format_msg/3', so a `{Format, Args}' message honors the configured
%% depth: deep sub-terms are truncated to `...' and the produced line is
%% still valid JSON.
msg_args_depth_truncates(_Config) ->
    Formatted = format({"~p", [deep_term()]}, config(3)),
    ?assertMatch(#{}, rabbit_json:decode(Formatted)),
    ?assertNotEqual(nomatch, binary:match(Formatted, <<"...">>)),
    ?assertEqual(nomatch, binary:match(Formatted, <<"secret">>)).

msg_args_depth_unlimited_is_full(_Config) ->
    Formatted = format({"~p", [deep_term()]}, config(unlimited)),
    ?assertMatch(#{}, rabbit_json:decode(Formatted)),
    ?assertNotEqual(nomatch, binary:match(Formatted, <<"secret">>)),
    ?assertEqual(nomatch, binary:match(Formatted, <<"...">>)).

%% Crash reports are logged as `{report, Report}' messages with an
%% arity-2 `report_cb'. They too flow through `format_msg/3', so the
%% depth is passed to the callback and the report is truncated.
msg_report_depth_truncates(_Config) ->
    Report = #{term => deep_term()},
    Cb = fun(#{term := T}, Extra) ->
                 Depth = maps:get(depth, Extra, unlimited),
                 case Depth of
                     unlimited -> io_lib:format("~p", [T]);
                     D         -> io_lib:format("~P", [T, D])
                 end
         end,
    Event = #{msg => {report, Report},
              level => error,
              meta => #{time => 0, report_cb => Cb}},
    Formatted = iolist_to_binary(
                  rabbit_logger_json_fmt:format(Event, config(3))),
    ?assertMatch(#{}, rabbit_json:decode(Formatted)),
    ?assertNotEqual(nomatch, binary:match(Formatted, <<"...">>)),
    ?assertEqual(nomatch, binary:match(Formatted, <<"secret">>)).
