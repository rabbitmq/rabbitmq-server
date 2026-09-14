%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_prelaunch_dist_SUITE).

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
    [set_credentials_obfuscation_secret_redacts_cookie].

groups() ->
    [{tests, [], all_tests()}].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(credentials_obfuscation),
    Config.

end_per_suite(_Config) ->
    _ = application:stop(credentials_obfuscation),
    ok.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

%%%===================================================================
%%% Test cases
%%%===================================================================

%% Must log the cookie hash, never the raw cookie.
set_credentials_obfuscation_secret_redacts_cookie(_Config) ->
    CookieBin = rabbit_data_coercion:to_binary(erlang:get_cookie()),
    CookieHash = rabbit_nodes_common:cookie_hash(),
    Lines = capture_debug_log(
              fun rabbit_prelaunch_dist:set_credentials_obfuscation_secret/0),
    ?assertNot(log_contains(Lines, binary_to_list(CookieBin))),
    ?assert(log_contains(Lines, CookieHash)).

%%%===================================================================
%%% Helpers
%%%===================================================================

%% Captures `?LOG_DEBUG` output via a temporary logger handler.
capture_debug_log(Fun) ->
    Ref = make_ref(),
    HandlerId = list_to_atom(
                  "rabbit_prelaunch_dist_SUITE_log_capture_" ++
                  integer_to_list(erlang:unique_integer([positive]))),
    #{level := PrevLevel} = logger:get_primary_config(),
    ok = logger:set_primary_config(level, debug),
    ok = logger:add_handler(
           HandlerId, ?MODULE,
           #{config => #{pid => self(), ref => Ref}, level => debug}),
    try
        Fun()
    after
        _ = logger:remove_handler(HandlerId),
        ok = logger:set_primary_config(level, PrevLevel)
    end,
    [format_event(Event) || Event <- drain_log_events(Ref, [])].

drain_log_events(Ref, Acc) ->
    receive
        {Ref, Event} -> drain_log_events(Ref, [Event | Acc])
    after 500 ->
        lists:reverse(Acc)
    end.

format_event(#{msg := {Fmt, Args}}) when is_list(Fmt) ->
    lists:flatten(io_lib:format(Fmt, Args));
format_event(_) ->
    "".

log_contains(Lines, Needle) ->
    lists:any(fun(Line) -> string:find(Line, Needle) =/= nomatch end, Lines).

%% Used by `capture_debug_log/1`.
log(LogEvent, #{config := #{pid := Pid, ref := Ref}}) ->
    Pid ! {Ref, LogEvent},
    ok.
