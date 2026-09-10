%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_prelaunch_conf_SUITE).

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
    [is_sensitive_var_matches_by_name,
     log_app_env_var_masks_sensitive_top_level_value,
     log_app_env_var_redacts_nested_password_only,
     log_app_env_var_handles_non_atom_nested_keys,
     log_app_env_var_redacts_binary_key,
     log_app_env_var_redacts_multi_level_nesting,
     log_app_env_var_redacts_nested_map,
     log_app_env_var_redacts_wide_tuple,
     log_app_env_var_redacts_nested_secret_in_non_sensitive_wide_tuple,
     log_app_env_var_passes_through_plain_value].

groups() ->
    [{tests, [], all_tests()}].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

%%%===================================================================
%%% Test cases
%%%===================================================================

is_sensitive_var_matches_by_name(_Config) ->
    ?assert(rabbit_prelaunch_conf:is_sensitive_var(password)),
    ?assert(rabbit_prelaunch_conf:is_sensitive_var(default_pass)),
    ?assert(rabbit_prelaunch_conf:is_sensitive_var(anonymous_login_pass)),
    ?assert(rabbit_prelaunch_conf:is_sensitive_var('config_entry_decoder.passphrase')),
    ?assert(rabbit_prelaunch_conf:is_sensitive_var(client_secret)),
    ?assert(rabbit_prelaunch_conf:is_sensitive_var(auth_token)),
    ?assert(rabbit_prelaunch_conf:is_sensitive_var(<<"Password">>)),
    ?assertNot(rabbit_prelaunch_conf:is_sensitive_var(certfile)),
    ?assertNot(rabbit_prelaunch_conf:is_sensitive_var(ssl_options)),
    ?assertNot(rabbit_prelaunch_conf:is_sensitive_var(routing_key)),
    %% Must not flag config-about-secrets keys as sensitive.
    ?assertNot(rabbit_prelaunch_conf:is_sensitive_var(password_hashing_module)),
    ?assertNot(rabbit_prelaunch_conf:is_sensitive_var(bypass_pem_cache)),
    ?assertNot(rabbit_prelaunch_conf:is_sensitive_var(token_endpoint)),
    ?assertNot(rabbit_prelaunch_conf:is_sensitive_var(token_path)),
    ?assertNot(rabbit_prelaunch_conf:is_sensitive_var(oauth_token_endpoint_params)).

%% Any value shape must be masked, not just the atom `password`.
log_app_env_var_masks_sensitive_top_level_value(_Config) ->
    Secret = list_to_binary("pass-secret-" ++ random_string()),
    Lines = capture_debug_log(
              fun() ->
                      rabbit_prelaunch_conf:log_app_env_var(default_pass, Secret)
              end),
    ?assertNot(log_contains(Lines, binary_to_list(Secret))),
    ?assert(log_contains(Lines, "********")).

%% Only the sensitive nested entry is redacted; siblings survive.
log_app_env_var_redacts_nested_password_only(_Config) ->
    Secret = "pass-secret-" ++ random_string(),
    PlainValue = "plain-value-" ++ random_string(),
    Lines = capture_debug_log(
              fun() ->
                      rabbit_prelaunch_conf:log_app_env_var(
                        ssl_options,
                        [{password, Secret}, {certfile, PlainValue}])
              end),
    ?assertNot(log_contains(Lines, Secret)),
    ?assert(log_contains(Lines, "********")),
    ?assert(log_contains(Lines, PlainValue)).

%% Non-atom nested keys must not crash.
log_app_env_var_handles_non_atom_nested_keys(_Config) ->
    ?assertEqual(
       ok,
       rabbit_prelaunch_conf:log_app_env_var(
         tcp_listen_options,
         [{1234, "value"}, {<<"k">>, "v"}, {[a, b], "v"}])).

%% Nested keys that are binaries (not just atoms) are matched too.
log_app_env_var_redacts_binary_key(_Config) ->
    Secret = "pass-secret-" ++ random_string(),
    Lines = capture_debug_log(
              fun() ->
                      rabbit_prelaunch_conf:log_app_env_var(
                        ssl_options, [{<<"password">>, Secret}])
              end),
    ?assertNot(log_contains(Lines, Secret)),
    ?assert(log_contains(Lines, "********")).

%% Sensitive keys nested two levels deep must be redacted too.
log_app_env_var_redacts_multi_level_nesting(_Config) ->
    Secret = "pass-secret-" ++ random_string(),
    PlainValue = "plain-value-" ++ random_string(),
    Lines = capture_debug_log(
              fun() ->
                      rabbit_prelaunch_conf:log_app_env_var(
                        listeners,
                        [{tcp, [{ssl_options,
                                 [{password, Secret},
                                  {certfile, PlainValue}]}]}])
              end),
    ?assertNot(log_contains(Lines, Secret)),
    ?assert(log_contains(Lines, "********")),
    ?assert(log_contains(Lines, PlainValue)).

%% Map-shaped values must be recursed into too.
log_app_env_var_redacts_nested_map(_Config) ->
    Secret = "pass-secret-" ++ random_string(),
    Lines = capture_debug_log(
              fun() ->
                      rabbit_prelaunch_conf:log_app_env_var(
                        key_config, #{ssl_options => #{password => Secret}})
              end),
    ?assertNot(log_contains(Lines, Secret)),
    ?assert(log_contains(Lines, "********")).

%% Sensitive keys in wider tuples must still be redacted.
log_app_env_var_redacts_wide_tuple(_Config) ->
    Secret = "pass-secret-" ++ random_string(),
    Lines = capture_debug_log(
              fun() ->
                      rabbit_prelaunch_conf:log_app_env_var(
                        ssl_options, [{password, Secret, extra}])
              end),
    ?assertNot(log_contains(Lines, Secret)),
    ?assert(log_contains(Lines, "********")).

%% Non-sensitive wide tuples must still recurse.
log_app_env_var_redacts_nested_secret_in_non_sensitive_wide_tuple(_Config) ->
    Secret = "pass-secret-" ++ random_string(),
    Lines = capture_debug_log(
              fun() ->
                      rabbit_prelaunch_conf:log_app_env_var(
                        listeners,
                        [{listener, "tcp", [{password, Secret}]}])
              end),
    ?assertNot(log_contains(Lines, Secret)),
    ?assert(log_contains(Lines, "********")).

log_app_env_var_passes_through_plain_value(_Config) ->
    PlainValue = "plain-value-" ++ random_string(),
    Lines = capture_debug_log(
              fun() ->
                      rabbit_prelaunch_conf:log_app_env_var(
                        default_vhost, PlainValue)
              end),
    ?assert(log_contains(Lines, PlainValue)).

%%%===================================================================
%%% Helpers
%%%===================================================================

random_string() -> integer_to_list(rand:uniform(50000)).

%% Captures `?LOG_DEBUG` output via a temporary logger handler.
capture_debug_log(Fun) ->
    Ref = make_ref(),
    HandlerId = list_to_atom(
                  "rabbit_prelaunch_conf_SUITE_log_capture_" ++
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
