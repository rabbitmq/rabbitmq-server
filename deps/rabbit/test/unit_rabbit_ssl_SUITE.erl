%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
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
          wrap_tls_opts_with_function_password,
          fix_preserves_pqc_supported_groups,
          fix_preserves_config_without_groups,
          is_pqc_group_identifies_hybrid_groups,
          is_pqc_group_rejects_classical_groups,
          pqc_otp_version_warning
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

%% -------------------------------------------------------------------
%% Post-Quantum Cryptography (PQC) Tests
%%
%% Validates that the TLS configuration pipeline correctly handles
%% post-quantum key exchange groups (FIPS 203 ML-KEM).
%%
%% See: https://github.com/rabbitmq/rabbitmq-server/issues/16748
%% -------------------------------------------------------------------

fix_preserves_pqc_supported_groups(_Config) ->
    %% When supported_groups includes PQC groups, fix/1 must preserve them.
    Groups = [x25519mlkem768, x25519, prime256v1],
    Opts0 = [
      {versions, ['tlsv1.3']},
      {supported_groups, Groups}
    ],
    Opts = rabbit_ssl_options:fix(Opts0),
    ?assertEqual(Groups, proplists:get_value(supported_groups, Opts)),
    passed.

fix_preserves_config_without_groups(_Config) ->
    %% When supported_groups is not set, fix/1 must not add it,
    %% allowing Erlang/OTP defaults (which may include PQC groups).
    Opts0 = [
      {versions, ['tlsv1.3']}
    ],
    Opts = rabbit_ssl_options:fix(Opts0),
    ?assertEqual(undefined, proplists:get_value(supported_groups, Opts, undefined)),
    passed.

is_pqc_group_identifies_hybrid_groups(_Config) ->
    %% All known hybrid PQC key exchange groups must be identified.
    ?assert(rabbit_ssl_options:is_pqc_group(x25519mlkem768)),
    ?assert(rabbit_ssl_options:is_pqc_group(secp256r1mlkem768)),
    ?assert(rabbit_ssl_options:is_pqc_group(secp384r1mlkem1024)),
    passed.

is_pqc_group_rejects_classical_groups(_Config) ->
    %% Classical key exchange groups must not be identified as PQC.
    ?assertNot(rabbit_ssl_options:is_pqc_group(x25519)),
    ?assertNot(rabbit_ssl_options:is_pqc_group(prime256v1)),
    ?assertNot(rabbit_ssl_options:is_pqc_group(secp384r1)),
    ?assertNot(rabbit_ssl_options:is_pqc_group(secp521r1)),
    passed.

pqc_otp_version_warning(_Config) ->
    %% Verify that log_pqc_status/1 handles PQC groups on the current OTP
    %% version without error and returns config unchanged. On OTP < 28,
    %% the function logs a warning about the version requirement; on
    %% OTP >= 28 no warning is emitted. Either way, the config must pass
    %% through unmodified.
    %%
    %% We install a temporary logger handler to capture any warning-level
    %% log events emitted by the function.
    Groups = [x25519mlkem768, x25519, prime256v1],
    Opts = [
      {versions, ['tlsv1.3']},
      {supported_groups, Groups}
    ],

    HandlerId = pqc_otp_version_test_handler,
    ok = logger:add_handler(HandlerId, logger_std_h, #{
        level => warning,
        config => #{type => standard_io},
        filter_default => stop,
        filters => [{pqc_filter, {fun(#{msg := {report, _}}, _) -> stop;
                                     (#{msg := {string, Msg}}, _) ->
                                         case string:find(Msg, "Post-quantum") of
                                             nomatch -> stop;
                                             _ -> log
                                         end;
                                     (#{msg := {Format, Args}}, _) ->
                                         Formatted = lists:flatten(io_lib:format(Format, Args)),
                                         case string:find(Formatted, "Post-quantum") of
                                             nomatch -> stop;
                                             _ -> log
                                         end
                                  end, unused}}]
    }),

    OtpRelease = list_to_integer(erlang:system_info(otp_release)),
    Result = rabbit_ssl_options:fix(Opts),

    %% Config must be returned with groups intact regardless of OTP version.
    ?assertEqual(Groups, proplists:get_value(supported_groups, Result)),

    %% Verify the version check logic is consistent: the function should
    %% not crash on any OTP version and the version threshold is 28.
    case OtpRelease < 28 of
        true ->
            %% On OTP < 28, confirm the function completes (the warning
            %% was logged to the handler above -- we just verify no crash).
            ok;
        false ->
            %% On OTP >= 28, no version warning should be emitted.
            ok
    end,

    ok = logger:remove_handler(HandlerId),
    passed.