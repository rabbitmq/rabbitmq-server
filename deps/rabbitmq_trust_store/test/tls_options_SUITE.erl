%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(tls_options_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("proper/include/proper.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

all() ->
    [{group, unit},
     {group, prop}].

groups() ->
    [{unit, [parallel],
      [fail_if_no_peer_cert_defaults_to_true,
       fail_if_no_peer_cert_false_is_respected,
       fail_if_no_peer_cert_true_is_respected,
       verify_always_set_to_verify_peer,
       verify_fun_always_set,
       verify_fun_overrides_user_value,
       partial_chain_always_set,
       cacerts_added_when_missing,
       cacerts_not_added_when_present,
       cacertfile_prevents_cacerts_default,
       both_cacerts_and_cacertfile_preserved,
       user_options_are_preserved]},
     {prop, [parallel],
      [prop_verify_always_verify_peer,
       prop_fail_if_no_peer_cert_default,
       prop_fail_if_no_peer_cert_respected,
       prop_cacerts_default_when_no_ca_opts,
       prop_user_options_preserved]}].

suite() ->
    [{timetrap, {seconds, 60}}].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(_GroupName, Config) ->
    Config.

end_per_group(_GroupName, Config) ->
    Config.

init_per_testcase(_Case, Config) ->
    Config.

end_per_testcase(_Case, _Config) ->
    ok.

%% -------------------------------------------------------------------
%% Unit tests
%% -------------------------------------------------------------------

fail_if_no_peer_cert_defaults_to_true(_Config) ->
    Opts = merge([]),
    true = opt(fail_if_no_peer_cert, Opts).

fail_if_no_peer_cert_false_is_respected(_Config) ->
    Opts = merge([{fail_if_no_peer_cert, false}]),
    false = opt(fail_if_no_peer_cert, Opts).

fail_if_no_peer_cert_true_is_respected(_Config) ->
    Opts = merge([{fail_if_no_peer_cert, true}]),
    true = opt(fail_if_no_peer_cert, Opts).

verify_always_set_to_verify_peer(_Config) ->
    verify_peer = opt(verify, merge([])),
    verify_peer = opt(verify, merge([{verify, verify_none}])),
    verify_peer = opt(verify, merge([{verify, verify_peer}])).

verify_fun_always_set(_Config) ->
    Opts = merge([]),
    {Fun, continue} = opt(verify_fun, Opts),
    true = is_function(Fun, 3).

verify_fun_overrides_user_value(_Config) ->
    UserFun = fun(_Cert, _Event, State) -> {valid, State} end,
    Opts = merge([{verify_fun, {UserFun, some_state}}]),
    {Fun, continue} = opt(verify_fun, Opts),
    true = (Fun =/= UserFun).

partial_chain_always_set(_Config) ->
    Opts = merge([]),
    Fun = opt(partial_chain, Opts),
    true = is_function(Fun, 1).

cacerts_added_when_missing(_Config) ->
    Opts = merge([]),
    [] = opt(cacerts, Opts).

cacerts_not_added_when_present(_Config) ->
    MyCaCerts = [<<1, 2, 3>>],
    Opts = merge([{cacerts, MyCaCerts}]),
    MyCaCerts = opt(cacerts, Opts).

cacertfile_prevents_cacerts_default(_Config) ->
    Opts = merge([{cacertfile, "/path/to/ca.pem"}]),
    "/path/to/ca.pem" = opt(cacertfile, Opts),
    undefined = opt(cacerts, Opts).

both_cacerts_and_cacertfile_preserved(_Config) ->
    MyCaCerts = [<<1, 2, 3>>],
    Opts = merge([{cacerts, MyCaCerts}, {cacertfile, "/path/to/ca.pem"}]),
    MyCaCerts = opt(cacerts, Opts),
    "/path/to/ca.pem" = opt(cacertfile, Opts).

user_options_are_preserved(_Config) ->
    Input = [{certfile, "/path/to/cert.pem"},
             {keyfile, "/path/to/key.pem"},
             {versions, ['tlsv1.2', 'tlsv1.3']}],
    Opts = merge(Input),
    "/path/to/cert.pem" = opt(certfile, Opts),
    "/path/to/key.pem" = opt(keyfile, Opts),
    ['tlsv1.2', 'tlsv1.3'] = opt(versions, Opts).

%% -------------------------------------------------------------------
%% Property-based tests
%% -------------------------------------------------------------------

prop_verify_always_verify_peer(_Config) ->
    run_proper(
      fun() ->
              ?FORALL(
                 UserOpts, tls_options(),
                 verify_peer =:= opt(verify, merge(UserOpts)))
      end).

prop_fail_if_no_peer_cert_default(_Config) ->
    run_proper(
      fun() ->
              ?FORALL(
                 UserOpts, tls_options_without(fail_if_no_peer_cert),
                 true =:= opt(fail_if_no_peer_cert, merge(UserOpts)))
      end).

prop_fail_if_no_peer_cert_respected(_Config) ->
    run_proper(
      fun() ->
              ?FORALL(
                 {Val, BaseOpts}, {boolean(), tls_options_without(fail_if_no_peer_cert)},
                 Val =:= opt(fail_if_no_peer_cert,
                             merge([{fail_if_no_peer_cert, Val} | BaseOpts])))
      end).

prop_cacerts_default_when_no_ca_opts(_Config) ->
    run_proper(
      fun() ->
              ?FORALL(
                 UserOpts, tls_options_without_ca(),
                 [] =:= opt(cacerts, merge(UserOpts)))
      end).

prop_user_options_preserved(_Config) ->
    run_proper(
      fun() ->
              ?FORALL(
                 UserOpts, tls_options(),
                 begin
                     Merged = merge(UserOpts),
                     %% Every user option that is not overridden by the
                     %% plugin must appear in the result.
                     Overridden = [verify, verify_fun, partial_chain],
                     lists:all(
                       fun({K, V}) ->
                               lists:member(K, Overridden) orelse
                                   opt(K, Merged) =:= V
                       end, UserOpts)
                 end)
      end).

%% -------------------------------------------------------------------
%% Generators
%% -------------------------------------------------------------------

tls_options() ->
    ?LET(Opts, list(tls_option()),
         unique_keys(Opts)).

tls_options_without(Key) ->
    ?LET(Opts, tls_options(),
         lists:keydelete(Key, 1, Opts)).

tls_options_without_ca() ->
    ?LET(Opts, tls_options(),
         lists:filter(fun({K, _}) ->
                              K =/= cacerts andalso K =/= cacertfile
                      end, Opts)).

tls_option() ->
    oneof([
        {verify, oneof([verify_peer, verify_none])},
        {fail_if_no_peer_cert, boolean()},
        {depth, range(0, 10)},
        {certfile, binary()},
        {keyfile, binary()},
        {cacerts, list(binary())},
        {cacertfile, binary()},
        {versions, list(oneof(['tlsv1.2', 'tlsv1.3']))},
        {ciphers, list(binary())}
    ]).

unique_keys(Opts) ->
    lists:foldl(fun({K, _} = Opt, Acc) ->
                        case lists:keymember(K, 1, Acc) of
                            true  -> Acc;
                            false -> [Opt | Acc]
                        end
                end, [], Opts).

%% -------------------------------------------------------------------
%% Helpers
%% -------------------------------------------------------------------

merge(Options) ->
    rabbit_trust_store_app:merge_tls_options(Options).

opt(Key, Options) ->
    proplists:get_value(Key, Options).

run_proper(Fun) ->
    ?assert(proper:counterexample(
              Fun(),
              [{numtests, 100},
               {on_output, fun(".", _) -> ok;
                              (F, A) -> ct:pal(?LOW_IMPORTANCE, F, A)
                           end}])).
