%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(http_provider_tls_prop_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("proper/include/proper.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

all() ->
    [{group, unit},
     {group, prop}].

groups() ->
    [{unit, [parallel],
      [sslv3_filtered_from_versions,
       other_versions_preserved,
       hibernate_after_added_when_absent,
       hibernate_after_preserved_when_set,
       cacertfile_prevents_cacerts_injection,
       cacerts_preserved_when_set,
       user_options_preserved]},
     {prop, [parallel],
      [prop_sslv3_never_present,
       prop_hibernate_after_always_set,
       prop_cacertfile_prevents_cacerts_injection,
       prop_user_options_preserved]}].

suite() ->
    [{timetrap, {seconds, 60}}].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(ssl),
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

sslv3_filtered_from_versions(_Config) ->
    Opts = prepare([{versions, [sslv3, 'tlsv1.2', 'tlsv1.3']}]),
    Versions = opt(versions, Opts),
    false = lists:member(sslv3, Versions),
    true  = lists:member('tlsv1.2', Versions),
    true  = lists:member('tlsv1.3', Versions).

other_versions_preserved(_Config) ->
    Opts = prepare([{versions, ['tlsv1.2', 'tlsv1.3']}]),
    ['tlsv1.2', 'tlsv1.3'] = opt(versions, Opts).

hibernate_after_added_when_absent(_Config) ->
    Opts = prepare([]),
    6000 = opt(hibernate_after, Opts).

hibernate_after_preserved_when_set(_Config) ->
    Opts = prepare([{hibernate_after, 12000}]),
    12000 = opt(hibernate_after, Opts).

cacertfile_prevents_cacerts_injection(_Config) ->
    Opts = prepare([{cacertfile, "/path/to/ca.pem"}]),
    undefined = opt(cacerts, Opts).

cacerts_preserved_when_set(_Config) ->
    MyCaCerts = [<<1, 2, 3>>],
    Opts = prepare([{cacerts, MyCaCerts}]),
    MyCaCerts = opt(cacerts, Opts).

user_options_preserved(_Config) ->
    Input = [{certfile, "/path/to/cert.pem"},
             {keyfile, "/path/to/key.pem"},
             {versions, ['tlsv1.2', 'tlsv1.3']}],
    Opts = prepare(Input),
    "/path/to/cert.pem" = opt(certfile, Opts),
    "/path/to/key.pem"  = opt(keyfile, Opts),
    ['tlsv1.2', 'tlsv1.3'] = opt(versions, Opts).

%% -------------------------------------------------------------------
%% Property-based tests
%% -------------------------------------------------------------------

prop_sslv3_never_present(_Config) ->
    run_proper(
      fun() ->
              ?FORALL(
                 UserOpts, ssl_options(),
                 not lists:member(sslv3, proplists:get_value(versions, prepare(UserOpts), [])))
      end).

prop_hibernate_after_always_set(_Config) ->
    run_proper(
      fun() ->
              ?FORALL(
                 UserOpts, ssl_options(),
                 is_integer(opt(hibernate_after, prepare(UserOpts))))
      end).

prop_cacertfile_prevents_cacerts_injection(_Config) ->
    run_proper(
      fun() ->
              ?FORALL(
                 BaseOpts, ssl_options_without_ca(),
                 undefined =:= opt(cacerts, prepare([{cacertfile, "/path/to/ca.pem"} | BaseOpts])))
      end).

prop_user_options_preserved(_Config) ->
    run_proper(
      fun() ->
              ?FORALL(
                 UserOpts, ssl_options(),
                 begin
                     Result = prepare(UserOpts),
                     %% Keys that fix_client may normalise or supplement.
                     Normalised = [versions, hibernate_after, cacerts],
                     lists:all(
                       fun({K, V}) ->
                               lists:member(K, Normalised) orelse
                                   opt(K, Result) =:= V
                       end, UserOpts)
                 end)
      end).

%% -------------------------------------------------------------------
%% Generators
%% -------------------------------------------------------------------

ssl_options() ->
    ?LET(Opts, list(ssl_option()),
         unique_keys(Opts)).

ssl_options_without_ca() ->
    ?LET(Opts, ssl_options(),
         lists:filter(fun({K, _}) ->
                              K =/= cacerts andalso K =/= cacertfile
                      end, Opts)).

ssl_option() ->
    oneof([
        {versions, list(oneof(['tlsv1.2', 'tlsv1.3']))},
        {hibernate_after, range(1000, 60000)},
        {certfile, binary()},
        {keyfile, binary()},
        {cacerts, list(binary())},
        {cacertfile, binary()},
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

prepare(Opts) ->
    rabbit_trust_store_http_provider:prepare_ssl_opts(Opts).

opt(Key, Options) ->
    proplists:get_value(Key, Options).

run_proper(Fun) ->
    ?assert(proper:counterexample(
              Fun(),
              [{numtests, 100},
               {on_output, fun(".", _) -> ok;
                              (F, A) -> ct:pal(?LOW_IMPORTANCE, F, A)
                           end}])).
