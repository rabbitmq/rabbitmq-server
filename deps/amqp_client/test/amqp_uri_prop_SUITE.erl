%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(amqp_uri_prop_SUITE).

-include_lib("proper/include/proper.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-include("amqp_client.hrl").

-compile([export_all, nowarn_export_all]).

all() ->
    [
     auth_mechanism_no_new_atoms_prop,
     auth_mechanism_colon_separated_no_new_atoms_prop,
     verify_rejects_unknown_values_prop,
     fail_if_no_peer_cert_rejects_non_boolean_prop,
     valid_uri_options_still_work,
     auth_mechanism_maps_to_expected_fun
    ].

%% -------------------------------------------------------------------
%% Generators
%% -------------------------------------------------------------------

%% Generates a random mechanism string that cannot possibly be an
%% existing atom (prefixed to avoid collisions).
nonexistent_mechanism() ->
    ?LET(N, pos_integer(),
         "zzz_nonesuch_" ++ integer_to_list(N)).

%% Generates a random string that is NOT "true" or "false".
non_boolean_string() ->
    ?LET(N, pos_integer(),
         "not_a_bool_" ++ integer_to_list(N)).

%% Generates a random string that is not a valid `verify` value.
unknown_atom_string() ->
    ?LET(N, pos_integer(),
         "zzz_no_such_verify_" ++ integer_to_list(N)).

%% -------------------------------------------------------------------
%% Properties
%% -------------------------------------------------------------------

auth_mechanism_no_new_atoms_prop(_Config) ->
    Prop = ?FORALL(
        Token, nonexistent_mechanism(),
        begin
            Uri = "amqp://host/?auth_mechanism=" ++ Token,
            Result = amqp_uri:parse(Uri),
            NotAnAtom = try list_to_existing_atom(Token) of
                            _ -> false
                        catch
                            error:badarg -> true
                        end,
            IsError = case Result of
                          {error, _} -> true;
                          _          -> false
                      end,
            NotAnAtom andalso IsError
        end),
    ?assert(proper:quickcheck(Prop, [quiet, {numtests, 500}])).

auth_mechanism_colon_separated_no_new_atoms_prop(_Config) ->
    Prop = ?FORALL(
        {Mod, Fun}, {nonexistent_mechanism(), nonexistent_mechanism()},
        begin
            Token = Mod ++ ":" ++ Fun,
            Uri = "amqp://host/?auth_mechanism=" ++ Token,
            Result = amqp_uri:parse(Uri),
            ModNotAtom = try list_to_existing_atom(Mod) of
                             _ -> false
                         catch
                             error:badarg -> true
                         end,
            FunNotAtom = try list_to_existing_atom(Fun) of
                             _ -> false
                         catch
                             error:badarg -> true
                         end,
            IsError = case Result of
                          {error, _} -> true;
                          _          -> false
                      end,
            ModNotAtom andalso FunNotAtom andalso IsError
        end),
    ?assert(proper:quickcheck(Prop, [quiet, {numtests, 200}])).

verify_rejects_unknown_values_prop(_Config) ->
    Prop = ?FORALL(
        Val, unknown_atom_string(),
        begin
            Uri = "amqps://host/?verify=" ++ Val,
            case amqp_uri:parse(Uri) of
                {error, _} -> true;
                {ok, _}    -> false
            end
        end),
    ?assert(proper:quickcheck(Prop, [quiet, {numtests, 200}])).

fail_if_no_peer_cert_rejects_non_boolean_prop(_Config) ->
    Prop = ?FORALL(
        Val, non_boolean_string(),
        begin
            Uri = "amqps://host/?fail_if_no_peer_cert=" ++ Val,
            case amqp_uri:parse(Uri) of
                {error, _} -> true;
                {ok, _}    -> false
            end
        end),
    ?assert(proper:quickcheck(Prop, [quiet, {numtests, 200}])).

%% Validates that known-good values still parse correctly.
valid_uri_options_still_work(_Config) ->
    %% Known auth mechanisms
    lists:foreach(
      fun(Mech) ->
              Uri = "amqp://host/?auth_mechanism=" ++ Mech,
              ?assertMatch({ok, #amqp_params_network{}}, amqp_uri:parse(Uri))
      end,
      ["plain", "amqplain", "external", "crdemo"]),

    %% verify values
    ?assertMatch({ok, #amqp_params_network{}},
                 amqp_uri:parse("amqps://host/?verify=verify_peer")),
    ?assertMatch({ok, #amqp_params_network{}},
                 amqp_uri:parse("amqps://host/?verify=verify_none")),

    %% fail_if_no_peer_cert values
    ?assertMatch({ok, #amqp_params_network{}},
                 amqp_uri:parse("amqps://host/?fail_if_no_peer_cert=true")),
    ?assertMatch({ok, #amqp_params_network{}},
                 amqp_uri:parse("amqps://host/?fail_if_no_peer_cert=false")),

    %% Colon-separated module:function form
    ?assertMatch({ok, #amqp_params_network{}},
                 amqp_uri:parse("amqp://host/?auth_mechanism=amqp_auth_mechanisms:plain")),

    %% Unknown module or function in colon-separated form
    ?assertMatch({error, _},
                 amqp_uri:parse("amqp://host/?auth_mechanism=zzz_fake_mod:plain")),
    ?assertMatch({error, _},
                 amqp_uri:parse("amqp://host/?auth_mechanism=amqp_auth_mechanisms:zzz_fake_fn")),

    %% Only `amqp_auth_mechanisms` functions can be named in a URI. Every value
    %% below resolves to existing atoms.
    ?assertMatch({error, {{unknown_mechanism, _}, _}},
                 amqp_uri:parse("amqp://host/?auth_mechanism=erlang:send")),
    ?assertMatch({error, {{unknown_mechanism, _}, _}},
                 amqp_uri:parse("amqp://host/?auth_mechanism=application:set_env")),
    ?assertMatch({error, {{unknown_mechanism, _}, _}},
                 amqp_uri:parse("amqp://host/?auth_mechanism=amqp_uri:parse")),
    %% A single token that is an existing atom but not a mechanism must fail at
    %% parse time, not with `undef` when the connection is established.
    ?assertMatch({error, {{unknown_mechanism, _}, _}},
                 amqp_uri:parse("amqp://host/?auth_mechanism=module_info")),
    %% Malformed separators.
    ?assertMatch({error, _}, amqp_uri:parse("amqp://host/?auth_mechanism=plain:")),
    ?assertMatch({error, _}, amqp_uri:parse("amqp://host/?auth_mechanism=:plain")),
    ?assertMatch({error, _},
                 amqp_uri:parse("amqp://host/?auth_mechanism=amqp_auth_mechanisms:plain:x")),
    ok.

%% Confirms the mechanism table maps each name to the expected fun, so a
%% future edit cannot silently map a name to the wrong mechanism.
auth_mechanism_maps_to_expected_fun(_Config) ->
    lists:foreach(
      fun({Mech, Expected}) ->
              Uri = "amqp://host/?auth_mechanism=" ++ Mech,
              {ok, #amqp_params_network{auth_mechanisms = [Fun]}} = amqp_uri:parse(Uri),
              ?assertEqual(Expected, Fun)
      end,
      [{"plain",                         fun amqp_auth_mechanisms:plain/3},
       {"amqplain",                      fun amqp_auth_mechanisms:amqplain/3},
       {"external",                      fun amqp_auth_mechanisms:external/3},
       {"crdemo",                        fun amqp_auth_mechanisms:crdemo/3},
       {"amqp_auth_mechanisms:external", fun amqp_auth_mechanisms:external/3}]),
    %% The default when no `auth_mechanism` is given.
    {ok, #amqp_params_network{auth_mechanisms = DefaultMechs}} =
        amqp_uri:parse("amqp://host/"),
    ?assertEqual([fun amqp_auth_mechanisms:plain/3,
                  fun amqp_auth_mechanisms:amqplain/3],
                 DefaultMechs),
    ok.
