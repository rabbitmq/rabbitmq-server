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
     valid_uri_options_still_work
    ].

%% -------------------------------------------------------------------
%% Setup / teardown
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    %% Ensure relevant modules are loaded so that their atoms
    %% (function names, SSL option values) exist in the atom table.
    {module, _} = code:ensure_loaded(amqp_auth_mechanisms),
    {module, _} = code:ensure_loaded(ssl),
    Config.

end_per_suite(_Config) ->
    ok.

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

%% Generates a random string unlikely to be an existing atom.
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
    ok.
