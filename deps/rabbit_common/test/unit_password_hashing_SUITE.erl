%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(unit_password_hashing_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").

-behaviour(rabbit_password_hashing).

-export([hash/1]).

all() -> [password_hashing, salt_length, pbkdf2_sha256_hash_and_verify,
          pbkdf2_sha256_salt_length, generate_salt_honours_configured_module,
          hash_salt_length_and_salted_hash_property,
          hash_differs_for_distinct_passwords_property].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown
%% -------------------------------------------------------------------

init_per_suite(Config) -> Config.
end_per_suite(Config) -> Config.

init_per_group(_Group, Config) -> Config.
end_per_group(_Group, Config) -> Config.

init_per_testcase(_Testcase, Config) ->
    [{previous_hashing_module,
      application:get_env(rabbit, password_hashing_module)} | Config].

end_per_testcase(_Testcase, Config) ->
    case proplists:get_value(previous_hashing_module, Config) of
        {ok, Mod} ->
            application:set_env(rabbit, password_hashing_module, Mod);
        undefined ->
            application:unset_env(rabbit, password_hashing_module)
    end,
    Config.

hash(Cleartext) ->
    rabbit_password_hashing_sha256:hash(Cleartext).

%% ---------------------------------------------------------------------------
%% Test Cases
%% ---------------------------------------------------------------------------

password_hashing(_Config) ->
    rabbit_password_hashing_sha256 = rabbit_password:hashing_mod(),
    application:set_env(rabbit, password_hashing_module,
                        rabbit_password_hashing_md5),
    rabbit_password_hashing_md5    = rabbit_password:hashing_mod(),
    application:set_env(rabbit, password_hashing_module,
                        rabbit_password_hashing_sha256),
    rabbit_password_hashing_sha256 = rabbit_password:hashing_mod(),

    rabbit_password_hashing_sha256 =
        rabbit_password:hashing_mod(rabbit_password_hashing_sha256),
    rabbit_password_hashing_md5    =
        rabbit_password:hashing_mod(rabbit_password_hashing_md5),
    rabbit_password_hashing_md5    =
        rabbit_password:hashing_mod(undefined),

    passed.

salt_length(_Config) ->
    ?assertEqual(4, rabbit_password:salt_length(rabbit_password_hashing_sha256)),
    ?assertEqual(4, rabbit_password:salt_length(rabbit_password_hashing_sha512)),
    ?assertEqual(4, rabbit_password:salt_length(rabbit_password_hashing_md5)),
    ?assertEqual(4, rabbit_password:salt_length(?MODULE)),
    ?assertEqual(16, rabbit_password:salt_length(rabbit_password_hashing_pbkdf2_sha256)),
    passed.

pbkdf2_sha256_salt_length(_Config) ->
    ?assertEqual(16, rabbit_password_hashing_pbkdf2_sha256:salt_length()),
    passed.

pbkdf2_sha256_hash_and_verify(_Config) ->
    application:set_env(rabbit, password_hashing_module,
                        rabbit_password_hashing_pbkdf2_sha256),
    Password = <<"correct horse battery staple">>,
    SaltLength = rabbit_password:salt_length(rabbit_password_hashing_pbkdf2_sha256),
    <<Salt:SaltLength/binary, Hash/binary>> = rabbit_password:hash(Password),
    ?assertEqual(Hash, rabbit_password:salted_hash(
                          rabbit_password_hashing_pbkdf2_sha256, Salt, Password)),
    ?assertNotEqual(Hash, rabbit_password:salted_hash(
                             rabbit_password_hashing_pbkdf2_sha256, Salt,
                             <<"wrong password">>)),
    ?assertEqual(Hash, crypto:pbkdf2_hmac(sha256, Password, Salt, 210000, 32)),
    passed.

generate_salt_honours_configured_module(_Config) ->
    application:set_env(rabbit, password_hashing_module,
                        rabbit_password_hashing_pbkdf2_sha256),
    ?assertEqual(16, byte_size(rabbit_password:generate_salt())),
    application:set_env(rabbit, password_hashing_module,
                        rabbit_password_hashing_sha256),
    ?assertEqual(4, byte_size(rabbit_password:generate_salt())),
    passed.

proper_opts(NumTests) ->
    [{numtests, NumTests},
     {on_output, fun(".", _) -> ok;
                    (F, A) -> ct:pal(?LOW_IMPORTANCE, F, A)
                 end}].

hashing_mod_gen() ->
    frequency([{5, rabbit_password_hashing_sha256},
               {5, rabbit_password_hashing_sha512},
               {5, rabbit_password_hashing_md5},
               {1, rabbit_password_hashing_pbkdf2_sha256}]).

hash_salt_length_and_salted_hash_property(_Config) ->
    ?assert(
       proper:quickcheck(
         ?FORALL({Mod, Password}, {hashing_mod_gen(), non_empty(binary())},
                 begin
                     SaltLength = rabbit_password:salt_length(Mod),
                     <<Salt:SaltLength/binary, Hash/binary>> = rabbit_password:hash(Mod, Password),
                     Hash =:= rabbit_password:salted_hash(Mod, Salt, Password)
                 end),
         proper_opts(20))).

hash_differs_for_distinct_passwords_property(_Config) ->
    ?assert(
       proper:quickcheck(
         ?FORALL({Mod, Password1, Password2},
                 {hashing_mod_gen(), non_empty(binary()), non_empty(binary())},
                 ?IMPLIES(Password1 =/= Password2,
                          begin
                              Salt = rabbit_password:generate_salt(Mod),
                              rabbit_password:salted_hash(Mod, Salt, Password1) =/=
                                  rabbit_password:salted_hash(Mod, Salt, Password2)
                          end)),
         proper_opts(20))).
