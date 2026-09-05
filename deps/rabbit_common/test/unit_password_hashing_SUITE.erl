%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(unit_password_hashing_SUITE).

-compile(export_all).

-include_lib("eunit/include/eunit.hrl").

all() -> [password_hashing, pbkdf2_sha256_hash_and_verify].

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

pbkdf2_sha256_hash_and_verify(_Config) ->
    application:set_env(rabbit, password_hashing_module,
                        rabbit_password_hashing_pbkdf2_sha256),
    Password = <<"correct horse battery staple">>,
    <<Salt:4/binary, Hash/binary>> = rabbit_password:hash(Password),
    ?assertEqual(Hash, rabbit_password:salted_hash(
                          rabbit_password_hashing_pbkdf2_sha256, Salt, Password)),
    ?assertNotEqual(Hash, rabbit_password:salted_hash(
                             rabbit_password_hashing_pbkdf2_sha256, Salt,
                             <<"wrong password">>)),
    passed.
