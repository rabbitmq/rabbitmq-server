%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_password).

-define(DEFAULT_HASHING_MODULE, rabbit_password_hashing_sha256).
-define(DEFAULT_SALT_LENGTH, 4).

%%
%% API
%%

-export([hash/1, hash/2, generate_salt/0, generate_salt/1, salt_length/1,
         salted_hash/2, salted_hash/3, hashing_mod/0, hashing_mod/1]).

hash(Cleartext) ->
    hash(hashing_mod(), Cleartext).

hash(HashingMod, Cleartext) ->
    SaltBin = generate_salt(HashingMod),
    Hash = salted_hash(HashingMod, SaltBin, Cleartext),
    <<SaltBin/binary, Hash/binary>>.

generate_salt() ->
    generate_salt(hashing_mod()).

generate_salt(HashingMod) ->
    crypto:strong_rand_bytes(salt_length(HashingMod)).

salt_length(Mod) ->
    _ = code:ensure_loaded(Mod),
    case erlang:function_exported(Mod, salt_length, 0) of
        true  -> Mod:salt_length();
        false -> ?DEFAULT_SALT_LENGTH
    end.

salted_hash(Salt, Cleartext) ->
    salted_hash(hashing_mod(), Salt, Cleartext).

salted_hash(Mod, Salt, Cleartext) ->
    Fun = fun Mod:hash/1,
    Fun(<<Salt/binary, Cleartext/binary>>).

hashing_mod() ->
    rabbit_misc:get_env(rabbit, password_hashing_module,
        ?DEFAULT_HASHING_MODULE).

hashing_mod(rabbit_password_hashing_sha256) ->
    rabbit_password_hashing_sha256;
hashing_mod(rabbit_password_hashing_md5) ->
    rabbit_password_hashing_md5;
%% fall back to the hashing function that's been used prior to 3.6.0
hashing_mod(undefined) ->
    rabbit_password_hashing_md5;
%% if a custom module is configured, simply use it
hashing_mod(CustomMod) when is_atom(CustomMod) ->
    CustomMod.
