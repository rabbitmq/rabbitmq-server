%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at https://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_password).
-include("rabbit.hrl").

-define(DEFAULT_HASHING_MODULE, rabbit_password_hashing_sha256).

%%
%% API
%%

-export([hash/1, hash/2, generate_salt/0, salted_hash/2, salted_hash/3,
         hashing_mod/0, hashing_mod/1]).

hash(Cleartext) ->
    hash(hashing_mod(), Cleartext).

hash(HashingMod, Cleartext) ->
    SaltBin = generate_salt(),
    Hash = salted_hash(HashingMod, SaltBin, Cleartext),
    <<SaltBin/binary, Hash/binary>>.

generate_salt() ->
    Salt = rand:uniform(16#ffffffff),
    <<Salt:32>>.

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
