%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_password_hashing_pbkdf2_sha256).

-behaviour(rabbit_password_hashing).

-export([hash/1, salt_length/0]).

%% OWASP's 2023 minimum recommendation for PBKDF2-HMAC-SHA256.
%%
%% Do not change this value in place. The iteration count is not stored
%% with the hash, so every existing PBKDF2 user would be locked out.
%% Introduce a new module with the new value instead.
-define(ITERATIONS, 210_000).
-define(KEY_LENGTH, 32).
-define(SALT_LENGTH, 16).

hash(<<Salt:?SALT_LENGTH/binary, Cleartext/binary>>) ->
    crypto:pbkdf2_hmac(sha256, Cleartext, Salt, ?ITERATIONS, ?KEY_LENGTH).

salt_length() ->
    ?SALT_LENGTH.
