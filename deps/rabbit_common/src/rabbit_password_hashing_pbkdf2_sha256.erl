%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_password_hashing_pbkdf2_sha256).

-behaviour(rabbit_password_hashing).

-export([hash/1]).

%% OWASP's 2023 minimum recommendation for PBKDF2-HMAC-SHA256.
-define(ITERATIONS, 210_000).
-define(KEY_LENGTH, 32).

%% rabbit_password:salted_hash/3 always calls this with
%% <<Salt:4/binary, Cleartext/binary>>; split it back apart since
%% PBKDF2 takes the salt as its own argument, not a data prefix.
hash(<<Salt:4/binary, Cleartext/binary>>) ->
    crypto:pbkdf2_hmac(sha256, Cleartext, Salt, ?ITERATIONS, ?KEY_LENGTH).
