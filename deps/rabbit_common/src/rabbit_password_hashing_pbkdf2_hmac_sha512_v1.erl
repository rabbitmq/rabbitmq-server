%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_password_hashing_pbkdf2_hmac_sha512_v1).

%% TODO: I don't know if I should extend this behaviour, or change the other
%% implementations to do salt prefixing themselves.
%% -behaviour(rabbit_password_hashing).

-export([hash/2]).

%% OWASP-recommended iteration count, as of 2024.
-define(ITERATIONS, 210000).
-define(KEYLEN, 64).

hash(Salt, Binary) ->
    crypto:pbkdf2_hmac(sha512, Binary, Salt, ?ITERATIONS, ?KEYLEN).
