%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

%% Stands in for a plugin-provided `password_hashing_module`: modules other
%% than the built-in `rabbit_password_hashing_*` ones must be accepted too.
-module(dummy_password_hashing).
-behaviour(rabbit_password_hashing).

-export([hash/1]).

hash(Cleartext) ->
    crypto:hash(sha256, Cleartext).
