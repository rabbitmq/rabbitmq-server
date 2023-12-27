%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.
%%

-module(rabbit_auth_cache).

-export([expiration/1, expired/1]).

-callback get(term()) -> term().

-callback put(term(), term(), integer()) -> ok.

-callback delete(term()) -> ok.

expiration(TTL) ->
    erlang:system_time(milli_seconds) + TTL.

expired(Exp) ->
    erlang:system_time(milli_seconds) > Exp.
