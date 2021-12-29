%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_auth_cache).

-export([expiration/1, expired/1]).

-ifdef(use_specs).

-callback get(term()) -> term().

-callback put(term(), term(), integer()) -> ok.

-callback delete(term()) -> ok.

-else.

-export([behaviour_info/1]).

behaviour_info(callbacks) ->
    [{get, 1}, {put, 3}, {delete, 1}];
behaviour_info(_Other) ->
    undefined.

-endif.

expiration(TTL) ->
    erlang:system_time(milli_seconds) + TTL.

expired(Exp) ->
    erlang:system_time(milli_seconds) > Exp.
