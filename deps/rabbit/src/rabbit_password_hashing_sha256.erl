%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_password_hashing_sha256).

-behaviour(rabbit_password_hashing).

-export([hash/1]).

hash(Binary) ->
    crypto:hash(sha256, Binary).
