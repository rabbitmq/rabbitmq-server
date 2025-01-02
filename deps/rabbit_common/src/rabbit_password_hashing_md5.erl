%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

%% Legacy hashing implementation, only used as a last resort when
%% #internal_user.hashing_algorithm is md5 or undefined (the case in
%% pre-3.6.0 user records).

-module(rabbit_password_hashing_md5).

-behaviour(rabbit_password_hashing).

-export([hash/1]).

hash(Binary) ->
    erlang:md5(Binary).
