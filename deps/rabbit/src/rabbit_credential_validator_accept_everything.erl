%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_credential_validator_accept_everything).

-behaviour(rabbit_credential_validator).

%%
%% API
%%

-export([validate/2]).

-spec validate(rabbit_types:username(), rabbit_types:password()) -> 'ok' | {'error', string()}.

validate(_Username, _Password) ->
    ok.
