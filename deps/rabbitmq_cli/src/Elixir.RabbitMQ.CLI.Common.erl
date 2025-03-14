%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module('Elixir.RabbitMQ.CLI.Common').

-include_lib("rabbit_common/include/rabbit.hrl").

-export([internal_user/0]).

internal_user() ->
    ?INTERNAL_USER.
