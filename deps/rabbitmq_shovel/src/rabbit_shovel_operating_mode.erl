%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_shovel_operating_mode).

-include("rabbit_shovel.hrl").

-export([
  operating_mode/0
]).

-type operating_mode() :: 'standard' | atom().

%%
%% API
%%

-spec operating_mode() -> operating_mode().
operating_mode() ->
    rabbit_data_coercion:to_atom(application:get_env(?SHOVEL_APP, operating_mode, standard)).
