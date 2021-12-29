%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_mgmt_load_definitions).

-include_lib("rabbit_common/include/rabbit.hrl").

-export([boot/0, maybe_load_definitions/0, maybe_load_definitions_from/2]).

%% This module exists for backwards compatibility only.
%% Definition import functionality is now a core server feature.

boot() ->
    rabbit_log:debug("Will import definitions file from management.load_definitions"),
    rabbit_definitions:maybe_load_definitions(rabbitmq_management, load_definitions).

maybe_load_definitions() ->
    rabbit_definitions:maybe_load_definitions().

maybe_load_definitions_from(true, Dir) ->
    rabbit_definitions:maybe_load_definitions_from(true, Dir);
maybe_load_definitions_from(false, File) ->
    rabbit_definitions:maybe_load_definitions_from(false, File).
