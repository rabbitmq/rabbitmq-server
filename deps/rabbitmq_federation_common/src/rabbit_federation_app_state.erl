%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

%% Tracks transient application state for federation plugins.
%% Used to prevent link restarts during node shutdown.
-module(rabbit_federation_app_state).

-export([is_shutting_down/0, mark_as_shutting_down/0, reset_shutting_down_marker/0]).

-define(APP, rabbitmq_federation_common).

-spec is_shutting_down() -> boolean().
is_shutting_down() ->
    application:get_env(?APP, shutting_down, false).

-spec mark_as_shutting_down() -> ok.
mark_as_shutting_down() ->
    application:set_env(?APP, shutting_down, true).

-spec reset_shutting_down_marker() -> ok.
reset_shutting_down_marker() ->
    application:unset_env(?APP, shutting_down).
