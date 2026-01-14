%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

%% Tracks transient application state using persistent_term storage.
%% This is used to prevent link restarts during node shutdown.
-module(rabbit_federation_app_state).

-export([is_shutting_down/0,
         mark_as_shutting_down/0,
         reset_shutting_down_marker/0]).

-define(SHUTDOWN_KEY, {?MODULE, shutting_down}).

-spec is_shutting_down() -> boolean().
is_shutting_down() ->
    persistent_term:get(?SHUTDOWN_KEY, false).

-spec mark_as_shutting_down() -> ok.
mark_as_shutting_down() ->
    persistent_term:put(?SHUTDOWN_KEY, true),
    ok.

-spec reset_shutting_down_marker() -> ok.
reset_shutting_down_marker() ->
    _ = persistent_term:erase(?SHUTDOWN_KEY),
    ok.
