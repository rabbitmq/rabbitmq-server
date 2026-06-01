%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(dummy_interceptor_priority_1_conflict).

-behaviour(rabbit_channel_interceptor).

-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("rabbit_common/include/rabbit_framing.hrl").

-compile(export_all).

init(_Ch) -> undefined.

description() -> [{description, <<"Conflicts with dummy_interceptor_priority_1">>}].

intercept(Method, Content, _State) ->
    {Method, Content}.

applies_to() -> ['basic.publish'].
