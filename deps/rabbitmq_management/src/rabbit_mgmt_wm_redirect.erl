%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_mgmt_wm_redirect).
-export([init/2]).

init(Req0, RedirectTo) ->
    Req = cowboy_req:reply(301, #{<<"location">> => RedirectTo}, Req0),
    {ok, Req, RedirectTo}.
