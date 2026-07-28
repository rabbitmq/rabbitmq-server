%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-record(context, {user,
                  password = none,
                  impl}). % storage for a context of the resource handler

%% Pluggable bearer token parser supplied by the hosting application.
%% Returns {basic, Username, Password} to route through Basic auth,
%% {bearer, Token} to route through OAuth2, or {error, Reason} to reject.
-type bearer_token_parser() ::
    fun((binary()) -> {basic, binary(), binary()}
                    | {bearer, binary()}
                    | {error, binary()}).

-record(auth_settings, {
    auth_realm          = "Basic realm=\"RabbitMQ undefined\"",
    basic_auth_enabled  = false,
    bearer_token_parser = undefined :: undefined | bearer_token_parser()
}).
