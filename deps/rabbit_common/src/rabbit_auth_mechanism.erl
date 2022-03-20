%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_auth_mechanism).

-behaviour(rabbit_registry_class).

-export([added_to_rabbit_registry/2, removed_from_rabbit_registry/1]).

%% A description.
-callback description() -> [proplists:property()].

%% If this mechanism is enabled, should it be offered for a given socket?
%% (primarily so EXTERNAL can be TLS-only)
-callback should_offer(rabbit_net:socket()) -> boolean().

%% Called before authentication starts. Should create a state
%% object to be passed through all the stages of authentication.
-callback init(rabbit_net:socket()) -> any().

%% Handle a stage of authentication. Possible responses:
%% {ok, User}
%%     Authentication succeeded, and here's the user record.
%% {challenge, Challenge, NextState}
%%     Another round is needed. Here's the state I want next time.
%% {protocol_error, Msg, Args}
%%     Client got the protocol wrong. Log and die.
%% {refused, Username, Msg, Args}
%%     Client failed authentication. Log and die.
-callback handle_response(binary(), any()) ->
    {'ok', rabbit_types:user()} |
    {'challenge', binary(), any()} |
    {'protocol_error', string(), [any()]} |
    {'refused', rabbit_types:username() | none, string(), [any()]}.

added_to_rabbit_registry(_Type, _ModuleName) -> ok.
removed_from_rabbit_registry(_Type) -> ok.
