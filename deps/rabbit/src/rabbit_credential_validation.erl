%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_credential_validation).

-include_lib("rabbit_common/include/rabbit.hrl").

%% used for backwards compatibility
-define(DEFAULT_BACKEND, rabbit_credential_validator_accept_everything).

%%
%% API
%%

-export([validate/2, backend/0]).

%% Validates a username/password pair by delegating to the effective
%% `rabbit_credential_validator`. Used by `rabbit_auth_backend_internal`.
%% Note that some validators may choose to only validate passwords.
%%
%% Possible return values:
%%
%% * ok: provided credentials passed validation.
%% * {error, Error, Args}: provided password password failed validation.

-spec validate(rabbit_types:username(), rabbit_types:password()) -> 'ok' | {'error', string()}.

validate(Username, Password) ->
    Backend = backend(),
    Backend:validate(Username, Password).

-spec backend() -> atom().

backend() ->
  case application:get_env(rabbit, credential_validator) of
    undefined      ->
      ?DEFAULT_BACKEND;
    {ok, Proplist} ->
      proplists:get_value(validation_backend, Proplist, ?DEFAULT_BACKEND)
  end.
