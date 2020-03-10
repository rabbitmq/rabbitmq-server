%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at https://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_credential_validation).

-include("rabbit.hrl").

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
