%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_auth_backend_oauth2_app).

-include_lib("kernel/include/logger.hrl").

-behaviour(application).
-export([start/2, stop/1]).

-behaviour(supervisor).
-export([init/1]).

start(_Type, _StartArgs) ->
    warn_about_unverified_issuers(),
    supervisor:start_link({local,?MODULE},?MODULE,[]).

warn_about_unverified_issuers() ->
    lists:foreach(fun(Id) ->
        ?LOG_WARNING("OAuth 2 provider ~ts has an issuer configured but does not "
                     "verify the iss claim of its tokens. Set auth_oauth2.verify_issuer "
                     "or auth_oauth2.oauth_providers.<name>.verify_issuer to true to "
                     "enable that",
                     [oauth2_client:format_oauth_provider_id(Id)])
        end, rabbit_oauth2_provider:oauth_provider_ids_with_unverified_issuer()),
    lists:foreach(fun(Id) ->
        ?LOG_WARNING("OAuth 2 provider ~ts verifies the iss claim of its tokens but "
                     "has no issuer configured: every token it signs will be rejected. "
                     "Configure its issuer or set verify_issuer to false for it",
                     [oauth2_client:format_oauth_provider_id(Id)])
        end, rabbit_oauth2_provider:oauth_provider_ids_without_issuer_to_verify()).

stop(_State) ->
    ok.

%%----------------------------------------------------------------------------

init([]) ->
    {ok, {{one_for_one,3,10},[]}}.
