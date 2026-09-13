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
        ?LOG_WARNING("OAuth 2 provider ~ts has an issuer configured but "
                     "verify_issuer is not enabled: the iss claim of its tokens "
                     "will not be verified",
                     [oauth2_client:format_oauth_provider_id(Id)])
        end, rabbit_oauth2_provider:oauth_provider_ids_with_unverified_issuer()).

stop(_State) ->
    ok.

%%----------------------------------------------------------------------------

init([]) ->
    {ok, {{one_for_one,3,10},[]}}.
