%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-define(AUTH_REALM, "Basic realm=\"RabbitMQ Management\"").

-define(HEALTH_CHECK_FAILURE_STATUS, 503).

-define(MANAGEMENT_PG_SCOPE, rabbitmq_management).
-define(MANAGEMENT_PG_GROUP, management_db).

-define(MANAGEMENT_DEFAULT_HTTP_MAX_BODY_SIZE, 20000000).

-define(OAUTH2_ACCESS_TOKEN_COOKIE_NAME, <<"access_token">>).
-define(OAUTH2_ACCESS_TOKEN_COOKIE_PATH, <<"js/oidc-oauth/bootstrap.js">>).
