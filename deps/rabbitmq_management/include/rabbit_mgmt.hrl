%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-define(AUTH_REALM, "Basic realm=\"RabbitMQ Management\"").

-define(HEALTH_CHECK_FAILURE_STATUS, 503).

-define(MANAGEMENT_PG_SCOPE, rabbitmq_management).
-define(MANAGEMENT_PG_GROUP, management_db).

-define(MANAGEMENT_DEFAULT_HTTP_MAX_BODY_SIZE, 20000000).

-define(OAUTH2_ACCESS_TOKEN,                        <<"access_token">>).
%% Where rabbit_mgmt_wm_bootstrap.erl (js/bootstrap.js) reads and clears the
%% short-lived cookies rabbit_mgmt_login.erl sets on the way here. Not
%% js/oidc-oauth/bootstrap.js: that endpoint is only ever loaded as an
%% embedded sub-resource (from index.html's generated js/bootstrap.js, or
%% from the oidc login/logout callback pages), never as the page the browser
%% is redirected to, so it would never actually receive these cookies.
-define(OAUTH2_BOOTSTRAP_PATH,                      <<"/js/bootstrap.js">>).
-define(MANAGEMENT_LOGIN_STRICT_AUTH_MECHANISM,     <<"strict_auth_mechanism">>).
-define(MANAGEMENT_LOGIN_PREFERRED_AUTH_MECHANISM,  <<"preferred_auth_mechanism">>).
