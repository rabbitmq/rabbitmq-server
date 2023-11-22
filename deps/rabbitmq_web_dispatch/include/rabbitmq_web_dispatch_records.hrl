%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.
%%

-record(context, {user,
                  password = none,
                  impl}). % storage for a context of the resource handler

-record(auth_settings, {auth_realm = "Basic realm=\"RabbitMQ undefined\"",
                        basic_auth_enabled = false,
                        oauth2_enabled = false,
                        oauth_client_id = <<"">>}).
