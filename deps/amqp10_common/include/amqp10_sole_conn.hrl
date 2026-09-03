%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.

%% (AMQP) Enforcing Connection Uniqueness Version 1.0
%% Committee Specification 01
%% 17 September 2018
%% https://docs.oasis-open.org/amqp/soleconn/v1.0/soleconn-v1.0.pdf
%%

-type enforcement_policy() :: refuse_connection | close_existing.

-define(CAP_SOLE_CONN, <<"sole-connection-for-container">>).
-define(SOLE_CONN_ENFORCEMENT_POLICY_KEY, <<"sole-connection-enforcement-policy">>).
-define(SOLE_CONN_ENFORCEMENT_POLICY, {symbol, ?SOLE_CONN_ENFORCEMENT_POLICY_KEY}).
-define(SOLE_CONN_ENFORCEMENT_POLICY_REFUSE_CONN, {uint, 0}).
-define(SOLE_CONN_ENFORCEMENT_POLICY_CLOSE_EXISTING, {uint, 1}).
-define(SOLE_CONN_DETECTION_POLICY, {symbol, <<"sole-connection-detection-policy">>}).
-define(SOLE_CONN_DETECTION_POLICY_WEAK, {uint, 1}).
-define(AMQP_ERROR_CONNECTION_ESTABLISHMENT_FAILED,
        {symbol, <<"amqp:connection-establishment-failed">>}).
-define(SOLE_CONN_ENFORCEMENT, {symbol, <<"sole-connection-enforcement">>}).
