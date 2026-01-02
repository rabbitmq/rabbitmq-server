%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.

%% AMQP Filter Expressions Version 1.0 Committee Specification Draft 01
%% https://docs.oasis-open.org/amqp/filtex/v1.0/csd01/filtex-v1.0-csd01.html#_Toc67929266
-define(DESCRIPTOR_NAME_PROPERTIES_FILTER, <<"amqp:properties-filter">>).
-define(DESCRIPTOR_CODE_PROPERTIES_FILTER, 16#173).
-define(DESCRIPTOR_NAME_APPLICATION_PROPERTIES_FILTER, <<"amqp:application-properties-filter">>).
-define(DESCRIPTOR_CODE_APPLICATION_PROPERTIES_FILTER, 16#174).

%% A filter with this name contains an AMQP SQL expression.
-define(FILTER_NAME_SQL, <<"sql-filter">>).
%% https://docs.oasis-open.org/amqp/filtex/v1.0/csd01/filtex-v1.0-csd01.html#_Toc67929276
-define(DESCRIPTOR_NAME_SQL_FILTER, <<"amqp:sql-filter">>).
-define(DESCRIPTOR_CODE_SQL_FILTER, 16#120).
