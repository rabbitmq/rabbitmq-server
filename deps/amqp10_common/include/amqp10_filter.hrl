%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.

%% AMQP Filter Expressions Version 1.0 Committee Specification Draft 01
%% https://docs.oasis-open.org/amqp/filtex/v1.0/csd01/filtex-v1.0-csd01.html#_Toc67929253
-define(CAP_FILTEX_SQL, <<"AMQP_FILTEX_SQL_V1_0">>).
-define(CAP_FILTEX_PROP, <<"AMQP_FILTEX_PROP_V1_0">>).
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

%% A filter with this name contains a JMS message selector.
%% We use the same name as Qpid JMS in
%% https://github.com/apache/qpid-jms/blob/2.10.0/qpid-jms-client/src/main/java/org/apache/qpid/jms/provider/amqp/AmqpSupport.java#L75
-define(FILTER_NAME_SELECTOR, <<"jms-selector">>).
%% SQL-based filtering syntax
%% This capability and these descriptors are defined in
%% https://www.amqp.org/specification/1.0/filters
-define(CAP_SELECTOR, <<"APACHE.ORG:SELECTOR">>).
-define(DESCRIPTOR_NAME_SELECTOR_FILTER, <<"apache.org:selector-filter:string">>).
-define(DESCRIPTOR_CODE_SELECTOR_FILTER, 16#0000468C00000004).
