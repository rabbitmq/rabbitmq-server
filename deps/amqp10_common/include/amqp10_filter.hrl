%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.

%% A filter with this name contains a JMS message selector.
%% We use the same name as sent by the Qpid JMS client in
%% https://github.com/apache/qpid-jms/blob/2.7.0/qpid-jms-client/src/main/java/org/apache/qpid/jms/provider/amqp/AmqpSupport.java#L75
-define(FILTER_NAME_JMS, <<"jms-selector">>).

%% A filter with this name contains an SQL expression.
%% In the current version, such a filter must comply with the JMS message selector syntax.
%% However, we use a name other than "jms-selector" in case we want to extend the allowed syntax
%% in the future, for example allowing for some of the extended grammar described in
%% §6 "SQL Filter Expressions" of
%% https://groups.oasis-open.org/higherlogic/ws/public/document?document_id=66227
-define(FILTER_NAME_SQL, <<"sql-filter">>).

%% SQL-based filtering syntax
%% These descriptors are defined in
%% https://www.amqp.org/specification/1.0/filters
-define(DESCRIPTOR_NAME_SELECTOR_FILTER, <<"apache.org:selector-filter:string">>).
-define(DESCRIPTOR_CODE_SELECTOR_FILTER, 16#0000468C00000004).

%% AMQP Filter Expressions Version 1.0 Working Draft 09
%% https://groups.oasis-open.org/higherlogic/ws/public/document?document_id=66227
-define(DESCRIPTOR_NAME_PROPERTIES_FILTER, <<"amqp:properties-filter">>).
-define(DESCRIPTOR_CODE_PROPERTIES_FILTER, 16#173).
-define(DESCRIPTOR_NAME_APPLICATION_PROPERTIES_FILTER, <<"amqp:application-properties-filter">>).
-define(DESCRIPTOR_CODE_APPLICATION_PROPERTIES_FILTER, 16#174).
