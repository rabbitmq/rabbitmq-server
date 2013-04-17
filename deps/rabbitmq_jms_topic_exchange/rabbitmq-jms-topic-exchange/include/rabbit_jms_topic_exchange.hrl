%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2012, 2013 VMware, Inc.  All rights reserved.
%%

%% JMS on Rabbit Topic Selector Exchange plugin definitions

-define(X_TYPE_NAME, <<"x-jms-topic">>).

%% mnesia stuff
-define(JMS_TOPIC_TABLE, x_jms_topic_table).
-define(JMS_TOPIC_RECORD, x_jms_topic_xs).

%% Key is x_name -- the exchange name
-record(?JMS_TOPIC_RECORD, {x_name, x_selector_funs, x_type_info}).

%% x_selector_funs field consists of a `dict` map of binding functions:
%%         dict: RoutingKey x DestName -> BindingSelectorFun
%% there is no default, but an empty map will be initially inserted.
%%
%% x_type_info field is a description of the assumed types of specific identifiers.
%%    For example:
%%    [ {<<"JMSDeliveryMode">>, [<<"PERSISTENT">>, <<"NON_PERSISTENT">>]}
%%    , {<<"JMSPriority">>, number}
%%    , {<<"JMSMessageID">>, string}
%%    , {<<"JMSTimestamp">>, number}
%%    , {<<"JMSCorrelationID">>, string}
%%    , {<<"JMSType">>, string}
%%    ]).
%% In general it is a list of identifier, type pairs:
%%    type_info() :: [{ident_name(), ident_type()}]
%%    ident_type() :: number | string | boolean | [enum_value()]
%% where
%%    ident_name() :: binary()
%%    enum_value() :: binary()
%% there is no default, but an empty list will be initially inserted.

%% Name of arg on binding used to specify selector -- string type
%%      private static final String RJMS_SELECTOR_ARG = "rjms_selector";
%% in JMS Client.
-define(RJMS_SELECTOR_ARG, <<"rjms_selector">>).

%% Name of argument on exchange create; used to specify identifier types -- string type
%%      private static final String RJMS_TYPE_INFO_ARG = "rjms_type_info";
%% in JMS Client.
-define(RJMS_TYPE_INFO_ARG, <<"rjms_type_info">>).

%% This is supplied in a slightly different format than required,
%% to allow all clients to create it.
%%
%% For example, the standard JMS type-info argument is supplied as:
%%   [ {<<"JMSType">>,          longstr, <<"string">>}
%%   , {<<"JMSCorrelationID">>, longstr, <<"string">>}
%%   , {<<"JMSMessageID">>,     longstr, <<"string">>}
%%   , {<<"JMSDeliveryMode">>,  array,
%%        [{longstr,<<"PERSISTENT">>}, {longstr,<<"NON_PERSISTENT">>}]}
%%   , {<<"JMSPriority">>,      longstr, <<"number">>}
%%   , {<<"JMSTimestamp">>,     longstr, <<"number">>}
%%   ]
