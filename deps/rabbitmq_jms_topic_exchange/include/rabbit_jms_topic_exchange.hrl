%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2012-2020 VMware, Inc. or its affiliates.  All rights reserved.
%% -----------------------------------------------------------------------------

%% JMS on Rabbit Topic Selector Exchange plugin definitions

%% -----------------------------------------------------------------------------
%% User-defined exchange type name
-define(X_TYPE_NAME, <<"x-jms-topic">>).

%% -----------------------------------------------------------------------------
%% mnesia database records
-define(JMS_TOPIC_TABLE, x_jms_topic_table).
-define(JMS_TOPIC_RECORD, x_jms_topic_xs).

%% Key is x_name -- the exchange name
-record(?JMS_TOPIC_RECORD, {x_name, x_selection_policy = undefined, x_selector_funs}).
%% fields:
%%  x_selector_funs
%%      a partial map (`dict`) of binding functions:
%%          dict: RoutingKey x DestName -/-> BindingSelectorFun
%%      (there is no default, but an empty map will be initially inserted)
%%      where a BindingSelectorFun has the signature:
%%          bsf : Headers -> boolean
%%  x_selection_policy
%%      not used, retained for backwards compatibility of db records.
%% -----------------------------------------------------------------------------

%% -----------------------------------------------------------------------------

%% -----------------------------------------------------------------------------
%% Name of arg on binding used to specify erlang term -- string type
%%      private static final String RJMS_COMPILED_SELECTOR_ARG = "rjms_erlang_selector";
%% in JMS Client.
-define(RJMS_COMPILED_SELECTOR_ARG, <<"rjms_erlang_selector">>).
%% -----------------------------------------------------------------------------
