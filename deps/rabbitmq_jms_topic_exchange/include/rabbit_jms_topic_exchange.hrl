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
%% The Initial Developer of the Original Code is Pivotal Software, Inc.
%% Copyright (c) 2012, 2013 Pivotal Software, Inc.  All rights reserved.
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
%% Name of arg on exchange creation and bindings. Used to supply client version
%% for plugin check.
%%      private static final String RJMS_VERSION_ARG = "rjms_version";
%% in JMS Client.
%% If absent, client version assumed to be < 1.2.0.
-define(RJMS_VERSION_ARG, <<"rjms_version">>).
%% -----------------------------------------------------------------------------

%% -----------------------------------------------------------------------------
%% Name of arg on binding used to specify erlang term -- string type
%%      private static final String RJMS_COMPILED_SELECTOR_ARG = "rjms_erlang_selector";
%% in JMS Client.
-define(RJMS_COMPILED_SELECTOR_ARG, <<"rjms_erlang_selector">>).
%% -----------------------------------------------------------------------------

%% -----------------------------------------------------------------------------
%% List of versions compatible with this level of topic exchange.
-define(RJMS_COMPATIBLE_VERSIONS, [ "1.4.7"             % current build release
                                 %, "1.4.5"             % release omitted
                                  , "1.4.4"
                                 %, "1.4.3"             % release omitted
                                 %, "1.4.2"             % release omitted
                                  , "1.4.1"
                                  , "1.3.4"
                                  , "1.3.3"
                                  , "1.3.2"
                                  , "1.3.1"
                                 %, "1.3.0"             % release aborted
                                  , "1.2.5"
                                  , "1.2.4"
                                  , "1.2.3"
                                 %, "1.2.2"             % release omitted
                                  , "1.2.1"
                                  , "1.2.0"
                                  , ""                  % development use only
                                  , "0.0.0"             % development use only
                                  ]).
%% -----------------------------------------------------------------------------
