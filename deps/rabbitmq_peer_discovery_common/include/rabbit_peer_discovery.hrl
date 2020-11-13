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
%% Copyright (c) 2007-2017 Pivotal Software, Inc.  All rights reserved.
%%

% rabbitmq/rabbitmq-peer-discovery-aws#25
% Note: this timeout must not be greater than the default
% gen_server:call timeout of 5000ms. This `timeout`,
% when set, is used as the connect and then request timeout
% by `httpc`
-define(DEFAULT_HTTP_TIMEOUT, 2250).

-type peer_discovery_config_value() :: atom() | integer() | string() | undefined.

-record(peer_discovery_config_entry_meta,
        {env_variable  :: string(),
         default_value :: peer_discovery_config_value(),
         type          :: atom()}).
