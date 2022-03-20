%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2020-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-include_lib("rabbit_common/include/logging.hrl").

-define(RMQLOG_DOMAIN_PEER_DIS, ?DEFINE_RMQLOG_DOMAIN(peer_discovery)).

% rabbitmq/rabbitmq-peer-discovery-aws#25
% Note: this timeout must not be greater than the default
% gen_server:call timeout of 5000ms. This `timeout`,
% when set, is used as the connect and then request timeout
% by `httpc`
-define(DEFAULT_HTTP_TIMEOUT, 2250).

-type peer_discovery_config_value() :: atom() | integer() | string() | list() | map() | any() | undefined.

-record(peer_discovery_config_entry_meta,
        {env_variable  :: string(),
         default_value :: peer_discovery_config_value(),
         type          :: atom()}).

-type(peer_discovery_config_entry_meta() :: #peer_discovery_config_entry_meta{
                                             type :: atom(),
                                             env_variable :: string(),
                                             default_value :: peer_discovery_config_value()
                                            }).
