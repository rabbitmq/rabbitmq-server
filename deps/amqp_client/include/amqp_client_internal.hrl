%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2020-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-include("amqp_client.hrl").

-define(PROTOCOL_VERSION_MAJOR, 0).
-define(PROTOCOL_VERSION_MINOR, 9).
-define(PROTOCOL_HEADER, <<"AMQP", 0, 0, 9, 1>>).
-define(PROTOCOL, rabbit_framing_amqp_0_9_1).

-define(MAX_CHANNEL_NUMBER, 65535).

-define(LOG_DEBUG(Format),      error_logger:info_msg(Format)).
-define(LOG_INFO(Format, Args), error_logger:info_msg(Format, Args)).
-define(LOG_WARN(Format, Args), error_logger:warning_msg(Format, Args)).
-define(LOG_ERR(Format, Args),  error_logger:error_msg(Format, Args)).

-define(CLIENT_CAPABILITIES,
    [{<<"publisher_confirms">>,           bool, true},
     {<<"exchange_exchange_bindings">>,   bool, true},
     {<<"basic.nack">>,                   bool, true},
     {<<"consumer_cancel_notify">>,       bool, true},
     {<<"connection.blocked">>,           bool, true},
     {<<"authentication_failure_close">>, bool, true}]).

-define(WAIT_FOR_CONFIRMS_TIMEOUT, {60000, millisecond}).

-define(DIRECT_OPERATION_TIMEOUT,  120000).
-define(CALL_TIMEOUT_DEVIATION,    10000).
