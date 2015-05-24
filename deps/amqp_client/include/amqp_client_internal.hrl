%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2015 Pivotal Software, Inc.  All rights reserved.
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
