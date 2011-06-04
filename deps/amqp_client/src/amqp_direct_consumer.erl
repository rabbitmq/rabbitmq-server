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
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2011-2011 VMware, Inc.  All rights reserved.
%%

%% @doc This module is an implementation of the amqp_gen_consumer behaviour and
%% can be used as part of the Consumer parameter when opening AMQP
%% channels.<br/>
%% <br/>
%% The Consumer parameter for this implementation is
%% {{@module}, [ConsumerPid]@}, where ConsumerPid is a process that
%% will receive queue subscription-related messages.<br/>
%% <br/>
%% This consumer implementation causes the channel to send to the ConsumerPid
%% all basic.consume_ok, basic.cancel_ok, basic.cancel and basic.deliver
%% messages received from the server.<br/>
%% <br/>
%% In addition, this consumer implementation creates a link between the channel
%% and the provided ConsumerPid.<br/>
%% Warning! It is not recommended to rely on a consumer on killing off the
%% channel (through the exit signal). That may cause messages to get lost.
%% Always use amqp_channel:close/{1,3} for a clean shut down.<br/>
%% <br/>
%% This module has no public functions.
-module(amqp_direct_consumer).

-include("amqp_gen_consumer_spec.hrl").

-behaviour(amqp_gen_consumer).

-export([init/1, handle_consume_ok/3, handle_cancel_ok/3, handle_cancel/2,
         handle_deliver/2, handle_call/3, terminate/2]).

%%---------------------------------------------------------------------------
%% amqp_gen_consumer callbacks
%%---------------------------------------------------------------------------

%% @private
init([ConsumerPid]) ->
    link(ConsumerPid),
    ConsumerPid.

%% @private
handle_consume_ok(M, _, C) ->
    C ! M,
    C.

%% @private
handle_cancel_ok(M, _, C) ->
    C ! M,
    C.

%% @private
handle_cancel(M, C) ->
    C ! M,
    C.

%% @private
handle_deliver(M, C) ->
    C ! M,
    C.

%% @private
handle_call(M, _, C) ->
    C ! M,
    {reply, ok, C}.

%% @private
terminate(_Reason, C) ->
    C.
