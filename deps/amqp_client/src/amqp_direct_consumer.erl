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
%% Copyright (c) 2007-2011 VMware, Inc.  All rights reserved.
%%

%% @doc TODO
-module(amqp_direct_consumer).

-behaviour(amqp_gen_consumer).

-export([init/1, handle_consume_ok/2, handle_cancel_ok/2, handle_cancel/2,
         handle_deliver/3, handle_message/2, terminate/2]).

%%---------------------------------------------------------------------------
%% amqp_gen_consumer callbacks
%%---------------------------------------------------------------------------

%% @private
init([ConsumerPid]) ->
    {ok, ConsumerPid}.

%% @private
handle_consume_ok(P, C) ->
    send(P, C).

%% @private
handle_cancel_ok(P, C) ->
    send(P, C).

%% @private
handle_cancel(P, C) ->
    send(P, C).

%% @private
handle_message(P, C) ->
    send(P, C).

%% @private
handle_deliver(Deliver, AmqpMsg, C) ->
    send({Deliver, AmqpMsg}, C).

%% @private
terminate(Reason, ConsumerPid) ->
    exit(ConsumerPid, Reason).

%%---------------------------------------------------------------------------
%% Internal plumbing
%%---------------------------------------------------------------------------

send(Param, ConsumerPid) ->
    ConsumerPid ! Param,
    {ok, ConsumerPid}.
