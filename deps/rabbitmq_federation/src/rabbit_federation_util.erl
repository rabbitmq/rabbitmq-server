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
%% The Original Code is RabbitMQ Federation.
%%
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2007-2011 VMware, Inc.  All rights reserved.
%%

-module(rabbit_federation_util).

-include_lib("amqp_client/include/amqp_client.hrl").
-include("rabbit_federation.hrl").

-export([federation_up/0, local_params/0]).

%%----------------------------------------------------------------------------

federation_up() ->
    lists:keysearch(rabbitmq_federation, 1,
                    application:which_applications(infinity)) =/= false.

local_params() ->
    {ok, U} = application:get_env(rabbitmq_federation, local_username),
    #amqp_params_direct{username = list_to_binary(U)}.

