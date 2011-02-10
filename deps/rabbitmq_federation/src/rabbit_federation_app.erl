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

-module(rabbit_federation_app).

-behaviour(application).
-export([start/2, stop/1]).

-include_lib("amqp_client/include/amqp_client.hrl").

start(_Type, _StartArgs) ->
    {ok, Xs} = application:get_env(exchanges),
    Sup = rabbit_federation_sup:start_link(),
    rabbit_federation_exchange:start(),
    [declare_exchange(X) || X <- Xs],
    Sup.

stop(_State) ->
    ok.

%%----------------------------------------------------------------------------

declare_exchange({Downstream, Upstream, Type}) ->
    {ok, Conn} = amqp_connection:start(direct),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    %% TODO make durable, recover bindings etc
    amqp_channel:call(
      Ch, #'exchange.declare'{
        exchange = list_to_binary(Downstream),
        type = <<"x-federation">>,
        arguments = [{<<"upstream">>, longstr, list_to_binary(Upstream)},
                     {<<"type">>,     longstr, list_to_binary(Type)}]}),
    amqp_channel:close(Ch),
    amqp_connection:close(Conn).
