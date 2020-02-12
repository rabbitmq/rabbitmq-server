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
%% Copyright (c) 2007-2020 Pivotal Software, Inc.  All rights reserved.
%%

%% @private
-module(amqp_client).

-behaviour(application).

-export([start/0]).
-export([start/2, stop/1]).

%%---------------------------------------------------------------------------
%% Interface
%%---------------------------------------------------------------------------

start() ->
    %% rabbit_common needs compiler and syntax_tools, see
    %%
    %%  * https://github.com/rabbitmq/rabbitmq-erlang-client/issues/72
    %%  * https://github.com/rabbitmq/rabbitmq-common/pull/149
    application:ensure_all_started(rabbit_common),
    {ok, _} = application:ensure_all_started(amqp_client),
    ok.

%%---------------------------------------------------------------------------
%% application callbacks
%%---------------------------------------------------------------------------

start(_StartType, _StartArgs) ->
    amqp_sup:start_link().

stop(_State) ->
    ok.
