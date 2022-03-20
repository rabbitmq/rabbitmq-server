%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
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
