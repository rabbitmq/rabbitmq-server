%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_web_stomp_examples_app).

-behaviour(application).
-export([start/2,stop/1]).

%% Dummy supervisor - see Ulf Wiger's comment at
%% http://erlang.org/pipermail/erlang-questions/2010-April/050508.html
-behaviour(supervisor).
-export([init/1]).

start(_Type, _StartArgs) ->
    {ok, Listener} = application:get_env(rabbitmq_web_stomp_examples, listener),
    {ok, _} = rabbit_web_dispatch:register_static_context(
                web_stomp_examples, Listener, "web-stomp-examples", ?MODULE,
                "priv", "WEB-STOMP: examples"),
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

stop(_State) ->
    rabbit_web_dispatch:unregister_context(web_stomp_examples),
    ok.

init([]) -> {ok, {{one_for_one, 3, 10}, []}}.
