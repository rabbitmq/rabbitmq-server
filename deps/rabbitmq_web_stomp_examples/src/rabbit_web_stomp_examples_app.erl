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
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2017 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_web_stomp_examples_app).

-behaviour(application).
-export([start/2,stop/1]).

%% Dummy supervisor - see Ulf Wiger's comment at
%% http://erlang.2086793.n4.nabble.com/initializing-library-applications-without-processes-td2094473.html
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
