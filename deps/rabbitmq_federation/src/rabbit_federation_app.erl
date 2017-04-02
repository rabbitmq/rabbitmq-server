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
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2017 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_federation_app).

-behaviour(application).
-export([start/2, stop/1]).

%% Dummy supervisor - see Ulf Wiger's comment at
%% http://erlang.2086793.n4.nabble.com/initializing-library-applications-without-processes-td2094473.html

%% All of our actual server processes are supervised by
%% rabbit_federation_sup, which is started by a rabbit_boot_step
%% (since it needs to start up before queue / exchange recovery, so it
%% can't be part of our application).
%%
%% However, we still need an application behaviour since we need to
%% know when our application has started since then the Erlang client
%% will have started and we can therefore start our links going. Since
%% the application behaviour needs a tree of processes to supervise,
%% this is it...
-behaviour(supervisor).
-export([init/1]).

start(_Type, _StartArgs) ->
    rabbit_federation_exchange_link:go(),
    rabbit_federation_queue_link:go(),
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

stop(_State) ->
    ok.
%%----------------------------------------------------------------------------

init([]) -> {ok, {{one_for_one, 3, 10}, []}}.
