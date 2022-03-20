%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_federation_app).

-include("rabbit_federation.hrl").

-behaviour(application).
-export([start/2, stop/1]).

%% Dummy supervisor - see Ulf Wiger's comment at
%% http://erlang.org/pipermail/erlang-questions/2010-April/050508.html

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
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

stop(_State) ->
    rabbit_federation_pg:stop_scope(),
    ok.

%%----------------------------------------------------------------------------

init([]) ->
    Flags = #{
        strategy  => one_for_one,
        intensity => 3,
        period    => 10
    },
    {ok, {Flags, []}}.
