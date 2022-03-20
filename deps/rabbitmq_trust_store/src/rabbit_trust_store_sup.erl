%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_trust_store_sup).
-behaviour(supervisor).
-export([start_link/0]).
-export([init/1]).

-include_lib("rabbit_common/include/rabbit.hrl").


%% ...

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).


%% ...

init([]) ->
    Flags = #{strategy => one_for_one,
              intensity => 10,
              period => 1},
    ChildSpecs = [
        #{
            id => trust_store,
            start => {rabbit_trust_store, start_link, []},
            restart => permanent,
            shutdown => timer:seconds(15),
            type => worker,
            modules => [rabbit_trust_store]
        }
    ],

    {ok, {Flags, ChildSpecs}}.
