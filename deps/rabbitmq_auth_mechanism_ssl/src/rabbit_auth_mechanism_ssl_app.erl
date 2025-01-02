%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_auth_mechanism_ssl_app).

-behaviour(application).
-export([start/2, stop/1]).

%% Dummy supervisor - see Ulf Wiger's comment at
%% http://erlang.org/pipermail/erlang-questions/2010-April/050508.html

-behaviour(supervisor).
-export([init/1]).

start(normal, []) ->
    supervisor:start_link({local,?MODULE},?MODULE,[]).

stop(_State) ->
    ok.

init([]) ->
    {ok, {{one_for_one,3,10},[]}}.
