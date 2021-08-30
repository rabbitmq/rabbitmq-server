%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2011-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(dummy_supervisor2).

-behaviour(supervisor2).

-export([
    start_link/0,
    init/1
  ]).

start_link() ->
    Pid = spawn_link(fun () ->
                             process_flag(trap_exit, true),
                             receive stop -> ok end
                     end),
    {ok, Pid}.

init([Timeout]) ->
    {ok, {{one_for_one, 0, 1},
          [{test_sup, {supervisor2, start_link,
                       [{local, ?MODULE}, ?MODULE, []]},
            transient, Timeout, supervisor, [?MODULE]}]}};
init([]) ->
    {ok, {{simple_one_for_one, 0, 1},
          [{test_worker, {?MODULE, start_link, []},
            temporary, 1000, worker, [?MODULE]}]}}.
