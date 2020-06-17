%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% https://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2011-2020 VMware, Inc. or its affiliates.  All rights reserved.
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
