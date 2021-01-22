%%%-------------------------------------------------------------------
%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2020-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_boot_state_sup).
-behaviour(supervisor).

-export([start_link/0,
         init/1]).

-export([notify_boot_state_listeners/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    SystemdSpec = #{id => rabbit_boot_state_systemd,
                    start => {rabbit_boot_state_systemd, start_link, []},
                    restart => transient},
    {ok, {#{strategy => one_for_one,
            intensity => 1,
            period => 5},
          [SystemdSpec]}}.

-spec notify_boot_state_listeners(rabbit_boot_state:boot_state()) -> ok.
notify_boot_state_listeners(BootState) ->
    lists:foreach(
      fun
          ({_, Child, _, _}) when is_pid(Child) ->
              gen_server:cast(Child, {notify_boot_state, BootState});
          (_) ->
              ok
      end,
      supervisor:which_children(?MODULE)).
