%%%-------------------------------------------------------------------
%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at https://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% Copyright (c) 2020 VMware, Inc. or its affiliates.  All rights reserved.
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
