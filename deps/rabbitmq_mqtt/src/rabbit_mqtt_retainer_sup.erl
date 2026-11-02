%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_mqtt_retainer_sup).

-behaviour(supervisor).

%% supervsior callback
-export([init/1]).

%% API
-export([start_link/0,
         start_child_for_vhost/1,
         delete_child_for_vhost/1]).

-spec start_link() -> supervisor:startlink_ret().
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

-spec start_child_for_vhost(rabbit_types:vhost()) -> pid().
start_child_for_vhost(VHost)
  when is_binary(VHost) ->
    case supervisor:start_child(?MODULE, child_spec(VHost)) of
        {ok, Pid}                       -> Pid;
        {error, {already_started, Pid}} -> Pid
    end.

-spec delete_child_for_vhost(rabbit_types:vhost()) -> ok.
delete_child_for_vhost(VHost) ->
    ok = supervisor:terminate_child(?MODULE, VHost),
    ok = supervisor:delete_child(?MODULE, VHost).

-spec init([]) ->
    {ok, {supervisor:sup_flags(),
          [supervisor:child_spec()]}}.
init([]) ->
    {ok, {#{intensity => 5,
            period => 5},
          child_specs(rabbit_vhost:list_names())
         }}.

-spec child_specs([rabbit_types:vhost()]) ->
    [supervisor:child_spec()].
child_specs(VHosts) ->
    lists:map(fun child_spec/1, VHosts).

-spec child_spec(rabbit_types:vhost()) ->
    supervisor:child_spec().
child_spec(VHost) ->
    #{id => VHost,
      start => {rabbit_mqtt_retainer, start_link, [VHost]},
      shutdown => 120_000}.
