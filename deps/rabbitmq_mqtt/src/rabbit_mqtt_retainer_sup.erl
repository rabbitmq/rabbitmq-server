%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_mqtt_retainer_sup).

-behaviour(supervisor).

-include_lib("kernel/include/logger.hrl").

%% supervsior callback
-export([init/1]).

%% API
-export([start_link/0,
         start_child_for_vhost/1,
         delete_child_for_vhost/1,
         delete_orphaned_storage/0]).

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

-spec delete_child_for_vhost(rabbit_types:vhost()) -> rabbit_types:ok_or_error(any()).
delete_child_for_vhost(VHost) ->
    Running = whereis(?MODULE) =/= undefined,
    _ = Running andalso supervisor:terminate_child(?MODULE, VHost),
    %% The stopped child stays until the files are gone, so that a concurrent
    %% start cannot open them.
    Result = delete_storage(VHost),
    _ = Running andalso supervisor:delete_child(?MODULE, VHost),
    Result.

%% Also matches files of other backends and the `.TMP` file of a DETS repair.
delete_storage(VHost) ->
    Dir = rabbit:data_dir(),
    rabbit_file:recursive_delete(
      [filename:join(Dir, F)
       || F <- filelib:wildcard(rabbit_mqtt_util:vhost_name_to_dir_name(VHost, ".*"), Dir)]).

%% Deletes the stores of vhosts that were deleted while this node was down.
-spec delete_orphaned_storage() -> ok.
delete_orphaned_storage() ->
    Dir = rabbit:data_dir(),
    Files = filelib:wildcard("mqtt_retained_*", Dir),
    %% The projection can be out of sync while Khepri recovers.
    %% rabbitmq/rabbitmq-server#16422.
    case rabbit_db_vhost:list_uncached() of
        {error, Reason} ->
            ?LOG_WARNING("Not deleting MQTT retained message stores of deleted vhosts, "
                         "vhosts cannot be listed: ~tp", [Reason]);
        {ok, VHosts} ->
            Live = maps:from_keys(
                     [rabbit_mqtt_util:vhost_name_to_dir_name(V, "") || V <- VHosts], true),
            case [filename:join(Dir, F) || F <- Files,
                                           not maps:is_key(hd(string:split(F, ".")), Live)] of
                [] ->
                    ok;
                Orphaned ->
                    ?LOG_INFO("Deleting MQTT retained message stores of deleted vhosts: ~tp",
                              [Orphaned]),
                    lists:foreach(
                      fun(Path) ->
                              case rabbit_file:recursive_delete([Path]) of
                                  ok ->
                                      ok;
                                  {error, Reason} ->
                                      ?LOG_WARNING("Failed to delete MQTT retained message "
                                                   "store of a deleted vhost: ~tp", [Reason])
                              end
                      end, Orphaned)
            end
    end.

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
