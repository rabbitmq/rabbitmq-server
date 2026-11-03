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
    case start_child(VHost) of
        {error, already_present} ->
            with_vhost_lock(VHost, fun() -> restart_child(VHost) end);
        Pid ->
            Pid
    end.

start_child(VHost) ->
    case supervisor:start_child(?MODULE, child_spec(VHost)) of
        {ok, Pid}                       -> Pid;
        {error, {already_started, Pid}} -> Pid;
        {error, already_present} = Err  -> Err
    end.

%% `delete_child_for_vhost/1` leaves the child stopped when it fails to
%% delete the files of a deleted vhost.
restart_child(VHost) ->
    case start_child(VHost) of
        {error, already_present} ->
            ok = delete_storage(VHost),
            ok = supervisor:delete_child(?MODULE, VHost),
            start_child(VHost);
        Pid ->
            Pid
    end.

-spec delete_child_for_vhost(rabbit_types:vhost()) -> rabbit_types:ok_or_error(any()).
delete_child_for_vhost(VHost) ->
    with_vhost_lock(VHost, fun() -> do_delete_child_for_vhost(VHost) end).

do_delete_child_for_vhost(VHost) ->
    Stop = case whereis(?MODULE) of
               undefined -> {error, not_found};
               _ -> supervisor:terminate_child(?MODULE, VHost)
           end,
    case Stop of
        ok ->
            case delete_storage(VHost) of
                ok ->
                    supervisor:delete_child(?MODULE, VHost);
                %% Keep the stopped child so that the next connection deletes
                %% the files before it starts a new store.
                Error ->
                    Error
            end;
        {error, not_found} ->
            delete_storage(VHost);
        Error ->
            Error
    end.

with_vhost_lock(VHost, Fun) ->
    global:trans({{?MODULE, VHost}, self()}, Fun, [node()]).

delete_storage(VHost) ->
    rabbit_file:recursive_delete(storage_paths(rabbit:data_dir(), VHost)).

storage_paths(Dir, VHost) ->
    [rabbit_mqtt_util:path_for(Dir, VHost),
     rabbit_mqtt_util:path_for(Dir, VHost, ".dets")].

%% Deletes the stores of vhosts that were deleted while this node was down.
-spec delete_orphaned_storage() -> ok.
delete_orphaned_storage() ->
    Dir = rabbit:data_dir(),
    %% Excludes the `.TMP` file that DETS writes while it repairs a table.
    Files = [filename:join(Dir, F)
             || F <- filelib:wildcard("mqtt_retained_*.{ets,dets}", Dir)],
    case rabbit_vhost:list_names() of
        %% The metadata store may not be ready yet.
        [] ->
            ok;
        VHosts ->
            Expected = maps:from_keys(
                         lists:append([storage_paths(Dir, V) || V <- VHosts]), true),
            case [F || F <- Files, not maps:is_key(F, Expected)] of
                [] ->
                    ok;
                Orphaned ->
                    ?LOG_INFO("Deleting MQTT retained message stores of deleted vhosts: ~tp",
                              [Orphaned]),
                    case rabbit_file:recursive_delete(Orphaned) of
                        ok ->
                            ok;
                        {error, Reason} ->
                            ?LOG_WARNING("Failed to delete MQTT retained message stores "
                                         "of deleted vhosts: ~tp", [Reason])
                    end
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
