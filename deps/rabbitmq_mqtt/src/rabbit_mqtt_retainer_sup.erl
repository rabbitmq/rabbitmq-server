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
         start_child_for_vhost/1, start_child_for_vhost/2,
         delete_child_for_vhost/1, delete_child_for_vhost/2, register_retainer/3]).

-spec start_link() -> supervisor:startlink_ret().
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

-spec start_child_for_vhost(rabbit_types:vhost()) -> pid() | {error, term()}.
start_child_for_vhost(VHost) when is_binary(VHost) ->
    resolve(VHost, any).

-spec start_child_for_vhost(rabbit_types:vhost(), vhost:id() | undefined) -> pid() | {error, term()}.
start_child_for_vhost(VHost, Id) when is_integer(Id); Id =:= undefined ->
    resolve(VHost, Id).

resolve(VHost, Expected) ->
    try
        case whereis(?MODULE) of
            undefined -> {error, {noproc, ?MODULE}};
            _ ->
                case rabbit_db_vhost:get(VHost) of
                    undefined -> {error, not_found};
                    V ->
                        Id = vhost:get_id(V),
                        case Expected =:= any orelse Expected =:= Id of
                            true -> lookup(VHost, Id);
                            false -> {error, stale_vhost_id}
                        end
                end
        end
    catch
        exit:Reason -> {error, Reason};
        error:badarg -> {error, retainer_unavailable}
    end.

lookup(VHost, Id) ->
    case ets:lookup(?MODULE, VHost) of
        [{VHost, Id, _, Pid}] ->
            case is_process_alive(Pid) of
                true -> Pid;
                false -> replace_child(VHost, Id)
            end;
        _ -> replace_child(VHost, Id)
    end.

replace_child(VHost, Id) ->
    with_vhost(VHost, fun() ->
        case rabbit_db_vhost:identity(VHost) of
            {ok, Id, StorageId} -> ensure_child(VHost, Id, StorageId);
            {ok, _, _} -> {error, stale_vhost_id};
            {error, _} = Error -> Error
        end
    end).

ensure_child(VHost, Id, StorageId) ->
    case ets:lookup(?MODULE, VHost) of
        [{VHost, _, StorageId, Pid}] ->
            case is_process_alive(Pid) of
                true ->
                    ets:insert(?MODULE, {VHost, Id, StorageId, Pid}),
                    Pid;
                false -> start_child(VHost, StorageId)
            end;
        _ ->
            [delete_child(VHost, Old) || {{V, Old}, _, _, _} <- supervisor:which_children(?MODULE),
                                        V =:= VHost, Old =/= StorageId],
            start_child(VHost, StorageId)
    end.

start_child(VHost, Id) ->
    Result = case supervisor:start_child(?MODULE, child_spec(VHost, Id)) of
                 {error, already_present} -> supervisor:restart_child(?MODULE, {VHost, Id});
                 Other -> Other
             end,
    case Result of
        {ok, Pid} when is_pid(Pid) -> Pid;
        {error, {already_started, Pid}} -> Pid;
        {error, running} ->
            case ets:lookup(?MODULE, VHost) of
                [{VHost, _, Id, Pid}] -> Pid;
                _ -> {error, retainer_unavailable}
            end;
        {ok, undefined} ->
            _ = supervisor:delete_child(?MODULE, {VHost, Id}),
            {error, retainer_unavailable};
        {error, _} = Error -> Error
    end.

-spec register_retainer(rabbit_types:vhost(), vhost:id() | undefined, vhost:id() | legacy) -> true.
register_retainer(VHost, Id, StorageId) ->
    ets:insert(?MODULE, {VHost, Id, StorageId, self()}).

-spec delete_child_for_vhost(rabbit_types:vhost()) -> ok.
delete_child_for_vhost(VHost) ->
    with_vhost(VHost, fun() ->
        [delete_child(VHost, Id) || {{V, Id}, _, _, _} <- supervisor:which_children(?MODULE),
                                   V =:= VHost],
        ok
    end).

-spec delete_child_for_vhost(rabbit_types:vhost(), vhost:id() | undefined) -> ok | {error, term()}.
delete_child_for_vhost(_VHost, undefined) -> ok;
delete_child_for_vhost(VHost, Id) ->
    case rabbit_db_vhost:storage_id(Id) of
        {ok, StorageId} -> with_vhost(VHost, fun() -> delete_child(VHost, StorageId) end);
        {error, _} = Error -> Error
    end.

delete_child(VHost, Id) ->
    case supervisor:terminate_child(?MODULE, {VHost, Id}) of
        ok ->
            case supervisor:delete_child(?MODULE, {VHost, Id}) of
                ok -> ok;
                {error, not_found} -> ok
            end;
        {error, not_found} -> ok
    end,
    case ets:lookup(?MODULE, VHost) of
        [{VHost, _, Id, _} = Entry] ->
            ets:delete_object(?MODULE, Entry),
            ok;
        _ -> ok
    end.

with_vhost(VHost, Fun) ->
    global:trans({{?MODULE, VHost}, self()}, Fun, [node()]).

-spec init([]) -> {ok, {supervisor:sup_flags(), [supervisor:child_spec()]}}.
init([]) ->
    _ = ets:new(?MODULE, [named_table, public, {read_concurrency, true}]),
    {ok, {#{intensity => 5, period => 5}, []}}.

child_spec(VHost, Id) ->
    #{id => {VHost, Id},
      start => {rabbit_mqtt_retainer, start_link, [VHost, {incarnation, Id}]},
      restart => transient,
      shutdown => 120_000}.
