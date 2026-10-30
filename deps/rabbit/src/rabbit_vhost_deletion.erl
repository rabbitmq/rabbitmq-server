%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

%% Lets components remove the data they keep for a virtual host after it has
%% been deleted. Deletion does not wait for them.
%%
%% Both callbacks run on each node, outside the deletion, and must be
%% idempotent. `vhost_deleted/2' is best-effort: it is not called on nodes that
%% are down at the time, or for components that are not running.
%% `remove_leftovers/1' then runs when the component starts. Data that must not
%% be visible to a vhost recreated with the same name has to be checked against
%% the vhost id where it is read.
-module(rabbit_vhost_deletion).

-include_lib("kernel/include/logger.hrl").

-export([vhost_deleted/2, remove_leftovers/1, deleted_vhosts/1]).

%% Cleanup requires an identified incarnation from the current metadata store.
-callback vhost_deleted(rabbit_types:vhost(), vhost:id() | undefined) -> ok.

%% `Live` is advisory. Recheck captured candidates with `deleted_vhosts/1` before deletion.
-callback remove_leftovers(Live :: #{rabbit_types:vhost() => vhost:id() | undefined}) -> ok.

-spec vhost_deleted(rabbit_types:vhost(), vhost:id() | undefined) -> ok.
vhost_deleted(VHost, Id) ->
    Handlers = handlers(started_apps()),
    _ = spawn(fun() ->
                      case deleted_vhosts([{VHost, Id}]) of
                          {ok, [{VHost, Id}]} ->
                              [run(Mod, vhost_deleted, [VHost, Id]) || Mod <- Handlers];
                          {ok, []} -> ok;
                          {error, Reason} ->
                              ?LOG_WARNING("Cleanup of deleted virtual host '~ts' deferred: ~tp",
                                           [VHost, Reason])
                      end
              end),
    ok.

%% Runs the callbacks of the given applications once they have started.
-spec remove_leftovers([atom()]) -> ok.
remove_leftovers(Apps) ->
    case handlers(Apps) of
        [] ->
            ok;
        Handlers ->
            _ = spawn(fun() ->
                              case rabbit_db_vhost:list_ids() of
                                  {ok, Live} ->
                                      [run(Mod, remove_leftovers, [Live]) || Mod <- Handlers];
                                  {error, Reason} ->
                                      ?LOG_WARNING("Not removing data of deleted virtual hosts, "
                                                   "virtual hosts cannot be listed: ~tp", [Reason])
                              end
                      end),
            ok
    end.

%% Capture candidates before reading metadata, so a newly created vhost cannot become a candidate.
-spec deleted_vhosts([{rabbit_types:vhost(), vhost:id() | undefined}]) ->
    {ok, [{rabbit_types:vhost(), vhost:id()}]} | {error, term()}.
deleted_vhosts(Candidates) ->
    case rabbit_db_vhost:cleanup_snapshot() of
        {ok, Epoch, Live} ->
            {ok, [{VHost, Id} || {VHost, Id} <- Candidates, is_integer(Id), vhost:id_epoch(Id) =:= Epoch,
                                case maps:find(VHost, Live) of
                                    error -> true;
                                    {ok, Current} when is_integer(Current) -> Current > Id;
                                    _ -> false
                                end]};
        {error, _} = Error -> Error
    end.

run(Mod, Fun, Args) ->
    try erlang:apply(Mod, Fun, Args)
    catch Class:Reason:Stacktrace ->
        ?LOG_WARNING("~ts:~ts failed to remove data of deleted virtual hosts: ~tp",
                     [Mod, Fun, {Class, Reason, Stacktrace}])
    end.

handlers(Apps) ->
    [Mod || {_App, Mod, Behaviours} <- rabbit_misc:module_attributes_from_apps(behaviour, Apps),
            lists:member(?MODULE, Behaviours)].

started_apps() ->
    [App || {App, _, _} <- application:which_applications()].
