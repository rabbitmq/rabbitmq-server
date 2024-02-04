%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_shovel_dyn_worker_sup_sup).
-behaviour(mirrored_supervisor).

-export([start_link/0, init/1, adjust/2, stop_child/1, cleanup_specs/0]).
-export([id_to_khepri_path/1]).

-import(rabbit_misc, [pget/2]).
-import(rabbit_data_coercion, [to_map/1, to_list/1]).

-include("rabbit_shovel.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").
-define(SUPERVISOR, ?MODULE).

start_link() ->
    Pid = case mirrored_supervisor:start_link(
                  {local, ?SUPERVISOR}, ?SUPERVISOR,
                  ?MODULE, []) of
            {ok, Pid0}                       -> Pid0;
            {error, {already_started, Pid0}} -> Pid0
          end,
    Shovels = rabbit_runtime_parameters:list_component(<<"shovel">>),
    [start_child({pget(vhost, Shovel), pget(name, Shovel)},
                 pget(value, Shovel)) || Shovel <- Shovels],
    {ok, Pid}.

adjust(Name, Def) ->
    case child_exists(Name) of
        true  -> stop_child(Name);
        false -> ok
    end,
    start_child(Name, Def).

start_child({VHost, ShovelName} = Name, Def) ->
    rabbit_log_shovel:debug("Asked to start a dynamic Shovel named '~ts' in virtual host '~ts'", [ShovelName, VHost]),
    LockId = rabbit_shovel_locks:lock(Name),
    cleanup_specs(),
    rabbit_log_shovel:debug("Starting a mirrored supervisor named '~ts' in virtual host '~ts'", [ShovelName, VHost]),
    case child_exists(Name)
        orelse mirrored_supervisor:start_child(
           ?SUPERVISOR,
           {id(Name), {rabbit_shovel_dyn_worker_sup, start_link, [Name, obfuscated_uris_parameters(Def)]},
            transient, ?WORKER_WAIT, worker, [rabbit_shovel_dyn_worker_sup]}) of
        true                             -> ok;
        {ok,                      _Pid}  -> ok;
        {error, {already_started, _Pid}} -> ok
    end,
    %% release the lock if we managed to acquire one
    rabbit_shovel_locks:unlock(LockId),
    ok.

obfuscated_uris_parameters(Def) when is_map(Def) ->
    to_map(rabbit_shovel_parameters:obfuscate_uris_in_definition(to_list(Def)));
obfuscated_uris_parameters(Def) when is_list(Def) ->
    rabbit_shovel_parameters:obfuscate_uris_in_definition(Def).

child_exists(Name) ->
    Id = id(Name),
    TmpExpId = temp_experimental_id(Name),
    lists:any(fun ({ChildId, _, _, _}) ->
                      ChildId =:= Id orelse ChildId =:= TmpExpId
              end,
              mirrored_supervisor:which_children(?SUPERVISOR)).

stop_child({VHost, ShovelName} = Name) ->
    rabbit_log_shovel:debug("Asked to stop a dynamic Shovel named '~ts' in virtual host '~ts'", [ShovelName, VHost]),
    LockId = rabbit_shovel_locks:lock(Name),
    case get({shovel_worker_autodelete, Name}) of
        true -> ok; %% [1]
        _ ->
            Id = id(Name),
            case stop_and_delete_child(Id) of
                ok ->
                    ok;
                {error, not_found} ->
                    TmpExpId = temp_experimental_id(Name),
                    _ = stop_and_delete_child(TmpExpId),
                    ok
            end,
            rabbit_shovel_status:remove(Name)
    end,
    rabbit_shovel_locks:unlock(LockId),
    ok.

stop_and_delete_child(Id) ->
    case mirrored_supervisor:terminate_child(?SUPERVISOR, Id) of
        ok ->
            ok = mirrored_supervisor:delete_child(?SUPERVISOR, Id);
        {error, not_found} = Error ->
            Error
    end.

%% [1] An autodeleting worker removes its own parameter, and thus ends
%% up here via the parameter callback. It is a transient worker that
%% is just about to terminate normally - so we don't need to tell the
%% supervisor to stop us - and as usual if we call into our own
%% supervisor we risk deadlock.
%%
%% See rabbit_shovel_worker:terminate/2

cleanup_specs() ->
    Children = mirrored_supervisor:which_children(?SUPERVISOR),
    ParamsSet = sets:from_list(
                  [id({proplists:get_value(vhost, S),
                       proplists:get_value(name, S)})
                   || S <- rabbit_runtime_parameters:list_component(
                             <<"shovel">>)]),
    %% Delete any supervisor children that do not have their respective runtime parameters in the database.
    lists:foreach(
      fun
          ({{VHost, ShovelName} = ChildId, _, _, _})
            when is_binary(VHost) andalso is_binary(ShovelName) ->
              case sets:is_element(ChildId, ParamsSet) of
                  false ->
                      _ = mirrored_supervisor:delete_child(
                            ?SUPERVISOR, ChildId);
                  true ->
                      ok
              end;
          ({{List, {VHost, ShovelName} = Id} = ChildId, _, _, _})
            when is_list(List) andalso
                 is_binary(VHost) andalso is_binary(ShovelName) ->
              case sets:is_element(Id, ParamsSet) of
                  false ->
                      _ = mirrored_supervisor:delete_child(
                            ?SUPERVISOR, ChildId);
                  true ->
                      ok
              end
        end, Children).

%%----------------------------------------------------------------------------

init([]) ->
    {ok, {{one_for_one, 3, 10}, []}}.

id({VHost, ShovelName} = Name)
  when is_binary(VHost) andalso is_binary(ShovelName) ->
    Name.

id_to_khepri_path({VHost, ShovelName})
  when is_binary(VHost) andalso is_binary(ShovelName) ->
    [VHost, ShovelName];
id_to_khepri_path({List, {VHost, ShovelName}})
  when is_list(List) andalso is_binary(VHost) andalso is_binary(ShovelName) ->
    [VHost, ShovelName].

%% Temporary experimental format, erroneously backported to some 3.11.x and
%% 3.12.x releases in rabbitmq/rabbitmq-server#9796.
%%
%% See rabbitmq/rabbitmq-server#10306.
temp_experimental_id({V, S} = Name) ->
    {[V, S], Name}.
