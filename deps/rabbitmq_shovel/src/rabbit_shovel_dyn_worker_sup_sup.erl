%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.
%%

-module(rabbit_shovel_dyn_worker_sup_sup).
-behaviour(mirrored_supervisor).

-export([start_link/0, init/1, adjust/2, stop_child/1, cleanup_specs/0]).

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
    %% older format, pre 3.13.0 and 3.12.8. See rabbitmq/rabbitmq-server#9894.
    OldId = old_id(Name),
    lists:any(fun ({ChildId, _, _, _}) ->
                      ChildId =:= Id orelse ChildId =:= OldId
              end,
              mirrored_supervisor:which_children(?SUPERVISOR)).

stop_child({VHost, ShovelName} = Name) ->
    rabbit_log_shovel:debug("Asked to stop a dynamic Shovel named '~ts' in virtual host '~ts'", [ShovelName, VHost]),
    LockId = rabbit_shovel_locks:lock(Name),
    case get({shovel_worker_autodelete, Name}) of
        true -> ok; %% [1]
        _ ->
            case stop_and_delete_child(id(Name)) of
                ok ->
                    ok;
                {error, not_found} ->
                    case rabbit_khepri:is_enabled() of
                        true ->
                            %% Old id format is not supported by and cannot exist in Khepri
                            ok;
                        false ->
                            %% try older format, pre 3.13.0 and 3.12.8.
                            %% See rabbitmq/rabbitmq-server#9894.
                            _ = stop_and_delete_child(old_id(Name)),
                            ok
                    end
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

    ChildIdSet = sets:from_list([element(1, S) || S <- Children]),
    ParamsSet = params_to_child_ids(rabbit_khepri:is_enabled()),
    F = fun(ChildId, ok) ->
            try
                %% The supervisor operation is very unlikely to fail, it's the schema
                %% data stores that can make a fuss about a non-existent or non-standard value passed in.
                %% For example, an old style Shovel name is an invalid Khepri query path element. MK.
                _ = mirrored_supervisor:delete_child(?SUPERVISOR, ChildId)
            catch _:_:_Stacktrace ->
                ok
            end,
            ok
        end,
    %% Delete any supervisor children that do not have their respective runtime parameters in the database.
    SetToCleanUp = sets:subtract(ChildIdSet, ParamsSet),
    ok = sets:fold(F, ok, SetToCleanUp).

params_to_child_ids(_KhepriEnabled = true) ->
    %% Old id format simply cannot exist in Khepri because having Khepri enabled
    %% means a cluster-wide move to 3.13+, so we can conditionally compute what specs we care about. MK.
    sets:from_list([id({proplists:get_value(vhost, S), proplists:get_value(name, S)})
                    || S <- rabbit_runtime_parameters:list_component(<<"shovel">>)]);
params_to_child_ids(_KhepriEnabled = false) ->
    sets:from_list(
      lists:flatmap(
        fun(S) ->
                Name = {proplists:get_value(vhost, S), proplists:get_value(name, S)},
                %% Supervisor Id format was different pre 3.13.0 and 3.12.8.
                %% Try both formats to cover the transitionary mixed version cluster period.
                [id(Name), old_id(Name)]
        end,
        rabbit_runtime_parameters:list_component(<<"shovel">>))).

%%----------------------------------------------------------------------------

init([]) ->
    {ok, {{one_for_one, 3, 10}, []}}.

id({V, S} = Name) ->
    {[V, S], Name}.

%% older format, pre 3.13.0 and 3.12.8. See rabbitmq/rabbitmq-server#9894
old_id({_V, _S} = Name) ->
    Name.
