%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_db_msup).

-include_lib("khepri/include/khepri.hrl").
-include("mirrored_supervisor.hrl").

-include("include/rabbit_khepri.hrl").
-include_lib("kernel/include/logger.hrl").

-export([
         create_or_update/5,
         find_mirror/2,
         update_all/2,
         delete/2,
         delete_all/1
        ]).

-export([clear/0]).

-export([khepri_mirrored_supervisor_path/2]).

%% -------------------------------------------------------------------
%% create_or_update().
%% -------------------------------------------------------------------

-spec create_or_update(Group, Overall, Delegate, ChildSpec, Id) -> Ret when
      Group :: mirrored_supervisor:group_name(),
      Overall :: pid(),
      Delegate :: pid() | undefined,
      ChildSpec :: supervisor2:child_spec(),
      Id :: mirrored_supervisor:child_id(),
      Ret :: start | undefined | pid().

create_or_update(Group, Overall, Delegate, ChildSpec, Id) ->
    Path = khepri_mirrored_supervisor_path(Group, Id),
    S = #mirrored_sup_childspec{key           = {Group, Id},
                                mirroring_pid = Overall,
                                childspec     = ChildSpec},
    case rabbit_khepri:adv_get(Path) of
        {ok, #{Path := #{data := #mirrored_sup_childspec{mirroring_pid = Pid},
                         payload_version := Vsn}}} ->
            case Overall of
                Pid ->
                    Delegate;
                _   ->
                    %% The supervisor(Pid) call can't happen inside of a transaction.
                    %% We have to read and update the record in two different khepri calls
                    case mirrored_supervisor:supervisor(Pid) of
                        dead ->
                            UpdatePath =
                                khepri_path:combine_with_conditions(
                                  Path, [#if_payload_version{version = Vsn}]),
                            Ret = rabbit_khepri:put(UpdatePath, S),
                            case Ret of
                                ok -> start;
                                {error, {khepri, mismatching_node, _}} ->
                                    create_or_update(Group, Overall, Delegate, ChildSpec, Id);
                                {error, _} = Error -> Error
                            end;
                        Delegate0 ->
                            Delegate0
                    end
            end;
        _  ->
            %% FIXME: Not atomic with the get above.
            ok = rabbit_khepri:put(Path, S),
            start
    end.

%% -------------------------------------------------------------------
%% delete().
%% -------------------------------------------------------------------

-spec delete(Group, Id) -> ok when
      Group :: mirrored_supervisor:group_name(),
      Id :: mirrored_supervisor:child_id().

delete(Group, Id) ->
    ok = rabbit_khepri:delete(khepri_mirrored_supervisor_path(Group, Id)).

%% -------------------------------------------------------------------
%% find_mirror().
%% -------------------------------------------------------------------

-spec find_mirror(Group, Id) -> Ret when
      Group :: mirrored_supervisor:group_name(),
      Id :: mirrored_supervisor:child_id(),
      Ret :: {ok, pid()} | {error, not_found}.

find_mirror(Group, Id) ->
    case rabbit_khepri:get(khepri_mirrored_supervisor_path(Group, Id)) of
        {ok, #mirrored_sup_childspec{mirroring_pid = Pid}} ->
            {ok, Pid};
        _ ->
            {error, not_found}
    end.

%% -------------------------------------------------------------------
%% update_all().
%% -------------------------------------------------------------------

-spec update_all(Overall, Overall) -> [ChildSpec] | Error when
      Overall :: pid(),
      ChildSpec :: supervisor2:child_spec(),
      Error :: {error, any()}.

update_all(Overall, OldOverall) ->
    Pattern = #mirrored_sup_childspec{mirroring_pid = OldOverall,
                                      _             = '_'},
    Conditions = [?KHEPRI_WILDCARD_STAR_STAR, #if_data_matches{pattern = Pattern}],
    PathPattern = khepri_mirrored_supervisor_path(
                    ?KHEPRI_WILDCARD_STAR,
                    #if_all{conditions = Conditions}),
    rabbit_khepri:transaction(
      fun() ->
              case khepri_tx:get_many(PathPattern) of
                  {ok, Map} ->
                      [begin
                           S = S0#mirrored_sup_childspec{mirroring_pid = Overall},
                           ok = khepri_tx:put(Path, S),
                           S0#mirrored_sup_childspec.childspec
                       end || {Path, S0} <- maps:to_list(Map)]
              end
      end).

%% -------------------------------------------------------------------
%% delete_all().
%% -------------------------------------------------------------------

-spec delete_all(Group) -> ok when
      Group :: mirrored_supervisor:group_name().

delete_all(Group) ->
    Pattern = #mirrored_sup_childspec{key = {Group, '_'},
                                      _   = '_'},
    Conditions = [?KHEPRI_WILDCARD_STAR_STAR, #if_data_matches{pattern = Pattern}],
    rabbit_khepri:delete(khepri_mirrored_supervisor_path(
                           ?KHEPRI_WILDCARD_STAR,
                           #if_all{conditions = Conditions})).

%% -------------------------------------------------------------------
%% clear().
%% -------------------------------------------------------------------

-spec clear() -> ok.

clear() ->
    Path = khepri_mirrored_supervisor_path(
             ?KHEPRI_WILDCARD_STAR, ?KHEPRI_WILDCARD_STAR_STAR),
    case rabbit_khepri:delete(Path) of
        ok -> ok;
        Error -> throw(Error)
    end.

%% -------------------------------------------------------------------
%% Khepri paths
%% -------------------------------------------------------------------

khepri_mirrored_supervisor_path(Group, Id)
  when ?IS_KHEPRI_PATH_CONDITION(Group) andalso
       ?IS_KHEPRI_PATH_CONDITION(Id) ->
    ?RABBITMQ_KHEPRI_MIRRORED_SUPERVISOR_PATH(Group, Id);
khepri_mirrored_supervisor_path(Group, Id)
  when is_atom(Group) ->
    IdPath = Group:id_to_khepri_path(Id),
    ?RABBITMQ_KHEPRI_ROOT_PATH ++ [mirrored_supervisors, Group] ++ IdPath.
