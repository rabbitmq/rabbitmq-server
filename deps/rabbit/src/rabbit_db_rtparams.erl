%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_db_rtparams).

-include_lib("khepri/include/khepri.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

-include("include/rabbit_khepri.hrl").

-export([set/2, set/4,
         get/1,
         get_all/0, get_all/2,
         delete/1, delete/3,
         delete_vhost/1]).

-export([khepri_vhost_rp_path/3,
         khepri_global_rp_path/1
        ]).

-define(KHEPRI_GLOBAL_PROJECTION, rabbit_khepri_global_rtparam).
-define(KHEPRI_VHOST_PROJECTION, rabbit_khepri_per_vhost_rtparam).
-define(any(Value), case Value of
                        '_' -> ?KHEPRI_WILDCARD_STAR;
                        _ -> Value
                    end).

%% -------------------------------------------------------------------
%% set().
%% -------------------------------------------------------------------

-spec set(Key, Term) -> Ret when
      Key :: atom(),
      Term :: any(),
      Ret :: new | {old, Term}.
%% @doc Sets the new value of the global runtime parameter named `Key'.
%%
%% @returns `new' if the runtime parameter was not set before, or `{old,
%% OldTerm}' with the old value.
%%
%% @private

set(Key, Term) when is_atom(Key) ->
    Path = khepri_rp_path(Key),
    Record = #runtime_parameters{key   = Key,
                                 value = Term},
    case rabbit_khepri:adv_put(Path, Record) of
        {ok, #{Path := #{data := Params}}} ->
            {old, Params#runtime_parameters.value};
        {ok, _} ->
            new
    end.

-spec set(VHostName, Comp, Name, Term) -> Ret when
      VHostName :: vhost:name(),
      Comp :: binary(),
      Name :: binary() | atom(),
      Term :: any(),
      Ret :: new | {old, Term}.
%% @doc Checks the existence of `VHostName' and sets the new value of the
%% non-global runtime parameter named `Key'.
%%
%% @returns `new' if the runtime parameter was not set before, or `{old,
%% OldTerm}' with the old value.
%%
%% @private

set(VHostName, Comp, Name, Term)
  when is_binary(VHostName) andalso
       is_binary(Comp) andalso
       (is_binary(Name) orelse is_atom(Name)) ->
    Key = {VHostName, Comp, Name},
    rabbit_khepri:transaction(
      rabbit_db_vhost:with_fun_in_khepri_tx(
        VHostName, fun() -> set_in_khepri_tx(Key, Term) end), rw).

set_in_khepri_tx(Key, Term) ->
    Path = khepri_rp_path(Key),
    Record = #runtime_parameters{key   = Key,
                                 value = Term},
    UsesUniformWriteRet = try
                              khepri_tx:does_api_comply_with(uniform_write_ret)
                          catch
                              error:undef ->
                                  false
                          end,
    case khepri_tx_adv:put(Path, Record) of
        {ok, #{Path := #{data := Params}}} when UsesUniformWriteRet ->
            {old, Params#runtime_parameters.value};
        {ok, #{data := Params}} when not UsesUniformWriteRet ->
            {old, Params#runtime_parameters.value};
        {ok, _} ->
            new
    end.

%% -------------------------------------------------------------------
%% get().
%% -------------------------------------------------------------------

-spec get(Key) -> Ret when
      Key :: atom() | {vhost:name(), binary(), binary()},
      Ret :: #runtime_parameters{} | undefined.
%% @doc Returns a runtime parameter.
%%
%% @returns the value of the runtime parameter if it exists, or `undefined'
%% otherwise.
%%
%% @private

get({VHostName, Comp, Name} = Key)
  when is_binary(VHostName) andalso
       is_binary(Comp) andalso
       (is_binary(Name) orelse is_atom(Name)) ->
    do_get(Key);
get(Key) when is_atom(Key) ->
    do_get(Key).

do_get(Key) when is_atom(Key) ->
    try ets:lookup(?KHEPRI_GLOBAL_PROJECTION, Key) of
        []       -> undefined;
        [Record] -> Record
    catch
        error:badarg ->
            undefined
    end;
do_get(Key) when is_tuple(Key) ->
    try ets:lookup(?KHEPRI_VHOST_PROJECTION, Key) of
        []       -> undefined;
        [Record] -> Record
    catch
        error:badarg ->
            undefined
    end.

%% -------------------------------------------------------------------
%% get_all().
%% -------------------------------------------------------------------

-spec get_all() -> Ret when
      Ret :: [#runtime_parameters{}].
%% @doc Gets all runtime parameters.
%%
%% @returns a list of runtime parameters records.
%%
%% @private

get_all() ->
    try
        ets:tab2list(?KHEPRI_GLOBAL_PROJECTION) ++
        ets:tab2list(?KHEPRI_VHOST_PROJECTION)
    catch
        error:badarg ->
            []
    end.

-spec get_all(VHostName, Comp) -> Ret when
      VHostName :: vhost:name() | '_',
      Comp :: binary() | '_',
      Ret :: [#runtime_parameters{}].
%% @doc Gets all non-global runtime parameters matching the given virtual host
%% and component.
%%
%% @returns a list of runtime parameters records.
%%
%% @private

get_all(VHostName, Comp)
  when (is_binary(VHostName) orelse VHostName =:= '_') andalso
       (is_binary(Comp) orelse Comp =:= '_') ->
    case VHostName of
        '_' -> ok;
        _   -> rabbit_vhost:assert(VHostName)
    end,
    try
        Match = #runtime_parameters{key = {VHostName, Comp, '_'},
                                    _   = '_'},
        ets:match_object(?KHEPRI_VHOST_PROJECTION, Match)
    catch
        error:badarg ->
            []
    end.

%% -------------------------------------------------------------------
%% delete().
%% -------------------------------------------------------------------

-spec delete(Key) -> ok when
      Key :: atom().
%% @doc Deletes the global runtime parameter named `Key'.
%%
%% @private

delete(Key) when is_atom(Key) ->
    do_delete(Key).

-spec delete(VHostName, Comp, Name) -> ok when
      VHostName :: vhost:name() | '_',
      Comp :: binary() | '_',
      Name :: binary() | atom() | '_'.
%% @doc Deletes the non-global runtime parameter named `Name' for the given
%% virtual host and component.
%%
%% @private

delete(VHostName, Comp, Name)
  when is_binary(VHostName) andalso
       is_binary(Comp) andalso
       (is_binary(Name) orelse (is_atom(Name) andalso Name =/= '_')) ->
    Key = {VHostName, Comp, Name},
    do_delete(Key);
delete(VHostName, Comp, Name)
  when VHostName =:= '_' orelse Comp =:= '_' orelse Name =:= '_' ->
    Key = {?any(VHostName), ?any(Comp), ?any(Name)},
    do_delete(Key).

do_delete(Key) ->
    Path = khepri_rp_path(Key),
    ok = rabbit_khepri:delete(Path).

%% -------------------------------------------------------------------
%% delete_vhost().
%% -------------------------------------------------------------------

-spec delete_vhost(VHostName) -> Ret when
      VHostName :: vhost:name(),
      Ret :: {ok, Deletions} | {error, Reason :: any()},
      Deletions :: [#runtime_parameters{}].
%% @doc Deletes all runtime parameters belonging to the given virtual host.
%%
%% @returns an OK tuple containing the deleted runtime parameters if
%% successful, or an error tuple otherwise.
%%
%% @private

delete_vhost(VHostName) when is_binary(VHostName) ->
    Pattern = khepri_vhost_rp_path(
                VHostName, ?KHEPRI_WILDCARD_STAR, ?KHEPRI_WILDCARD_STAR),
    case rabbit_khepri:adv_delete(Pattern) of
        {ok, NodePropsMap} ->
            RTParams =
            maps:fold(
              fun(Path, Props, Acc) ->
                      case {Path, Props} of
                          {?RABBITMQ_KHEPRI_VHOST_RUNTIME_PARAM_PATH(
                             VHostName, _, _),
                           #{data := RTParam}} ->
                              [RTParam | Acc];
                          {_, _} ->
                              Acc
                      end
              end, [], NodePropsMap),
            {ok, RTParams};
        {error, _} = Err ->
            Err
    end.

%% -------------------------------------------------------------------

khepri_rp_path({VHost, Component, Name}) ->
    khepri_vhost_rp_path(VHost, Component, Name);
khepri_rp_path(Key) ->
    khepri_global_rp_path(Key).

khepri_global_rp_path(Key) when ?IS_KHEPRI_PATH_CONDITION(Key) ->
    ?RABBITMQ_KHEPRI_GLOBAL_RUNTIME_PARAM_PATH(Key).

khepri_vhost_rp_path(VHost, Component, Name)
  when ?IS_KHEPRI_PATH_CONDITION(Component) andalso
       ?IS_KHEPRI_PATH_CONDITION(Name) ->
    ?RABBITMQ_KHEPRI_VHOST_RUNTIME_PARAM_PATH(VHost, Component, Name).
