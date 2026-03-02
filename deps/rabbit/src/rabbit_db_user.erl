%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_db_user).

-include_lib("stdlib/include/assert.hrl").

-include_lib("khepri/include/khepri.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

-include("include/rabbit_khepri.hrl").

-export([create/1,
         update/2,
         get/1,
         get_all/0,
         count_all/0,
         with_fun_in_khepri_tx/2,
         get_user_permissions/2,
         check_and_match_user_permissions/2,
         set_user_permissions/1,
         clear_user_permissions/2,
         clear_matching_user_permissions/2,
         get_topic_permissions/3,
         check_and_match_topic_permissions/3,
         set_topic_permissions/1,
         clear_topic_permissions/3,
         clear_matching_topic_permissions/3,
         delete/1,
         clear_all_permissions_for_vhost/1]).

-export([khepri_user_path/1,
         khepri_user_permission_path/2,
         khepri_topic_permission_path/3]).

%% for testing
-export([clear/0]).

-define(KHEPRI_USERS_PROJECTION, rabbit_khepri_user).
-define(KHEPRI_PERMISSIONS_PROJECTION, rabbit_khepri_user_permission).

%% -------------------------------------------------------------------
%% create().
%% -------------------------------------------------------------------

-spec create(User) -> ok when
      User :: internal_user:internal_user().
%% @doc Creates and writes an internal user record.
%%
%% @returns `ok' if the record was written to the database. It throws an
%% exception if the user already exists or the transaction fails.
%%
%% @private

create(User) ->
    Username = internal_user:get_username(User),
    Path = khepri_user_path(Username),
    case rabbit_khepri:create(Path, User) of
        ok ->
            ok;
        {error, {khepri, mismatching_node, _}} ->
            throw({error, {user_already_exists, Username}});
        {error, _} = Error ->
            throw(Error)
    end.

%% -------------------------------------------------------------------
%% update().
%% -------------------------------------------------------------------

-spec update(Username, UpdateFun) -> ok when
      Username :: internal_user:username(),
      User :: internal_user:internal_user(),
      UpdateFun :: fun((User) -> User).
%% @doc Updates an existing internal user record using `UpdateFun'.
%%
%% @private

update(Username, UpdateFun)
  when is_binary(Username) andalso is_function(UpdateFun, 1) ->
    rabbit_khepri:transaction(
      fun () ->
              Path = khepri_user_path(Username),
              case khepri_tx:get(Path) of
                  {ok, User} ->
                      case khepri_tx:put(Path, UpdateFun(User)) of
                          ok -> ok;
                          Error -> khepri_tx:abort(Error)
                      end;
                  _ ->
                      khepri_tx:abort({no_such_user, Username})
              end
      end).

%% -------------------------------------------------------------------
%% get().
%% -------------------------------------------------------------------

-spec get(Username) -> User | undefined when
      Username :: internal_user:username(),
      User :: internal_user:internal_user().
%% @doc Returns the record of the internal user named `Username'.
%%
%% @returns the internal user record or `undefined' if no internal user is named
%% `Username'.
%%
%% @private

get(Username) when is_binary(Username) ->
    try ets:lookup(?KHEPRI_USERS_PROJECTION, Username) of
        [User] -> User;
        _      -> undefined
    catch
        error:badarg ->
            undefined
    end.

%% -------------------------------------------------------------------
%% get_all().
%% -------------------------------------------------------------------

-spec get_all() -> [User] when
      User :: internal_user:internal_user().
%% @doc Returns all user records.
%%
%% @returns the list of internal user records.
%%
%% @private

get_all() ->
    Path = khepri_user_path(?KHEPRI_WILDCARD_STAR),
    case rabbit_khepri:get_many(Path) of
        {ok, Users} -> maps:values(Users);
        _           -> []
    end.

%% -------------------------------------------------------------------
%% count_all().
%% -------------------------------------------------------------------

-spec count_all() -> {ok, Count} | {error, any()} when
      Count :: non_neg_integer().
%% @doc Returns all user records.
%%
%% @returns the count of internal user records.
%%
%% @private

count_all() ->
    Path = khepri_user_path(?KHEPRI_WILDCARD_STAR),
    rabbit_khepri:count(Path).

%% -------------------------------------------------------------------
%% with_fun_in_*().
%% -------------------------------------------------------------------

with_fun_in_khepri_tx(Username, TxFun)
  when is_binary(Username) andalso is_function(TxFun, 0) ->
    fun() ->
            Path = khepri_user_path(Username),
            case khepri_tx:exists(Path) of
                true  -> TxFun();
                false -> khepri_tx:abort({no_such_user, Username})
            end
    end.

%% -------------------------------------------------------------------
%% get_user_permissions().
%% -------------------------------------------------------------------

-spec get_user_permissions(Username, VHostName) -> Ret when
      Username :: internal_user:username(),
      VHostName :: vhost:name(),
      Ret :: UserPermission | undefined,
      UserPermission :: #user_permission{}.
%% @doc Returns the user permissions for the given user in the given virtual
%% host.
%%
%% @returns the user permissions record if any, or `undefined'.
%%
%% @private

get_user_permissions(Username, VHostName)
  when is_binary(Username) andalso is_binary(VHostName) ->
    UserVHost = #user_vhost{username     = Username,
                            virtual_host = VHostName},
    try ets:lookup(?KHEPRI_PERMISSIONS_PROJECTION, UserVHost) of
        [UserPermission] ->
            UserPermission;
        _ ->
            undefined
    catch
        error:badarg ->
            undefined
    end.

%% -------------------------------------------------------------------
%% check_and_match_user_permissions().
%% -------------------------------------------------------------------

-spec check_and_match_user_permissions(Username, VHostName) -> Ret when
      Username :: internal_user:username() | '_',
      VHostName :: vhost:name() | '_',
      Ret :: [UserPermission],
      UserPermission :: #user_permission{}.
%% @doc Returns the user permissions matching the given user in the given virtual
%% host.
%%
%% @returns a list of user permission records.
%%
%% @private

check_and_match_user_permissions(Username, VHostName)
  when (is_binary(Username) orelse Username =:= '_') andalso
       (is_binary(VHostName) orelse VHostName =:= '_') ->
    match_user_permissions_in_khepri(Username, VHostName).

match_user_permissions_in_khepri('_' = _Username, '_' = _VHostName) ->
    Path = khepri_user_permission_path(?KHEPRI_WILDCARD_STAR, ?KHEPRI_WILDCARD_STAR),
    case rabbit_khepri:get_many(Path) of
        {ok, Map} ->
            maps:values(Map);
        _ ->
            []
    end;
match_user_permissions_in_khepri('_' = _Username, VHostName) ->
    rabbit_khepri:transaction(
        rabbit_db_vhost:with_fun_in_khepri_tx(
          VHostName,
          fun() ->
                  match_user_permissions_in_khepri_tx(?KHEPRI_WILDCARD_STAR, VHostName)
          end),
        ro);
match_user_permissions_in_khepri(Username, '_' = _VHostName) ->
    rabbit_khepri:transaction(
        with_fun_in_khepri_tx(
          Username,
          fun() ->
                  match_user_permissions_in_khepri_tx(Username, ?KHEPRI_WILDCARD_STAR)
          end),
        ro);
match_user_permissions_in_khepri(Username, VHostName) ->
    rabbit_khepri:transaction(
        with_fun_in_khepri_tx(
          Username,
          rabbit_db_vhost:with_fun_in_khepri_tx(
            VHostName,
            fun() ->
                    match_user_permissions_in_khepri_tx(Username, VHostName)
            end)),
        ro).

match_user_permissions_in_khepri_tx(Username, VHostName) ->
    Path = khepri_user_permission_path(Username, VHostName),
    case khepri_tx:get_many(Path) of
        {ok, Map} -> maps:values(Map);
        _         -> []
    end.

%% -------------------------------------------------------------------
%% set_user_permissions().
%% -------------------------------------------------------------------

-spec set_user_permissions(UserPermission) -> ok when
      UserPermission :: #user_permission{}.
%% @doc Sets the user permissions for the user and virtual host set in the
%% `UserPermission' record.
%%
%% @private

set_user_permissions(
  #user_permission{user_vhost = #user_vhost{username = Username,
                                            virtual_host = VHostName}}
  = UserPermission) ->
    rabbit_khepri:transaction(
      with_fun_in_khepri_tx(
        Username,
        rabbit_db_vhost:with_fun_in_khepri_tx(
          VHostName,
          fun() ->
                  set_user_permissions_in_khepri_tx(Username, VHostName, UserPermission)
          end)), rw).

set_user_permissions_in_khepri_tx(Username, VHostName, UserPermission) ->
    %% TODO: Check user presence in a transaction.
    Path = khepri_user_permission_path(
             Username,
             VHostName),
    Extra = #{keep_while =>
                  #{rabbit_db_user:khepri_user_path(Username) =>
                        #if_node_exists{exists = true}}},
    Ret = khepri_tx:put(
            Path, UserPermission, Extra),
    case Ret of
        ok      -> ok;
        Error   -> khepri_tx:abort(Error)
    end.

%% -------------------------------------------------------------------
%% clear_user_permissions().
%% -------------------------------------------------------------------

-spec clear_user_permissions(Username, VHostName) -> ok when
      Username :: internal_user:username(),
      VHostName :: vhost:name().
%% @doc Clears the user permissions of the given user in the given virtual
%% host.
%%
%% @private

clear_user_permissions(Username, VHostName)
  when is_binary(Username) andalso is_binary(VHostName) ->
    Path = khepri_user_permission_path(Username, VHostName),
    case rabbit_khepri:delete(Path) of
        ok      -> ok;
        Error   -> khepri_tx:abort(Error)
    end.

%% -------------------------------------------------------------------
%% clear_matching_user_permissions().
%% -------------------------------------------------------------------

-spec clear_matching_user_permissions(Username, VHostName) -> Ret when
      Username :: internal_user:username() | '_',
      VHostName :: vhost:name() | '_',
      Ret :: ok.
%% @doc Clears all user permissions matching arguments.
%%
%% @private

clear_matching_user_permissions(Username, VHostName)
  when (is_binary(Username) orelse Username =:= '_') andalso
       (is_binary(VHostName) orelse VHostName =:= '_') ->
    Path = khepri_user_permission_path(any(Username), any(VHostName)),
    ok = rabbit_khepri:delete(Path).

any('_') -> ?KHEPRI_WILDCARD_STAR;
any(Value) -> Value.

%% -------------------------------------------------------------------
%% clear_all_permissions_for_vhost().
%% -------------------------------------------------------------------

-spec clear_all_permissions_for_vhost(VHostName) -> Ret when
      VHostName :: vhost:name(),
      Ret :: {ok, DeletedPermissions} | {error, Reason :: any()},
      DeletedPermissions :: [#topic_permission{} | #user_permission{}].
%% @doc Transactionally deletes all user and topic permissions for a virtual
%% host, returning any permissions that were deleted.
%%
%% @returns an OK-tuple with the deleted permissions or an error tuple if the
%% operation could not be completed.
%%
%% @private

clear_all_permissions_for_vhost(VHostName) when is_binary(VHostName) ->
    rabbit_khepri:transaction(
      fun() ->
              clear_all_permissions_for_vhost_in_khepri_tx(VHostName)
      end, rw, #{timeout => infinity}).

clear_all_permissions_for_vhost_in_khepri_tx(VHostName) ->
    UserPermissionsPattern = khepri_user_permission_path(
                               ?KHEPRI_WILDCARD_STAR, VHostName),
    TopicPermissionsPattern = khepri_topic_permission_path(
                                ?KHEPRI_WILDCARD_STAR, VHostName,
                                ?KHEPRI_WILDCARD_STAR),
    {ok, UserNodePropsMap} = khepri_tx_adv:delete_many(UserPermissionsPattern),
    {ok, TopicNodePropsMap} = khepri_tx_adv:delete_many(
                                TopicPermissionsPattern),
    Deletions0 =
    maps:fold(
      fun(Path, Props, Acc) ->
              case {Path, Props} of
                  {?RABBITMQ_KHEPRI_USER_PERMISSION_PATH(VHostName, _),
                   #{data := Permission}} ->
                      [Permission | Acc];
                  {_, _} ->
                      Acc
              end
      end, [], UserNodePropsMap),
    Deletions1 =
    maps:fold(
      fun(Path, Props, Acc) ->
              case {Path, Props} of
                  {?RABBITMQ_KHEPRI_TOPIC_PERMISSION_PATH(VHostName, _, _),
                   #{data := Permission}} ->
                      [Permission | Acc];
                  {_, _} ->
                      Acc
              end
      end, Deletions0, TopicNodePropsMap),
    {ok, Deletions1}.

%% -------------------------------------------------------------------
%% get_topic_permissions().
%% -------------------------------------------------------------------

-spec get_topic_permissions(Username, VHostName, ExchangeName) -> Ret when
      Username :: rabbit_types:username(),
      VHostName :: vhost:name(),
      ExchangeName :: binary(),
      Ret :: TopicPermission | undefined,
      TopicPermission :: #topic_permission{}.
%% @doc Returns the topic permissions for the given user and exchange in the
%% given virtual host.
%%
%% @returns the topic permissions record if any, or `undefined'.
%%
%% @private

get_topic_permissions(Username, VHostName, ExchangeName)
  when is_binary(Username) andalso
       is_binary(VHostName) andalso
       is_binary(ExchangeName) ->
    Path = khepri_topic_permission_path(Username, VHostName, ExchangeName),
    case rabbit_khepri:get(Path) of
        {ok, TopicPermission} -> TopicPermission;
        _                     -> undefined
    end.

%% -------------------------------------------------------------------
%% check_and_match_topic_permissions().
%% -------------------------------------------------------------------

-spec check_and_match_topic_permissions(
        Username, VHostName, ExchangeName) -> Ret when
      Username :: internal_user:username() | '_',
      VHostName :: vhost:name() | '_',
      ExchangeName :: binary() | '_',
      Ret :: [TopicPermission],
      TopicPermission :: #topic_permission{}.
%% @doc Returns the topic permissions matching the given user and exchange in
%% the given virtual host.
%%
%% @returns a list of topic permissions records.
%%
%% @private

check_and_match_topic_permissions(Username, VHostName, ExchangeName)
  when (is_binary(Username) orelse Username =:= '_') andalso
       (is_binary(VHostName) orelse VHostName =:= '_') andalso
       (is_binary(ExchangeName) orelse ExchangeName =:= '_') ->
    match_topic_permissions_in_khepri(Username, VHostName, ExchangeName).

match_topic_permissions_in_khepri('_' = _Username, '_' = _VHostName, ExchangeName) ->
    rabbit_khepri:transaction(
      fun() ->
              match_topic_permissions_in_khepri_tx(
                ?KHEPRI_WILDCARD_STAR, ?KHEPRI_WILDCARD_STAR, any(ExchangeName))
      end, ro);
match_topic_permissions_in_khepri('_' = _Username, VHostName, ExchangeName) ->
    rabbit_khepri:transaction(
        rabbit_db_vhost:with_fun_in_khepri_tx(
          VHostName,
          fun() ->
                  match_topic_permissions_in_khepri_tx(
                    ?KHEPRI_WILDCARD_STAR, VHostName, any(ExchangeName))
          end),
        ro);
match_topic_permissions_in_khepri(
  Username, '_' = _VHostName, ExchangeName) ->
    rabbit_khepri:transaction(
        with_fun_in_khepri_tx(
          Username,
          fun() ->
                  match_topic_permissions_in_khepri_tx(
                    Username, ?KHEPRI_WILDCARD_STAR, any(ExchangeName))
          end),
        ro);
match_topic_permissions_in_khepri(
  Username, VHostName, ExchangeName) ->
    rabbit_khepri:transaction(
        with_fun_in_khepri_tx(
          Username,
          rabbit_db_vhost:with_fun_in_khepri_tx(
            VHostName,
            fun() ->
                    match_topic_permissions_in_khepri_tx(
                      Username, VHostName, any(ExchangeName))
            end)),
        ro).

match_topic_permissions_in_khepri_tx(Username, VHostName, ExchangeName) ->
    Path = khepri_topic_permission_path(Username, VHostName, ExchangeName),
    case khepri_tx:get_many(Path) of
        {ok, Map} -> maps:values(Map);
        _         -> []
    end.

%% -------------------------------------------------------------------
%% set_topic_permissions().
%% -------------------------------------------------------------------

-spec set_topic_permissions(TopicPermission) -> ok when
      TopicPermission :: #topic_permission{}.
%% @doc Sets the permissions for the user, virtual host and exchange set in
%% the `TopicPermission' record.
%%
%% @private

set_topic_permissions(
  #topic_permission{
     topic_permission_key =
     #topic_permission_key{
        user_vhost = #user_vhost{username = Username,
                                 virtual_host = VHostName}}}
  = TopicPermission) ->
    rabbit_khepri:transaction(
      with_fun_in_khepri_tx(
        Username,
        rabbit_db_vhost:with_fun_in_khepri_tx(
          VHostName,
          fun() ->
                  set_topic_permissions_in_khepri_tx(Username, VHostName, TopicPermission)
          end)), rw).

set_topic_permissions_in_khepri_tx(Username, VHostName, TopicPermission) ->
    #topic_permission{topic_permission_key =
                          #topic_permission_key{exchange = ExchangeName}} = TopicPermission,
    %% TODO: Check user presence in a transaction.
    Path = khepri_topic_permission_path(
             Username,
             VHostName,
             ExchangeName),
    Extra = #{keep_while =>
                  #{rabbit_db_user:khepri_user_path(Username) =>
                        #if_node_exists{exists = true}}},
    Ret = khepri_tx:put(Path, TopicPermission, Extra),
    case Ret of
        ok -> ok;
        Error   -> khepri_tx:abort(Error)
    end.

%% -------------------------------------------------------------------
%% clear_topic_permissions().
%% -------------------------------------------------------------------

-spec clear_topic_permissions(Username, VHostName, ExchangeName) -> ok when
      Username :: internal_user:username(),
      VHostName :: vhost:name(),
      ExchangeName :: binary() | '_'.
%% @doc Clears the topic permissions of the given user and exchange in the
%% given virtual host and exchange.
%%
%% @private

clear_topic_permissions(Username, VHostName, ExchangeName)
  when is_binary(Username) andalso is_binary(VHostName) andalso
       (is_binary(ExchangeName) orelse ExchangeName =:= '_') ->
    Path = khepri_topic_permission_path(any(Username), any(VHostName), any(ExchangeName)),
    rabbit_khepri:delete(Path).

%% -------------------------------------------------------------------
%% clear_matching_topic_permissions().
%% -------------------------------------------------------------------

-spec clear_matching_topic_permissions(Username, VHostName, ExchangeName) ->
    Ret when
      Username :: rabbit_types:username() | '_',
      VHostName :: vhost:name() | '_',
      ExchangeName :: binary() | '_',
      Ret :: ok.
%% @doc Clears all topic permissions matching arguments.
%%
%% @private

clear_matching_topic_permissions(Username, VHostName, ExchangeName)
  when (is_binary(Username) orelse Username =:= '_') andalso
       (is_binary(VHostName) orelse VHostName =:= '_') andalso
       (is_binary(ExchangeName) orelse ExchangeName =:= '_') ->
    Path = khepri_topic_permission_path(
             any(Username), any(VHostName), any(ExchangeName)),
    ok = rabbit_khepri:delete(Path).

%% -------------------------------------------------------------------
%% delete().
%% -------------------------------------------------------------------

-spec delete(Username) -> Existed when
      Username :: internal_user:username(),
      Existed :: boolean() | {error, any()}.
%% @doc Deletes a user and its permissions from the database.
%%
%% @returns a boolean indicating if the user existed. It throws an exception
%% if the transaction fails.
%%
%% @private

delete(Username) when is_binary(Username) ->
    Path = khepri_user_path(Username),
    case rabbit_khepri:delete_or_fail(Path) of
        ok -> true;
        {error, {node_not_found, _}} -> false;
        Error -> Error
    end.

%% -------------------------------------------------------------------
%% clear().
%% -------------------------------------------------------------------

-spec clear() -> ok.
%% @doc Deletes all users and permissions.
%%
%% @private

clear() ->
    Path = khepri_user_path(?KHEPRI_WILDCARD_STAR),
    case rabbit_khepri:delete(Path) of
        ok    -> ok;
        Error -> throw(Error)
    end.

%% --------------------------------------------------------------
%% Paths
%% --------------------------------------------------------------

khepri_user_path(Username)
  when ?IS_KHEPRI_PATH_CONDITION(Username) ->
    ?RABBITMQ_KHEPRI_USER_PATH(Username).

khepri_user_permission_path(Username, VHostName)
  when ?IS_KHEPRI_PATH_CONDITION(Username) andalso
       ?IS_KHEPRI_PATH_CONDITION(VHostName) ->
    ?RABBITMQ_KHEPRI_USER_PERMISSION_PATH(VHostName, Username).

khepri_topic_permission_path(Username, VHostName, Exchange)
  when ?IS_KHEPRI_PATH_CONDITION(Username) andalso
       ?IS_KHEPRI_PATH_CONDITION(VHostName) andalso
       ?IS_KHEPRI_PATH_CONDITION(Exchange) ->
    ?RABBITMQ_KHEPRI_TOPIC_PERMISSION_PATH(VHostName, Exchange, Username).
