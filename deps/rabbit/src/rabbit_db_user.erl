%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_db_user).

-include_lib("stdlib/include/assert.hrl").

-include_lib("rabbit_common/include/rabbit.hrl").

-export([create/1,
         update/2,
         get/1,
         get_all/0,
         with_fun_in_mnesia_tx/2,
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
         delete/1]).

-export([clear/0]).

-define(MNESIA_TABLE, rabbit_user).
-define(PERM_MNESIA_TABLE, rabbit_user_permission).
-define(TOPIC_PERM_MNESIA_TABLE, rabbit_topic_permission).

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
    rabbit_db:run(
      #{mnesia => fun() -> create_in_mnesia(Username, User) end}).

create_in_mnesia(Username, User) ->
    rabbit_mnesia:execute_mnesia_transaction(
      fun() -> create_in_mnesia_tx(Username, User) end).

create_in_mnesia_tx(Username, User) ->
    case mnesia:wread({?MNESIA_TABLE, Username}) of
        [] -> ok = mnesia:write(?MNESIA_TABLE, User, write);
        _  -> mnesia:abort({user_already_exists, Username})
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
    rabbit_db:run(
      #{mnesia => fun() -> update_in_mnesia(Username, UpdateFun) end}).

update_in_mnesia(Username, UpdateFun) ->
    rabbit_mnesia:execute_mnesia_transaction(
      fun() -> update_in_mnesia_tx(Username, UpdateFun) end).

update_in_mnesia_tx(Username, UpdateFun) ->
    case mnesia:wread({?MNESIA_TABLE, Username}) of
        [User0] ->
            User1 = UpdateFun(User0),
            ok = mnesia:write(?MNESIA_TABLE, User1, write);
        [] ->
            mnesia:abort({no_such_user, Username})
    end.

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
    rabbit_db:run(
      #{mnesia => fun() -> get_in_mnesia(Username) end}).

get_in_mnesia(Username) ->
    case ets:lookup(?MNESIA_TABLE, Username) of
        [User] -> User;
        []     -> undefined
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
    rabbit_db:run(
      #{mnesia => fun() -> get_all_in_mnesia() end}).

get_all_in_mnesia() ->
    mnesia:dirty_match_object(
      ?MNESIA_TABLE,
      internal_user:pattern_match_all()).

%% -------------------------------------------------------------------
%% with_fun_in_*().
%% -------------------------------------------------------------------

-spec with_fun_in_mnesia_tx(Username, TxFun) -> Fun when
      Username :: internal_user:username(),
      TxFun :: fun(() -> Ret),
      Fun :: fun(() -> Ret),
      Ret :: any().
%% @doc Returns a function, calling `TxFun' only if the user named `Username'
%% exists.
%%
%% The returned function must be used inside a Mnesia transaction.
%%
%% @returns a function calling `TxFun' only if the user exists.
%%
%% @private

with_fun_in_mnesia_tx(Username, TxFun)
  when is_binary(Username) andalso is_function(TxFun, 0) ->
    fun() ->
            ?assert(mnesia:is_transaction()),
            case mnesia:read({?MNESIA_TABLE, Username}) of
                [_User] -> TxFun();
                []      -> mnesia:abort({no_such_user, Username})
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
    rabbit_db:run(
      #{mnesia =>
        fun() -> get_user_permissions_in_mnesia(Username, VHostName) end}).

get_user_permissions_in_mnesia(Username, VHostName) ->
    Key = #user_vhost{username     = Username,
                      virtual_host = VHostName},
    case mnesia:dirty_read({?PERM_MNESIA_TABLE, Key}) of
        [UserPermission] -> UserPermission;
        []               -> undefined
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
    rabbit_db:run(
      #{mnesia =>
        fun() -> match_user_permissions_in_mnesia(Username, VHostName) end}).

match_user_permissions_in_mnesia('_' = Username, '_' = VHostName) ->
    rabbit_mnesia:execute_mnesia_transaction(
      fun() ->
              match_user_permissions_in_mnesia_tx(Username, VHostName)
      end);
match_user_permissions_in_mnesia('_' = Username, VHostName) ->
    rabbit_mnesia:execute_mnesia_transaction(
        rabbit_db_vhost:with_fun_in_mnesia_tx(
          VHostName,
          fun() ->
                  match_user_permissions_in_mnesia_tx(Username, VHostName)
          end));
match_user_permissions_in_mnesia(Username, '_' = VHostName) ->
    rabbit_mnesia:execute_mnesia_transaction(
        with_fun_in_mnesia_tx(
          Username,
          fun() ->
                  match_user_permissions_in_mnesia_tx(Username, VHostName)
          end));
match_user_permissions_in_mnesia(Username, VHostName) ->
    rabbit_mnesia:execute_mnesia_transaction(
        with_fun_in_mnesia_tx(
          Username,
          rabbit_db_vhost:with_fun_in_mnesia_tx(
            VHostName,
            fun() ->
                    match_user_permissions_in_mnesia_tx(Username, VHostName)
            end))).

match_user_permissions_in_mnesia_tx(Username, VHostName) ->
    mnesia:match_object(
      ?PERM_MNESIA_TABLE,
      #user_permission{user_vhost = #user_vhost{
                                       username     = Username,
                                       virtual_host = VHostName},
                       permission = '_'},
      read).

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
    rabbit_db:run(
      #{mnesia =>
        fun() ->
                set_user_permissions_in_mnesia(
                  Username, VHostName, UserPermission)
        end}).

set_user_permissions_in_mnesia(Username, VHostName, UserPermission) ->
    rabbit_mnesia:execute_mnesia_transaction(
      with_fun_in_mnesia_tx(
        Username,
        rabbit_db_vhost:with_fun_in_mnesia_tx(
          VHostName,
          fun() -> set_user_permissions_in_mnesia_tx(UserPermission) end))).

set_user_permissions_in_mnesia_tx(UserPermission) ->
    mnesia:write(?PERM_MNESIA_TABLE, UserPermission, write).

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
    rabbit_db:run(
      #{mnesia =>
        fun() -> clear_user_permissions_in_mnesia(Username, VHostName) end}).

clear_user_permissions_in_mnesia(Username, VHostName) ->
    rabbit_mnesia:execute_mnesia_transaction(
      fun() -> clear_user_permissions_in_mnesia_tx(Username, VHostName) end).

clear_user_permissions_in_mnesia_tx(Username, VHostName) ->
    mnesia:delete({?PERM_MNESIA_TABLE,
                   #user_vhost{username     = Username,
                               virtual_host = VHostName}}).

%% -------------------------------------------------------------------
%% clear_matching_user_permissions().
%% -------------------------------------------------------------------

-spec clear_matching_user_permissions(Username, VHostName) -> Ret when
      Username :: internal_user:username() | '_',
      VHostName :: vhost:name() | '_',
      Ret :: [#user_permission{}].
%% @doc Clears all user permissions matching arguments.
%%
%% @returns a list of matching user permissions.
%%
%% @private

clear_matching_user_permissions(Username, VHostName)
  when (is_binary(Username) orelse Username =:= '_') andalso
       (is_binary(VHostName) orelse VHostName =:= '_') ->
    rabbit_db:run(
      #{mnesia =>
        fun() ->
                clear_matching_user_permissions_in_mnesia(Username, VHostName)
        end
       }).

clear_matching_user_permissions_in_mnesia(Username, VHostName) ->
    rabbit_mnesia:execute_mnesia_transaction(
      fun() ->
              clear_matching_user_permissions_in_mnesia_tx( Username, VHostName)
      end).

clear_matching_user_permissions_in_mnesia_tx(Username, VHostName) ->
    [begin
         mnesia:delete(?PERM_MNESIA_TABLE, Key, write),
         Record
     end
     || #user_permission{user_vhost = Key} = Record
        <- match_user_permissions_in_mnesia_tx(Username, VHostName)].

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
    rabbit_db:run(
      #{mnesia =>
        fun() ->
                get_topic_permissions_in_mnesia(
                  Username, VHostName, ExchangeName)
        end}).

get_topic_permissions_in_mnesia(Username, VHostName, ExchangeName) ->
    Key = #topic_permission_key{
             user_vhost = #user_vhost{username     = Username,
                                      virtual_host = VHostName},
             exchange = ExchangeName},
    case mnesia:dirty_read({?TOPIC_PERM_MNESIA_TABLE, Key}) of
        [TopicPermission] -> TopicPermission;
        []                -> undefined
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
    rabbit_db:run(
      #{mnesia =>
        fun() ->
                match_topic_permissions_in_mnesia(
                  Username, VHostName, ExchangeName)
        end}).

match_topic_permissions_in_mnesia(
  '_' = Username, '_' = VHostName, ExchangeName) ->
    rabbit_mnesia:execute_mnesia_transaction(
      fun() ->
              match_topic_permissions_in_mnesia_tx(
                Username, VHostName, ExchangeName)
      end);
match_topic_permissions_in_mnesia(
  '_' = Username, VHostName, ExchangeName) ->
    rabbit_mnesia:execute_mnesia_transaction(
        rabbit_db_vhost:with_fun_in_mnesia_tx(
          VHostName,
          fun() ->
                  match_topic_permissions_in_mnesia_tx(
                    Username, VHostName, ExchangeName)
          end));
match_topic_permissions_in_mnesia(
  Username, '_' = VHostName, ExchangeName) ->
    rabbit_mnesia:execute_mnesia_transaction(
        with_fun_in_mnesia_tx(
          Username,
          fun() ->
                  match_topic_permissions_in_mnesia_tx(
                    Username, VHostName, ExchangeName)
          end));
match_topic_permissions_in_mnesia(
  Username, VHostName, ExchangeName) ->
    rabbit_mnesia:execute_mnesia_transaction(
        with_fun_in_mnesia_tx(
          Username,
          rabbit_db_vhost:with_fun_in_mnesia_tx(
            VHostName,
            fun() ->
                    match_topic_permissions_in_mnesia_tx(
                      Username, VHostName, ExchangeName)
            end))).

match_topic_permissions_in_mnesia_tx(Username, VHostName, ExchangeName) ->
    mnesia:match_object(
      ?TOPIC_PERM_MNESIA_TABLE,
      #topic_permission{
         topic_permission_key = #topic_permission_key{
                                   user_vhost = #user_vhost{
                                                   username     = Username,
                                                   virtual_host = VHostName},
                                   exchange = ExchangeName},
         permission = '_'},
      read).

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
    rabbit_db:run(
      #{mnesia =>
        fun() ->
                set_topic_permissions_in_mnesia(
                  Username, VHostName, TopicPermission)
        end}).

set_topic_permissions_in_mnesia(Username, VHostName, TopicPermission) ->
    rabbit_mnesia:execute_mnesia_transaction(
      with_fun_in_mnesia_tx(
        Username,
        rabbit_db_vhost:with_fun_in_mnesia_tx(
          VHostName,
          fun() ->
                  set_topic_permissions_in_mnesia_tx(TopicPermission)
          end))).

set_topic_permissions_in_mnesia_tx(TopicPermission) ->
    mnesia:write(?TOPIC_PERM_MNESIA_TABLE, TopicPermission, write).

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
    rabbit_db:run(
      #{mnesia =>
        fun() ->
                clear_topic_permissions_in_mnesia(
                  Username, VHostName, ExchangeName)
        end}).

clear_topic_permissions_in_mnesia(Username, VHostName, ExchangeName) ->
    rabbit_mnesia:execute_mnesia_transaction(
      fun() ->
              clear_topic_permissions_in_mnesia_tx(
                Username, VHostName, ExchangeName)
      end).

clear_topic_permissions_in_mnesia_tx(Username, VHostName, ExchangeName) ->
    delete_topic_permission_in_mnesia_tx(Username, VHostName, ExchangeName).

%% -------------------------------------------------------------------
%% clear_matching_topic_permissions().
%% -------------------------------------------------------------------

-spec clear_matching_topic_permissions(Username, VHostName, ExchangeName) ->
    Ret when
      Username :: rabbit_types:username() | '_',
      VHostName :: vhost:name() | '_',
      ExchangeName :: binary() | '_',
      Ret :: [#topic_permission{}].
%% @doc Clears all topic permissions matching arguments.
%%
%% @returns a list of matching topic permissions.
%%
%% @private

clear_matching_topic_permissions(Username, VHostName, ExchangeName)
  when (is_binary(Username) orelse Username =:= '_') andalso
       (is_binary(VHostName) orelse VHostName =:= '_') andalso
       (is_binary(ExchangeName) orelse ExchangeName =:= '_') ->
    rabbit_db:run(
      #{mnesia =>
        fun() ->
                clear_matching_topic_permissions_in_mnesia(
                  Username, VHostName, ExchangeName)
        end}).

clear_matching_topic_permissions_in_mnesia(
  Username, VHostName, ExchangeName) ->
    rabbit_mnesia:execute_mnesia_transaction(
      fun() ->
              clear_matching_topic_permissions_in_mnesia_tx(
                Username, VHostName, ExchangeName)
      end).

clear_matching_topic_permissions_in_mnesia_tx(
  Username, VHostName, ExchangeName) ->
    [begin
         mnesia:delete(?TOPIC_PERM_MNESIA_TABLE, Key, write),
         Record
     end
     || #topic_permission{topic_permission_key = Key} = Record
        <- match_topic_permissions_in_mnesia_tx(
             Username, VHostName, ExchangeName)].

%% -------------------------------------------------------------------
%% delete().
%% -------------------------------------------------------------------

-spec delete(Username) -> Existed when
      Username :: internal_user:username(),
      Existed :: boolean().
%% @doc Deletes a user and its permissions from the database.
%%
%% @returns a boolean indicating if the user existed. It throws an exception
%% if the transaction fails.
%%
%% @private

delete(Username) when is_binary(Username) ->
    rabbit_db:run(
      #{mnesia => fun() -> delete_in_mnesia(Username) end}).

delete_in_mnesia(Username) ->
    rabbit_mnesia:execute_mnesia_transaction(
      fun() -> delete_in_mnesia_tx(Username) end).

delete_in_mnesia_tx(Username) ->
    Existed = mnesia:wread({?MNESIA_TABLE, Username}) =/= [],
    delete_user_in_mnesia_tx(Username),
    delete_user_permission_in_mnesia_tx(Username, '_'),
    delete_topic_permission_in_mnesia_tx(Username, '_', '_'),
    Existed.

delete_user_in_mnesia_tx(Username) ->
    mnesia:delete({?MNESIA_TABLE, Username}).

delete_user_permission_in_mnesia_tx(Username, VHostName) ->
    Pattern = user_permission_pattern(Username, VHostName),
    _ = [mnesia:delete_object(?PERM_MNESIA_TABLE, R, write) ||
         R <- mnesia:match_object(?PERM_MNESIA_TABLE, Pattern, write)],
    ok.

delete_topic_permission_in_mnesia_tx(Username, VHostName, ExchangeName) ->
    Pattern = topic_permission_pattern(Username, VHostName, ExchangeName),
    _ = [mnesia:delete_object(?TOPIC_PERM_MNESIA_TABLE, R, write) ||
         R <- mnesia:match_object(?TOPIC_PERM_MNESIA_TABLE, Pattern, write)],
    ok.

user_permission_pattern(Username, VHostName) ->
    #user_permission{user_vhost = #user_vhost{
                                     username = Username,
                                     virtual_host = VHostName},
                     permission = '_'}.

topic_permission_pattern(Username, VHostName, ExchangeName) ->
    #topic_permission{
             topic_permission_key =
             #topic_permission_key{
                user_vhost = #user_vhost{
                                username     = Username,
                                virtual_host = VHostName},
                exchange = ExchangeName},
             permission = '_'}.

%% -------------------------------------------------------------------
%% clear().
%% -------------------------------------------------------------------

-spec clear() -> ok.
%% @doc Deletes all users and permissions.
%%
%% @private

clear() ->
    rabbit_db:run(
      #{mnesia => fun() -> clear_in_mnesia() end}).

clear_in_mnesia() ->
    {atomic, ok} = mnesia:clear_table(?MNESIA_TABLE),
    {atomic, ok} = mnesia:clear_table(?PERM_MNESIA_TABLE),
    {atomic, ok} = mnesia:clear_table(?TOPIC_PERM_MNESIA_TABLE),
    ok.
