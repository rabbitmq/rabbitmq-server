%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_db_user).

-include_lib("stdlib/include/assert.hrl").

-include_lib("khepri/include/khepri.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

-export([create/1,
         update/2,
         get/1,
         get_all/0,
         with_fun_in_mnesia_tx/2,
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
         delete/1]).

-export([khepri_users_path/0,
         khepri_user_path/1,
         khepri_user_permission_path/2,
         khepri_topic_permission_path/3]).

%% for testing
-export([clear/0]).

-ifdef(TEST).
-export([get_in_mnesia/1,
         get_in_khepri/1,
         create_in_mnesia/2,
         create_in_khepri/2,
         get_all_in_mnesia/0,
         get_all_in_khepri/0,
         update_in_mnesia/2,
         update_in_khepri/2,
         delete_in_mnesia/1,
         delete_in_khepri/1,
         get_user_permissions_in_mnesia/2,
         get_user_permissions_in_khepri/2,
         set_user_permissions_in_mnesia/3,
         set_user_permissions_in_khepri/3,
         set_topic_permissions_in_mnesia/3,
         set_topic_permissions_in_khepri/3,
         match_user_permissions_in_mnesia/2,
         match_user_permissions_in_khepri/2,
         clear_user_permissions_in_mnesia/2,
         clear_user_permissions_in_khepri/2,
         get_topic_permissions_in_mnesia/3,
         get_topic_permissions_in_khepri/3,
         match_topic_permissions_in_mnesia/3,
         match_topic_permissions_in_khepri/3,
         clear_topic_permissions_in_mnesia/3,
         clear_topic_permissions_in_khepri/3
        ]).
-endif.

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
    rabbit_khepri:handle_fallback(
      #{mnesia => fun() -> create_in_mnesia(Username, User) end,
        khepri => fun() -> create_in_khepri(Username, User) end}).

create_in_mnesia(Username, User) ->
    rabbit_mnesia:execute_mnesia_transaction(
      fun() -> create_in_mnesia_tx(Username, User) end).

create_in_mnesia_tx(Username, User) ->
    case mnesia:wread({?MNESIA_TABLE, Username}) of
        [] -> ok = mnesia:write(?MNESIA_TABLE, User, write);
        _  -> mnesia:abort({user_already_exists, Username})
    end.

create_in_khepri(Username, User) ->
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
    rabbit_khepri:handle_fallback(
      #{mnesia => fun() -> update_in_mnesia(Username, UpdateFun) end,
        khepri => fun() -> update_in_khepri(Username, UpdateFun) end}).

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

update_in_khepri(Username, UpdateFun) ->
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
    rabbit_khepri:handle_fallback(
      #{mnesia => fun() -> get_in_mnesia(Username) end,
        khepri => fun() -> get_in_khepri(Username) end}).

get_in_mnesia(Username) ->
    case ets:lookup(?MNESIA_TABLE, Username) of
        [User] -> User;
        []     -> undefined
    end.

get_in_khepri(Username) ->
    case ets:lookup(rabbit_khepri_users, Username) of
        [User] -> User;
        _      -> undefined
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
    rabbit_khepri:handle_fallback(
      #{mnesia => fun() -> get_all_in_mnesia() end,
        khepri => fun() -> get_all_in_khepri() end}).

get_all_in_mnesia() ->
    mnesia:dirty_match_object(
      ?MNESIA_TABLE,
      internal_user:pattern_match_all()).

get_all_in_khepri() ->
    Path = khepri_users_path(),
    case rabbit_khepri:list(Path) of
        {ok, Users} -> maps:values(Users);
        _           -> []
    end.

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
    rabbit_khepri:handle_fallback(
      #{mnesia =>
        fun() -> get_user_permissions_in_mnesia(Username, VHostName) end,
        khepri =>
        fun() -> get_user_permissions_in_khepri(Username, VHostName) end}).

get_user_permissions_in_mnesia(Username, VHostName) ->
    Key = #user_vhost{username     = Username,
                      virtual_host = VHostName},
    case mnesia:dirty_read({?PERM_MNESIA_TABLE, Key}) of
        [UserPermission] -> UserPermission;
        []               -> undefined
    end.

get_user_permissions_in_khepri(Username, VHostName) ->
    UserVHost = #user_vhost{username     = Username,
                            virtual_host = VHostName},
    case ets:lookup(rabbit_khepri_user_permissions, UserVHost) of
        [UserPermission] -> UserPermission;
        _      -> undefined
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
    rabbit_khepri:handle_fallback(
      #{mnesia =>
        fun() -> match_user_permissions_in_mnesia(Username, VHostName) end,
        khepri =>
        fun() -> match_user_permissions_in_khepri(Username, VHostName) end}).

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

match_user_permissions_in_khepri('_' = _Username, '_' = _VHostName) ->
    Path = khepri_user_permission_path(?KHEPRI_WILDCARD_STAR, ?KHEPRI_WILDCARD_STAR),
    case rabbit_khepri:match(Path) of
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
          end));
match_user_permissions_in_khepri(Username, '_' = _VHostName) ->
    rabbit_khepri:transaction(
        with_fun_in_khepri_tx(
          Username,
          fun() ->
                  match_user_permissions_in_khepri_tx(Username, ?KHEPRI_WILDCARD_STAR)
          end));
match_user_permissions_in_khepri(Username, VHostName) ->
    rabbit_khepri:transaction(
        with_fun_in_khepri_tx(
          Username,
          rabbit_db_vhost:with_fun_in_khepri_tx(
            VHostName,
            fun() ->
                    match_user_permissions_in_khepri_tx(Username, VHostName)
            end))).

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
    rabbit_khepri:handle_fallback(
      #{mnesia =>
        fun() ->
                set_user_permissions_in_mnesia(
                  Username, VHostName, UserPermission)
        end,
        khepri =>
        fun() ->
                set_user_permissions_in_khepri(
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

set_user_permissions_in_khepri(Username, VHostName, UserPermission) ->
    rabbit_khepri:transaction(
      with_fun_in_khepri_tx(
        Username,
        rabbit_db_vhost:with_fun_in_khepri_tx(
          VHostName,
          fun() ->
                  set_user_permissions_in_khepri_tx(Username, VHostName, UserPermission)
          end)), rw).

set_user_permissions_in_khepri_tx(Username, VHostName, UserPermission) ->
    Path = khepri_user_permission_path(
             #if_all{conditions =
                         [Username,
                          #if_node_exists{exists = true}]},
             VHostName),
    Extra = #{keep_while =>
                  #{rabbit_db_vhost:khepri_vhost_path(VHostName) =>
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
    rabbit_khepri:handle_fallback(
      #{mnesia =>
        fun() -> clear_user_permissions_in_mnesia(Username, VHostName) end,
        khepri =>
        fun() -> clear_user_permissions_in_khepri(Username, VHostName) end
       }).

clear_user_permissions_in_mnesia(Username, VHostName) ->
    rabbit_mnesia:execute_mnesia_transaction(
      fun() -> clear_user_permissions_in_mnesia_tx(Username, VHostName) end).

clear_user_permissions_in_mnesia_tx(Username, VHostName) ->
    mnesia:delete({?PERM_MNESIA_TABLE,
                   #user_vhost{username     = Username,
                               virtual_host = VHostName}}).

clear_user_permissions_in_khepri(Username, VHostName) ->
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
    rabbit_khepri:handle_fallback(
      #{mnesia =>
        fun() ->
                clear_matching_user_permissions_in_mnesia(Username, VHostName)
        end,
        khepri =>
        fun() ->
                clear_matching_user_permissions_in_khepri(Username, VHostName)
        end
       }).

clear_matching_user_permissions_in_mnesia(Username, VHostName) ->
    rabbit_mnesia:execute_mnesia_transaction(
      fun() ->
              _ = clear_matching_user_permissions_in_mnesia_tx(
                    Username, VHostName),
              ok
      end).

clear_matching_user_permissions_in_mnesia_tx(Username, VHostName) ->
    [begin
         mnesia:delete(?PERM_MNESIA_TABLE, Key, write),
         Record
     end
     || #user_permission{user_vhost = Key} = Record
        <- match_user_permissions_in_mnesia_tx(Username, VHostName)].

clear_matching_user_permissions_in_khepri(Username, VHostName) ->
    Path = khepri_user_permission_path(any(Username), any(VHostName)),
    ok = rabbit_khepri:delete(Path).

any('_') -> ?KHEPRI_WILDCARD_STAR;
any(Value) -> Value.

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
    rabbit_khepri:handle_fallback(
      #{mnesia =>
        fun() ->
                get_topic_permissions_in_mnesia(
                  Username, VHostName, ExchangeName)
        end,
        khepri =>
        fun() ->
                get_topic_permissions_in_khepri(
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

get_topic_permissions_in_khepri(Username, VHostName, ExchangeName) ->
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
    rabbit_khepri:handle_fallback(
      #{mnesia =>
        fun() ->
                match_topic_permissions_in_mnesia(
                  Username, VHostName, ExchangeName)
        end,
        khepri =>
        fun() ->
                match_topic_permissions_in_khepri(
                  Username, VHostName, ExchangeName)
        end
       }).

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

match_topic_permissions_in_khepri('_' = _Username, '_' = _VHostName, ExchangeName) ->
    rabbit_khepri:transaction(
      fun() ->
              match_topic_permissions_in_khepri_tx(
                ?KHEPRI_WILDCARD_STAR, ?KHEPRI_WILDCARD_STAR, any(ExchangeName))
      end);
match_topic_permissions_in_khepri('_' = _Username, VHostName, ExchangeName) ->
    rabbit_khepri:transaction(
        rabbit_db_vhost:with_fun_in_khepri_tx(
          VHostName,
          fun() ->
                  match_topic_permissions_in_khepri_tx(
                    ?KHEPRI_WILDCARD_STAR, VHostName, any(ExchangeName))
          end));
match_topic_permissions_in_khepri(
  Username, '_' = _VHostName, ExchangeName) ->
    rabbit_khepri:transaction(
        with_fun_in_khepri_tx(
          Username,
          fun() ->
                  match_topic_permissions_in_khepri_tx(
                    Username, ?KHEPRI_WILDCARD_STAR, any(ExchangeName))
          end));
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
            end))).

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
    rabbit_khepri:handle_fallback(
      #{mnesia =>
        fun() ->
                set_topic_permissions_in_mnesia(
                  Username, VHostName, TopicPermission)
        end,
        khepri =>
            fun() ->
                    set_topic_permissions_in_khepri(
                      Username, VHostName, TopicPermission)
            end
       }).

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

set_topic_permissions_in_khepri(Username, VHostName, TopicPermission) ->
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
    Path = khepri_topic_permission_path(
             #if_all{conditions =
                         [Username,
                          #if_node_exists{exists = true}]},
             VHostName,
             ExchangeName),
    Extra = #{keep_while =>
                  #{rabbit_db_vhost:khepri_vhost_path(VHostName) =>
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
    rabbit_khepri:handle_fallback(
      #{mnesia =>
        fun() ->
                clear_topic_permissions_in_mnesia(
                  Username, VHostName, ExchangeName)
        end,
        khepri =>
            fun() ->
                    clear_topic_permissions_in_khepri(
                      Username, VHostName, ExchangeName)
            end
       }).

clear_topic_permissions_in_mnesia(Username, VHostName, ExchangeName) ->
    rabbit_mnesia:execute_mnesia_transaction(
      fun() ->
              clear_topic_permissions_in_mnesia_tx(
                Username, VHostName, ExchangeName)
      end).

clear_topic_permissions_in_mnesia_tx(Username, VHostName, ExchangeName) ->
    delete_topic_permission_in_mnesia_tx(Username, VHostName, ExchangeName).

clear_topic_permissions_in_khepri(Username, VHostName, ExchangeName) ->
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
    rabbit_khepri:handle_fallback(
      #{mnesia =>
        fun() ->
                clear_matching_topic_permissions_in_mnesia(
                  Username, VHostName, ExchangeName)
        end,
        khepri =>
        fun() ->
                clear_matching_topic_permissions_in_khepri(
                  Username, VHostName, ExchangeName)
        end
       }).

clear_matching_topic_permissions_in_mnesia(
  Username, VHostName, ExchangeName) ->
    rabbit_mnesia:execute_mnesia_transaction(
      fun() ->
              _ = clear_matching_topic_permissions_in_mnesia_tx(
                    Username, VHostName, ExchangeName),
              ok
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

clear_matching_topic_permissions_in_khepri(
  Username, VHostName, ExchangeName) ->
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
    rabbit_khepri:handle_fallback(
      #{mnesia => fun() -> delete_in_mnesia(Username) end,
        khepri => fun() -> delete_in_khepri(Username) end
       }).

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

delete_in_khepri(Username) ->
    Path = khepri_user_path(Username),
    case rabbit_khepri:delete_or_fail(Path) of
        ok -> true;
        {error, {node_not_found, _}} -> false;
        Error -> Error
    end.

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
    rabbit_khepri:handle_fallback(
      #{mnesia => fun() -> clear_in_mnesia() end,
        khepri => fun() -> clear_in_khepri() end}).

clear_in_mnesia() ->
    {atomic, ok} = mnesia:clear_table(?MNESIA_TABLE),
    {atomic, ok} = mnesia:clear_table(?PERM_MNESIA_TABLE),
    {atomic, ok} = mnesia:clear_table(?TOPIC_PERM_MNESIA_TABLE),
    ok.

clear_in_khepri() ->
    Path = khepri_users_path(),
    case rabbit_khepri:delete(Path) of
        ok    -> ok;
        Error -> throw(Error)
    end.

%% --------------------------------------------------------------
%% Paths
%% --------------------------------------------------------------

khepri_users_path()        -> [?MODULE, users].
khepri_user_path(Username) -> [?MODULE, users, Username].

khepri_user_permission_path(Username, VHostName) ->
    [?MODULE, users, Username, user_permissions, VHostName].

khepri_topic_permission_path(Username, VHostName, Exchange) ->
    [?MODULE, users, Username, topic_permissions, VHostName, Exchange].
