%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_auth_backend_internal).
-include_lib("rabbit_common/include/rabbit.hrl").

-behaviour(rabbit_authn_backend).
-behaviour(rabbit_authz_backend).

-export([user_login_authentication/2, user_login_authorization/2,
         check_vhost_access/3, check_resource_access/4, check_topic_access/4]).

-export([add_user/3, add_user/4, add_user/5, delete_user/2, lookup_user/1, exists/1,
         change_password/3, clear_password/2,
         hash_password/2, change_password_hash/2, change_password_hash/3,
         set_tags/3, set_permissions/6, clear_permissions/3, set_permissions_globally/5,
         set_topic_permissions/6, clear_topic_permissions/3, clear_topic_permissions/4,
         clear_all_permissions_for_vhost/2,
         add_user_sans_validation/3, put_user/2, put_user/3,
         update_user/5,
         update_user_with_hash/5,
         add_user_sans_validation/6,
         add_user_with_pre_hashed_password_sans_validation/3
]).

-export([set_user_limits/3, clear_user_limits/3, is_over_connection_limit/1,
         is_over_channel_limit/1, get_user_limits/0, get_user_limits/1]).

-export([user_info_keys/0, perms_info_keys/0,
         user_perms_info_keys/0, vhost_perms_info_keys/0,
         user_vhost_perms_info_keys/0, all_users/0,
         user_topic_perms_info_keys/0, vhost_topic_perms_info_keys/0,
         user_vhost_topic_perms_info_keys/0,
         list_users/0, list_users/2, list_permissions/0,
         list_user_permissions/1, list_user_permissions/3,
         list_topic_permissions/0,
         list_vhost_permissions/1, list_vhost_permissions/3,
         list_user_vhost_permissions/2,
         list_user_topic_permissions/1, list_vhost_topic_permissions/1, list_user_vhost_topic_permissions/2]).

-export([expiry_timestamp/1]).

-export([hashing_module_for_user/1, expand_topic_permission/2]).

-ifdef(TEST).
-export([extract_user_permission_params/2,
         extract_topic_permission_params/2]).
-endif.

-import(rabbit_data_coercion, [to_atom/1, to_list/1, to_binary/1]).

%%----------------------------------------------------------------------------

-type regexp() :: binary().

%%----------------------------------------------------------------------------
%% Implementation of rabbit_auth_backend

%% Returns a password hashing module for the user record provided. If
%% there is no information in the record, we consider it to be legacy
%% (inserted by a version older than 3.6.0) and fall back to MD5, the
%% now obsolete hashing function.
hashing_module_for_user(User) ->
    ModOrUndefined = internal_user:get_hashing_algorithm(User),
    rabbit_password:hashing_mod(ModOrUndefined).

-define(BLANK_PASSWORD_REJECTION_MESSAGE,
        "user '~ts' attempted to log in with a blank password, which is prohibited by the internal authN backend. "
        "To use TLS/x509 certificate-based authentication, see the rabbitmq_auth_mechanism_ssl plugin and configure the client to use the EXTERNAL authentication mechanism. "
        "Alternatively change the password for the user to be non-blank.").

%% For cases when we do not have a set of credentials,
%% namely when x509 (TLS) certificates are used. This should only be
%% possible when the EXTERNAL authentication mechanism is used, see
%% rabbit_auth_mechanism_plain:handle_response/2 and rabbit_reader:auth_phase/2.
user_login_authentication(Username, []) ->
    user_login_authentication(Username, [{password, none}]);
%% For cases when we do have a set of credentials. rabbit_auth_mechanism_plain:handle_response/2
%% performs initial validation.
user_login_authentication(Username, AuthProps) ->
    case lists:keyfind(password, 1, AuthProps) of
        {password, <<"">>} ->
            {refused, ?BLANK_PASSWORD_REJECTION_MESSAGE,
             [Username]};
        {password, ""} ->
            {refused, ?BLANK_PASSWORD_REJECTION_MESSAGE,
             [Username]};
        {password, none} -> %% For cases when authenticating using an x.509 certificate
            internal_check_user_login(Username, fun(_) -> true end);
        {password, Cleartext} ->
            internal_check_user_login(
              Username,
              fun(User) ->
                  case internal_user:get_password_hash(User) of
                      <<Salt:4/binary, Hash/binary>> ->
                          Hash =:= rabbit_password:salted_hash(
                              hashing_module_for_user(User), Salt, Cleartext);
                      _ ->
                          false
                  end
              end);
        false ->
            case proplists:get_value(rabbit_auth_backend_internal, AuthProps, undefined) of
                undefined -> {refused, ?BLANK_PASSWORD_REJECTION_MESSAGE, [Username]};
                _ -> internal_check_user_login(Username, fun(_) -> true end)
            end
    end.

expiry_timestamp(_) -> never.

user_login_authorization(Username, _AuthProps) ->
    case user_login_authentication(Username, []) of
        {ok, #auth_user{impl = Impl, tags = Tags}} -> {ok, Impl, Tags};
        Else                                       -> Else
    end.

internal_check_user_login(Username, Fun) ->
    Refused = {refused, "user '~ts' - invalid credentials", [Username]},
    case lookup_user(Username) of
        {ok, User} ->
            Tags = internal_user:get_tags(User),
            case Fun(User) of
                true -> {ok, #auth_user{username = Username,
                                        tags     = Tags,
                                        impl     = fun() -> none end}};
                _    -> Refused
            end;
        {error, not_found} ->
            Refused
    end.

check_vhost_access(#auth_user{username = Username}, VHostPath, _AuthzData) ->
    rabbit_db_user:get_user_permissions(Username, VHostPath) =/= undefined.

check_resource_access(#auth_user{username = Username},
                      #resource{virtual_host = VHostPath, name = Name},
                      Permission,
                      _AuthContext) ->
    case rabbit_db_user:get_user_permissions(Username, VHostPath) of
        undefined ->
            false;
        #user_permission{permission = P} ->
            PermRegexp = case element(permission_index(Permission), P) of
                             %% <<"^$">> breaks Emacs' erlang mode
                             <<"">> -> <<$^, $$>>;
                             RE     -> RE
                         end,
            case re:run(Name, PermRegexp, [{capture, none}]) of
                match    -> true;
                nomatch  -> false
            end
    end.

check_topic_access(#auth_user{username = Username},
                   #resource{virtual_host = VHostPath, name = Name, kind = topic},
                   Permission,
                   Context) ->
    case rabbit_db_user:get_topic_permissions(Username, VHostPath, Name) of
        undefined ->
            true;
        #topic_permission{permission = P} ->
            PermRegexp = case element(permission_index(Permission), P) of
                             %% <<"^$">> breaks Emacs' erlang mode
                             <<"">> -> <<$^, $$>>;
                             RE     -> RE
                         end,
            PermRegexpExpanded = expand_topic_permission(
                PermRegexp,
                maps:get(variable_map, Context, undefined)
            ),
            case re:run(maps:get(routing_key, Context), PermRegexpExpanded, [{capture, none}]) of
                match    -> true;
                nomatch  -> false
            end
    end.

expand_topic_permission(Permission, ToExpand) when is_map(ToExpand) ->
    Opening = <<"{">>,
    Closing = <<"}">>,
    ReplaceFun = fun(K, V, Acc) ->
                    Placeholder = <<Opening/binary, K/binary, Closing/binary>>,
                    binary:replace(Acc, Placeholder, V, [global])
                 end,
    maps:fold(ReplaceFun, Permission, ToExpand);
expand_topic_permission(Permission, _ToExpand) ->
    Permission.

permission_index(configure) -> #permission.configure;
permission_index(write)     -> #permission.write;
permission_index(read)      -> #permission.read.

%%----------------------------------------------------------------------------
%% Manipulation of the user database

validate_credentials(Username, Password) ->
    rabbit_credential_validation:validate(Username, Password).

validate_and_alternate_credentials(Username, Password, ActingUser, Fun) ->
    case validate_credentials(Username, Password) of
        ok           ->
            Fun(Username, Password, ActingUser);
        {error, Err} ->
            rabbit_log:error("Credential validation for user '~ts' failed!", [Username]),
            {error, Err}
    end.

-spec add_user(rabbit_types:username(), rabbit_types:password(),
               rabbit_types:username()) -> 'ok' | {'error', string()}.

add_user(Username, Password, ActingUser) ->
    validate_and_alternate_credentials(Username, Password, ActingUser,
                                       fun add_user_sans_validation/3).

-spec add_user(rabbit_types:username(), rabbit_types:password(),
               rabbit_types:username(), [atom()]) -> 'ok' | {'error', string()}.

add_user(Username, Password, ActingUser, Tags) ->
    add_user(Username, Password, ActingUser, undefined, Tags).

add_user(Username, Password, ActingUser, Limits, Tags) ->
    validate_and_alternate_credentials(Username, Password, ActingUser,
                                       add_user_sans_validation(Limits, Tags)).

add_user_with_pre_hashed_password_sans_validation(Username, PasswordHash, ActingUser) ->
    HashingAlgorithm = rabbit_password:hashing_mod(),
    add_user_sans_validation(Username, PasswordHash, HashingAlgorithm, [], undefined, ActingUser).

add_user_sans_validation(Username, Password, ActingUser) ->
    add_user_sans_validation(Username, Password, ActingUser, undefined, []).

add_user_sans_validation(Limits, Tags) ->
    fun(Username, Password, ActingUser) ->
            add_user_sans_validation(Username, Password, ActingUser, Limits, Tags)
    end.

add_user_sans_validation(Username, Password, ActingUser, Limits, Tags) ->
    rabbit_log:debug("Asked to create a new user '~ts', password length in bytes: ~tp", [Username, bit_size(Password)]),
    %% hash_password will pick the hashing function configured for us
    %% but we also need to store a hint as part of the record, so we
    %% retrieve it here one more time
    HashingMod = rabbit_password:hashing_mod(),
    PasswordHash = hash_password(HashingMod, Password),
    User0 = internal_user:create_user(Username, PasswordHash, HashingMod),
    ConvertedTags = [rabbit_data_coercion:to_atom(I) || I <- Tags],
    User1 = internal_user:set_tags(User0, ConvertedTags),
    User = case Limits of
               undefined -> User1;
               Term -> internal_user:update_limits(add, User1, Term)
           end,
    add_user_sans_validation_in(Username, User, ConvertedTags, Limits, ActingUser).

add_user_sans_validation(Username, PasswordHash, HashingMod, Tags, Limits, ActingUser) ->
    rabbit_log:debug("Asked to create a new user '~ts' with password hash", [Username]),
    ConvertedTags = [rabbit_data_coercion:to_atom(I) || I <- Tags],
    User0 = internal_user:create_user(Username, PasswordHash, HashingMod),
    User1 = internal_user:set_tags(
              internal_user:set_password_hash(User0, PasswordHash, HashingMod),
              ConvertedTags),
    User = case Limits of
               undefined -> User1;
               Term -> internal_user:update_limits(add, User1, Term)
           end,
    add_user_sans_validation_in(Username, User, ConvertedTags, Limits, ActingUser).

add_user_sans_validation_in(Username, User, ConvertedTags, Limits, ActingUser) ->
    try
        R = rabbit_db_user:create(User),
        rabbit_log:info("Created user '~ts'", [Username]),
        rabbit_event:notify(user_created, [{name, Username},
                                           {user_who_performed_action, ActingUser}]),
        case ConvertedTags of
            [] -> ok;
            _ -> notify_user_tags_set(Username, ConvertedTags, ActingUser)
        end,
        case Limits of
            undefined -> ok;
            _ -> notify_limit_set(Username, ActingUser, Limits)
        end,
        R
    catch
        throw:{error, {user_already_exists, _}} = Error ->
            rabbit_log:warning("Failed to add user '~ts': the user already exists", [Username]),
            throw(Error);
        Class:Error:Stacktrace ->
            rabbit_log:warning("Failed to add user '~ts': ~tp", [Username, Error]),
            erlang:raise(Class, Error, Stacktrace)
    end .

-spec delete_user(rabbit_types:username(), rabbit_types:username()) -> 'ok'.

delete_user(Username, ActingUser) ->
    rabbit_log:debug("Asked to delete user '~ts'", [Username]),
    try
        case rabbit_db_user:delete(Username) of
            true ->
                rabbit_log:info("Deleted user '~ts'", [Username]),
                rabbit_event:notify(user_deleted,
                                    [{name, Username},
                                     {user_who_performed_action, ActingUser}]),
                ok;
            false ->
                ok;
            Error0 ->
                rabbit_log:info("Failed to delete user '~ts': ~tp", [Username, Error0]),
                throw(Error0)
        end
    catch
        Class:Error:Stacktrace ->
            rabbit_log:warning("Failed to delete user '~ts': ~tp", [Username, Error]),
            erlang:raise(Class, Error, Stacktrace)
    end .

-spec lookup_user
        (rabbit_types:username()) ->
            rabbit_types:ok(internal_user:internal_user()) |
            rabbit_types:error('not_found').

lookup_user(Username) ->
    case rabbit_db_user:get(Username) of
        undefined -> {error, not_found};
        User      -> {ok, User}
    end.

-spec exists(rabbit_types:username()) -> boolean().

exists(Username) ->
    case lookup_user(Username) of
        {error, not_found} -> false;
        _                  -> true
    end.

-spec change_password
        (rabbit_types:username(), rabbit_types:password(), rabbit_types:username()) -> 'ok'.

change_password(Username, Password, ActingUser) ->
    validate_and_alternate_credentials(Username, Password, ActingUser,
                                       fun change_password_sans_validation/3).

change_password_sans_validation(Username, Password, ActingUser) ->
    try
        rabbit_log:debug("Asked to change password of user '~ts', new password length in bytes: ~tp", [Username, bit_size(Password)]),
        HashingAlgorithm = rabbit_password:hashing_mod(),
        R = change_password_hash(Username,
                                 hash_password(rabbit_password:hashing_mod(),
                                               Password),
                                 HashingAlgorithm),
        rabbit_log:info("Successfully changed password for user '~ts'", [Username]),
        rabbit_event:notify(user_password_changed,
                            [{name, Username},
                             {user_who_performed_action, ActingUser}]),
        R
    catch
        throw:{error, {no_such_user, _}} = Error ->
            rabbit_log:warning("Failed to change password for user '~ts': the user does not exist", [Username]),
            throw(Error);
        Class:Error:Stacktrace ->
            rabbit_log:warning("Failed to change password for user '~ts': ~tp", [Username, Error]),
            erlang:raise(Class, Error, Stacktrace)
    end.

update_user(Username, Password, Tags, Limits, ActingUser) ->
    validate_and_alternate_credentials(Username, Password, ActingUser,
                                       update_user_sans_validation(Tags, Limits)).

update_user_sans_validation(Tags, Limits) ->
    fun(Username, Password, ActingUser) ->
            try
                rabbit_log:debug("Asked to change password of user '~ts', new password length in bytes: ~tp", [Username, bit_size(Password)]),
                HashingAlgorithm = rabbit_password:hashing_mod(),

                rabbit_log:debug("Asked to set user tags for user '~ts' to ~tp", [Username, Tags]),

                ConvertedTags = [rabbit_data_coercion:to_atom(I) || I <- Tags],
                R = update_user_with_hash(Username,
                                          hash_password(rabbit_password:hashing_mod(),
                                                        Password),
                                          HashingAlgorithm,
                                          ConvertedTags,
                                          Limits),
                rabbit_log:info("Successfully changed password for user '~ts'", [Username]),
                rabbit_event:notify(user_password_changed,
                                    [{name, Username},
                                     {user_who_performed_action, ActingUser}]),

                notify_user_tags_set(Username, ConvertedTags, ActingUser),
                R
            catch
                throw:{error, {no_such_user, _}} = Error ->
                    rabbit_log:warning("Failed to change password for user '~ts': the user does not exist", [Username]),
                    throw(Error);
                Class:Error:Stacktrace ->
                    rabbit_log:warning("Failed to change password for user '~ts': ~tp", [Username, Error]),
                    erlang:raise(Class, Error, Stacktrace)
            end
    end.

-spec clear_password(rabbit_types:username(), rabbit_types:username()) -> 'ok'.

clear_password(Username, ActingUser) ->
    rabbit_log:info("Clearing password for user '~ts'", [Username]),
    R = change_password_hash(Username, <<"">>),
    rabbit_event:notify(user_password_cleared,
                        [{name, Username},
                         {user_who_performed_action, ActingUser}]),
    R.

-spec hash_password
        (module(), rabbit_types:password()) -> rabbit_types:password_hash().

hash_password(HashingMod, Cleartext) ->
    rabbit_password:hash(HashingMod, Cleartext).

-spec change_password_hash
        (rabbit_types:username(), rabbit_types:password_hash()) -> 'ok'.

change_password_hash(Username, PasswordHash) ->
    change_password_hash(Username, PasswordHash, rabbit_password:hashing_mod()).


change_password_hash(Username, PasswordHash, HashingAlgorithm) ->
    rabbit_db_user:update(Username,
                          fun(User) ->
                                  internal_user:set_password_hash(User, PasswordHash, HashingAlgorithm)
                          end).

update_user_with_hash(Username, PasswordHash, HashingAlgorithm, ConvertedTags, Limits) ->
    rabbit_db_user:update(Username,
                          fun(User0) ->
                                  User1 = internal_user:set_password_hash(User0,
                                                                          PasswordHash, HashingAlgorithm),
                                  User2 = case Limits of
                                              undefined -> User1;
                                              _         -> internal_user:update_limits(add, User1, Limits)
                                          end,
                                  internal_user:set_tags(User2, ConvertedTags)
                          end).

-spec set_tags(rabbit_types:username(), [atom()], rabbit_types:username()) -> 'ok'.

set_tags(Username, Tags, ActingUser) ->
    ConvertedTags = [rabbit_data_coercion:to_atom(I) || I <- Tags],
    rabbit_log:debug("Asked to set user tags for user '~ts' to ~tp", [Username, ConvertedTags]),
    try
        R = rabbit_db_user:update(Username, fun(User) ->
                                                    internal_user:set_tags(User, ConvertedTags)
                                            end),
        notify_user_tags_set(Username, ConvertedTags, ActingUser),
        R
    catch
        throw:{error, {no_such_user, _}} = Error ->
            rabbit_log:warning("Failed to set tags for user '~ts': the user does not exist", [Username]),
            throw(Error);
        Class:Error:Stacktrace ->
            rabbit_log:warning("Failed to set tags for user '~ts': ~tp", [Username, Error]),
            erlang:raise(Class, Error, Stacktrace)
    end .

notify_user_tags_set(Username, ConvertedTags, ActingUser) ->
    rabbit_log:info("Successfully set user tags for user '~ts' to ~tp", [Username, ConvertedTags]),
    rabbit_event:notify(user_tags_set, [{name, Username}, {tags, ConvertedTags},
                                        {user_who_performed_action, ActingUser}]).

-spec set_permissions
        (rabbit_types:username(), rabbit_types:vhost(), regexp(), regexp(),
         regexp(), rabbit_types:username()) ->
            'ok'.

set_permissions(Username, VirtualHost, ConfigurePerm, WritePerm, ReadPerm, ActingUser) ->
    rabbit_log:debug("Asked to set permissions for user "
                     "'~ts' in virtual host '~ts' to '~ts', '~ts', '~ts'",
                     [Username, VirtualHost, ConfigurePerm, WritePerm, ReadPerm]),
    _ = lists:map(
      fun (RegexpBin) ->
              Regexp = binary_to_list(RegexpBin),
              case re:compile(Regexp) of
                  {ok, _}         -> ok;
                  {error, Reason} ->
                      rabbit_log:warning("Failed to set permissions for user '~ts' in virtual host '~ts': "
                                         "regular expression '~ts' is invalid",
                                         [Username, VirtualHost, RegexpBin]),
                      throw({error, {invalid_regexp, Regexp, Reason}})
              end
      end, [ConfigurePerm, WritePerm, ReadPerm]),
    try
        UserPermission = #user_permission{
                            user_vhost = #user_vhost{
                                            username     = Username,
                                            virtual_host = VirtualHost},
                            permission = #permission{
                                            configure  = ConfigurePerm,
                                            write      = WritePerm,
                                            read       = ReadPerm}},
        R = rabbit_db_user:set_user_permissions(UserPermission),
        rabbit_log:info("Successfully set permissions for user "
                        "'~ts' in virtual host '~ts' to '~ts', '~ts', '~ts'",
                        [Username, VirtualHost, ConfigurePerm, WritePerm, ReadPerm]),
        rabbit_event:notify(permission_created, [{user,      Username},
                                                 {vhost,     VirtualHost},
                                                 {configure, ConfigurePerm},
                                                 {write,     WritePerm},
                                                 {read,      ReadPerm},
                                                 {user_who_performed_action, ActingUser}]),
        R
    catch
        throw:{error, {no_such_vhost, _}} = Error ->
            rabbit_log:warning("Failed to set permissions for user '~ts': virtual host '~ts' does not exist",
                               [Username, VirtualHost]),
            throw(Error);
        throw:{error, {no_such_user, _}} = Error ->
            rabbit_log:warning("Failed to set permissions for user '~ts': the user does not exist",
                               [Username]),
            throw(Error);
        Class:Error:Stacktrace ->
            rabbit_log:warning("Failed to set permissions for user '~ts' in virtual host '~ts': ~tp",
                               [Username, VirtualHost, Error]),
            erlang:raise(Class, Error, Stacktrace)
    end.

-spec clear_permissions
        (rabbit_types:username(), rabbit_types:vhost(), rabbit_types:username()) -> 'ok'.

clear_permissions(Username, VirtualHost, ActingUser) ->
    rabbit_log:debug("Asked to clear permissions for user '~ts' in virtual host '~ts'",
                     [Username, VirtualHost]),
    try
        R = rabbit_db_user:clear_user_permissions(Username, VirtualHost),
        rabbit_log:info("Successfully cleared permissions for user '~ts' in virtual host '~ts'",
                        [Username, VirtualHost]),
        rabbit_event:notify(permission_deleted, [{user,  Username},
                                                 {vhost, VirtualHost},
                                                 {user_who_performed_action, ActingUser}]),
        R
    catch
        Class:Error:Stacktrace ->
            rabbit_log:warning("Failed to clear permissions for user '~ts' in virtual host '~ts': ~tp",
                               [Username, VirtualHost, Error]),
            erlang:raise(Class, Error, Stacktrace)
    end.

-spec clear_all_permissions_for_vhost(VirtualHost, ActingUser) -> Ret when
      VirtualHost :: rabbit_types:vhost(),
      ActingUser :: rabbit_types:username(),
      Ret :: ok | {error, Reason :: any()}.

clear_all_permissions_for_vhost(VirtualHost, ActingUser) ->
    case rabbit_db_user:clear_all_permissions_for_vhost(VirtualHost) of
        {ok, Deletions} ->
            lists:foreach(
              fun (#topic_permission{topic_permission_key =
                      #topic_permission_key{user_vhost =
                         #user_vhost{username = Username}}}) ->
                      rabbit_event:notify(
                        topic_permission_deleted,
                        [{user, Username},
                         {vhost, VirtualHost},
                         {user_who_performed_action, ActingUser}]);
                  (#user_permission{user_vhost =
                                    #user_vhost{username = Username}}) ->
                      rabbit_event:notify(
                        permission_deleted,
                        [{user, Username},
                         {vhost, VirtualHost},
                         {user_who_performed_action, ActingUser}])
              end, Deletions),
            ok;
        {error, _} = Err ->
            Err
    end.

set_permissions_globally(Username, ConfigurePerm, WritePerm, ReadPerm, ActingUser) ->
    VirtualHosts = rabbit_vhost:list_names(),
    [set_permissions(Username, VH, ConfigurePerm, WritePerm, ReadPerm, ActingUser) || VH <- VirtualHosts],
    ok.

set_topic_permissions(Username, VirtualHost, Exchange, WritePerm, ReadPerm, ActingUser) ->
    rabbit_log:debug("Asked to set topic permissions on exchange '~ts' for "
                     "user '~ts' in virtual host '~ts' to '~ts', '~ts'",
                     [Exchange, Username, VirtualHost, WritePerm, ReadPerm]),
    WritePermRegex = rabbit_data_coercion:to_binary(WritePerm),
    ReadPermRegex = rabbit_data_coercion:to_binary(ReadPerm),
    lists:foreach(
      fun (RegexpBin) ->
              case re:compile(RegexpBin) of
                  {ok, _}         -> ok;
                  {error, Reason} ->
                      rabbit_log:warning("Failed to set topic permissions on exchange '~ts' for user "
                                         "'~ts' in virtual host '~ts': regular expression '~ts' is invalid",
                                         [Exchange, Username, VirtualHost, RegexpBin]),
                      throw({error, {invalid_regexp, RegexpBin, Reason}})
              end
      end, [WritePerm, ReadPerm]),
    try
        TopicPermission = #topic_permission{
                             topic_permission_key = #topic_permission_key{
                                                       user_vhost = #user_vhost{
                                                                       username     = Username,
                                                                       virtual_host = VirtualHost},
                                                       exchange = Exchange
                                                      },
                             permission = #permission{
                                             write = WritePermRegex,
                                             read  = ReadPermRegex
                                            }
                            },
        R = rabbit_db_user:set_topic_permissions(TopicPermission),
        rabbit_log:info("Successfully set topic permissions on exchange '~ts' for "
                         "user '~ts' in virtual host '~ts' to '~ts', '~ts'",
                         [Exchange, Username, VirtualHost, WritePerm, ReadPerm]),
        rabbit_event:notify(topic_permission_created, [
            {user,      Username},
            {vhost,     VirtualHost},
            {exchange,  Exchange},
            {write,     WritePermRegex},
            {read,      ReadPermRegex},
            {user_who_performed_action, ActingUser}]),
        R
    catch
        throw:{error, {no_such_vhost, _}} = Error ->
            rabbit_log:warning("Failed to set topic permissions on exchange '~ts' for user '~ts': virtual host '~ts' does not exist.",
                               [Exchange, Username, VirtualHost]),
            throw(Error);
        throw:{error, {no_such_user, _}} = Error ->
            rabbit_log:warning("Failed to set topic permissions on exchange '~ts' for user '~ts': the user does not exist.",
                               [Exchange, Username]),
            throw(Error);
        Class:Error:Stacktrace ->
            rabbit_log:warning("Failed to set topic permissions on exchange '~ts' for user '~ts' in virtual host '~ts': ~tp.",
                               [Exchange, Username, VirtualHost, Error]),
            erlang:raise(Class, Error, Stacktrace)
    end .

clear_topic_permissions(Username, VirtualHost, ActingUser) ->
    rabbit_log:debug("Asked to clear topic permissions for user '~ts' in virtual host '~ts'",
                     [Username, VirtualHost]),
    try
        R = rabbit_db_user:clear_topic_permissions(Username, VirtualHost, '_'),
        rabbit_log:info("Successfully cleared topic permissions for user '~ts' in virtual host '~ts'",
                        [Username, VirtualHost]),
        rabbit_event:notify(topic_permission_deleted, [{user,  Username},
            {vhost, VirtualHost},
            {user_who_performed_action, ActingUser}]),
        R
    catch
        Class:Error:Stacktrace ->
            rabbit_log:warning("Failed to clear topic permissions for user '~ts' in virtual host '~ts': ~tp",
                               [Username, VirtualHost, Error]),
            erlang:raise(Class, Error, Stacktrace)
    end.

clear_topic_permissions(Username, VirtualHost, Exchange, ActingUser) ->
    rabbit_log:debug("Asked to clear topic permissions on exchange '~ts' for user '~ts' in virtual host '~ts'",
                     [Exchange, Username, VirtualHost]),
    try
        R = rabbit_db_user:clear_topic_permissions(
              Username, VirtualHost, Exchange),
        rabbit_log:info("Successfully cleared topic permissions on exchange '~ts' for user '~ts' in virtual host '~ts'",
                        [Exchange, Username, VirtualHost]),
        rabbit_event:notify(topic_permission_deleted, [{user,  Username},
                                                       {vhost, VirtualHost},
                                                       {user_who_performed_action, ActingUser}]),
        R
    catch
        Class:Error:Stacktrace ->
            rabbit_log:warning("Failed to clear topic permissions on exchange '~ts' for user '~ts' in virtual host '~ts': ~tp",
                               [Exchange, Username, VirtualHost, Error]),
            erlang:raise(Class, Error, Stacktrace)
    end.

put_user(User, ActingUser) -> put_user(User, undefined, ActingUser).

put_user(User, Version, ActingUser) ->
    Username        = maps:get(name, User),
    HasPassword     = maps:is_key(password, User),
    HasPasswordHash = maps:is_key(password_hash, User),
    Password        = maps:get(password, User, undefined),
    PasswordHash    = maps:get(password_hash, User, undefined),

    Tags            = case {maps:get(tags, User, undefined), maps:get(administrator, User, undefined)} of
                          {undefined, undefined} ->
                              throw({error, tags_not_present});
                          {undefined, AdminS} ->
                              case rabbit_misc:parse_bool(AdminS) of
                                  true  -> [administrator];
                                  false -> []
                              end;
                          {TagsVal, _} ->
                              tag_list_from(TagsVal)
                      end,

    %% pre-configured, only applies to newly created users
    Permissions     = maps:get(permissions, User, undefined),

    PassedCredentialValidation =
        case {HasPassword, HasPasswordHash} of
            {true, false} ->
                rabbit_credential_validation:validate(Username, Password) =:= ok;
            {false, true} -> true;
            _             ->
                rabbit_credential_validation:validate(Username, Password) =:= ok
        end,

        Limits = case maps:get(limits, User, undefined) of
                     undefined ->
                         undefined;
                     Term ->
                         case validate_user_limits(Term) of
                             ok -> Term;
                             Error -> throw(Error)
                         end
                 end,
    case exists(Username) of
        true  ->
            case {HasPassword, HasPasswordHash} of
                {true, false} ->
                    update_user_password(PassedCredentialValidation, Username, Password, Tags, Limits, ActingUser);
                {false, true} ->
                    update_user_password_hash(Username, PasswordHash, Tags, Limits, User, Version);
                {true, true} ->
                    throw({error, both_password_and_password_hash_are_provided});
                %% clear password, update tags if needed
                _ ->
                    set_tags(Username, Tags, ActingUser),
                    clear_password(Username, ActingUser)
            end;
        false ->
            case {HasPassword, HasPasswordHash} of
                {true, false}  ->
                    create_user_with_password(PassedCredentialValidation, Username, Password, Tags, Permissions, Limits, ActingUser);
                {false, true}  ->
                    create_user_with_password_hash(Username, PasswordHash, Tags, User, Version, Permissions, Limits, ActingUser);
                {true, true}   ->
                    throw({error, both_password_and_password_hash_are_provided});
                {false, false} ->
                    %% this user won't be able to sign in using
                    %% a username/password pair but can be used for x509 certificate authentication,
                    %% with authn backends such as HTTP or LDAP and so on.
                    create_user_with_password(PassedCredentialValidation, Username, <<"">>, Tags, Permissions, Limits, ActingUser)
            end
    end.

update_user_password(_PassedCredentialValidation = true,  Username, Password, Tags, Limits, ActingUser) ->
    %% change_password, set_tags and limits
    update_user(Username, Password, Tags, Limits, ActingUser);
update_user_password(_PassedCredentialValidation = false, _Username, _Password, _Tags, _Limits, _ActingUser) ->
    %% we don't log here because
    %% rabbit_auth_backend_internal will do it
    throw({error, credential_validation_failed}).

update_user_password_hash(Username, PasswordHash, Tags, Limits, User, Version) ->
    %% when a hash this provided, credential validation
    %% is not applied
    HashingAlgorithm = hashing_algorithm(User, Version),

    Hash = rabbit_misc:b64decode_or_throw(PasswordHash),
    ConvertedTags = [rabbit_data_coercion:to_atom(I) || I <- Tags],
    update_user_with_hash(
      Username, Hash, HashingAlgorithm, ConvertedTags, Limits).

create_user_with_password(_PassedCredentialValidation = true,  Username, Password, Tags, undefined, Limits, ActingUser) ->
    ok = add_user(Username, Password, ActingUser, Limits, Tags);
create_user_with_password(_PassedCredentialValidation = true,  Username, Password, Tags, PreconfiguredPermissions, Limits, ActingUser) ->
    ok = add_user(Username, Password, ActingUser, Limits, Tags),
    preconfigure_permissions(Username, PreconfiguredPermissions, ActingUser);
create_user_with_password(_PassedCredentialValidation = false, _Username, _Password, _Tags, _, _, _) ->
    %% we don't log here because
    %% rabbit_auth_backend_internal will do it
    throw({error, credential_validation_failed}).

create_user_with_password_hash(Username, PasswordHash, Tags, User, Version, PreconfiguredPermissions, Limits, ActingUser) ->
    %% when a hash this provided, credential validation
    %% is not applied
    HashingAlgorithm = hashing_algorithm(User, Version),
    Hash             = rabbit_misc:b64decode_or_throw(PasswordHash),

    add_user_sans_validation(Username, Hash, HashingAlgorithm, Tags, Limits, ActingUser),
    preconfigure_permissions(Username, PreconfiguredPermissions, ActingUser).

preconfigure_permissions(_Username, undefined, _ActingUser) ->
    ok;
preconfigure_permissions(Username, Map, ActingUser) when is_map(Map) ->
    _ = maps:map(fun(VHost, M) ->
                     set_permissions(Username, VHost,
                                                  maps:get(<<"configure">>, M),
                                                  maps:get(<<"write">>,     M),
                                                  maps:get(<<"read">>,      M),
                                                  ActingUser)
             end,
             Map),
    ok.

set_user_limits(Username, Definition, ActingUser) when is_list(Definition); is_binary(Definition) ->
    case rabbit_json:try_decode(rabbit_data_coercion:to_binary(Definition)) of
        {ok, Term} ->
            validate_parameters_and_update_limit(Username, Term, ActingUser);
        {error, Reason} ->
            {error_string,
                rabbit_misc:format("Could not parse JSON document: ~tp", [Reason])}
    end;
set_user_limits(Username, Definition, ActingUser) when is_map(Definition) ->
    validate_parameters_and_update_limit(Username, Definition, ActingUser).

validate_parameters_and_update_limit(Username, Term, ActingUser) ->
    case validate_user_limits(Term) of
        ok ->
            rabbit_db_user:update(Username, fun(User) ->
                                                    internal_user:update_limits(add, User, Term)
                                            end),
            notify_limit_set(Username, ActingUser, Term);
        {errors, [{Reason, Arguments}]} ->
            {error_string, rabbit_misc:format(Reason, Arguments)}
    end.

validate_user_limits(Term) ->
    flatten_errors(rabbit_parameter_validation:proplist(
                     <<"user-limits">>, user_limit_validation(), Term)).

user_limit_validation() ->
    [{<<"max-connections">>, fun rabbit_parameter_validation:integer/2, optional},
     {<<"max-channels">>, fun rabbit_parameter_validation:integer/2, optional}].

clear_user_limits(Username, <<"all">>, ActingUser) ->
    rabbit_db_user:update(Username, fun(User) ->
                                            internal_user:clear_limits(User)
                                    end),
    notify_limit_clear(Username, ActingUser);
clear_user_limits(Username, LimitType, ActingUser) ->
    rabbit_db_user:update(Username, fun(User) ->
                                            internal_user:update_limits(remove, User, LimitType)
                                    end),
    notify_limit_clear(Username, ActingUser).

tag_list_from(Tags) when is_list(Tags) ->
    [to_atom(string:strip(to_list(T))) || T <- Tags];
tag_list_from(Tags) when is_binary(Tags) ->
    [to_atom(string:strip(T)) || T <- string:tokens(to_list(Tags), ",")].

flatten_errors(L) ->
    case [{F, A} || I <- lists:flatten([L]), {error, F, A} <- [I]] of
        [] -> ok;
        E  -> {errors, E}
    end.

%%----------------------------------------------------------------------------
%% Listing

-define(PERMS_INFO_KEYS, [configure, write, read]).
-define(USER_INFO_KEYS, [user, tags]).

-spec user_info_keys() -> rabbit_types:info_keys().

user_info_keys() -> ?USER_INFO_KEYS.

-spec perms_info_keys() -> rabbit_types:info_keys().

perms_info_keys()            -> [user, vhost | ?PERMS_INFO_KEYS].

-spec vhost_perms_info_keys() -> rabbit_types:info_keys().

vhost_perms_info_keys()      -> [user | ?PERMS_INFO_KEYS].

-spec user_perms_info_keys() -> rabbit_types:info_keys().

user_perms_info_keys()       -> [vhost | ?PERMS_INFO_KEYS].

-spec user_vhost_perms_info_keys() -> rabbit_types:info_keys().

user_vhost_perms_info_keys() -> ?PERMS_INFO_KEYS.

topic_perms_info_keys()            -> [user, vhost, exchange, write, read].
user_topic_perms_info_keys()       -> [vhost, exchange, write, read].
vhost_topic_perms_info_keys()      -> [user, exchange, write, read].
user_vhost_topic_perms_info_keys() -> [exchange, write, read].

all_users() -> rabbit_db_user:get_all().

-spec list_users() -> [rabbit_types:infos()].

list_users() ->
    [extract_internal_user_params(U) ||
        U <- all_users()].

-spec list_users(reference(), pid()) -> 'ok'.

list_users(Ref, AggregatorPid) ->
    rabbit_control_misc:emitting_map(
      AggregatorPid, Ref,
      fun(U) -> extract_internal_user_params(U) end,
      all_users()).

-spec list_permissions() -> [rabbit_types:infos()].

list_permissions() ->
    list_permissions(
      perms_info_keys(),
      rabbit_db_user:check_and_match_user_permissions('_', '_')).

list_permissions(Keys, UserPermissions) ->
    [extract_user_permission_params(Keys, U) ||
        U <- UserPermissions].

list_permissions(Keys, UserPermissions, Ref, AggregatorPid) ->
    rabbit_control_misc:emitting_map(
      AggregatorPid, Ref, fun(U) -> extract_user_permission_params(Keys, U) end,
      UserPermissions).

filter_props(Keys, Props) -> [T || T = {K, _} <- Props, lists:member(K, Keys)].

-spec list_user_permissions
        (rabbit_types:username()) -> [rabbit_types:infos()].

list_user_permissions(Username) ->
    list_permissions(
      user_perms_info_keys(),
      rabbit_db_user:check_and_match_user_permissions(Username, '_')).

-spec list_user_permissions
        (rabbit_types:username(), reference(), pid()) -> 'ok'.

list_user_permissions(Username, Ref, AggregatorPid) ->
    list_permissions(
      user_perms_info_keys(),
      rabbit_db_user:check_and_match_user_permissions(Username, '_'),
      Ref, AggregatorPid).

-spec list_vhost_permissions
        (rabbit_types:vhost()) -> [rabbit_types:infos()].

list_vhost_permissions(VHostPath) ->
    list_permissions(
      vhost_perms_info_keys(),
      rabbit_db_user:check_and_match_user_permissions('_', VHostPath)).

-spec list_vhost_permissions
        (rabbit_types:vhost(), reference(), pid()) -> 'ok'.

list_vhost_permissions(VHostPath, Ref, AggregatorPid) ->
    list_permissions(
      vhost_perms_info_keys(),
      rabbit_db_user:check_and_match_user_permissions('_', VHostPath),
      Ref, AggregatorPid).

-spec list_user_vhost_permissions
        (rabbit_types:username(), rabbit_types:vhost()) -> [rabbit_types:infos()].

list_user_vhost_permissions(Username, VHostPath) ->
    list_permissions(
      user_vhost_perms_info_keys(),
      rabbit_db_user:check_and_match_user_permissions(Username, VHostPath)).

extract_user_permission_params(Keys, #user_permission{
                                        user_vhost =
                                            #user_vhost{username     = Username,
                                                        virtual_host = VHostPath},
                                        permission = #permission{
                                                        configure = ConfigurePerm,
                                                        write     = WritePerm,
                                                        read      = ReadPerm}}) ->
    filter_props(Keys, [{user,      Username},
                        {vhost,     VHostPath},
                        {configure, ConfigurePerm},
                        {write,     WritePerm},
                        {read,      ReadPerm}]).

extract_internal_user_params(User) ->
    [{user, internal_user:get_username(User)},
     {tags, internal_user:get_tags(User)}].

list_topic_permissions() ->
    list_topic_permissions(
      topic_perms_info_keys(),
      rabbit_db_user:check_and_match_topic_permissions('_', '_', '_')).

list_user_topic_permissions(Username) ->
    list_topic_permissions(
      user_topic_perms_info_keys(),
      rabbit_db_user:check_and_match_topic_permissions(Username, '_', '_')).

list_vhost_topic_permissions(VHost) ->
    list_topic_permissions(
      vhost_topic_perms_info_keys(),
      rabbit_db_user:check_and_match_topic_permissions('_', VHost, '_')).

list_user_vhost_topic_permissions(Username, VHost) ->
    list_topic_permissions(
      user_vhost_topic_perms_info_keys(),
      rabbit_db_user:check_and_match_topic_permissions(Username, VHost, '_')).

list_topic_permissions(Keys, TopicPermissions) ->
    [extract_topic_permission_params(Keys, U) ||
        U <- TopicPermissions].

extract_topic_permission_params(Keys, #topic_permission{
            topic_permission_key = #topic_permission_key{
                                    user_vhost = #user_vhost{username     = Username,
                                                             virtual_host = VHostPath},
                                    exchange = Exchange},
            permission = #permission{
                write     = WritePerm,
                read      = ReadPerm}}) ->
    filter_props(Keys, [{user,      Username},
        {vhost,     VHostPath},
        {exchange,  Exchange},
        {write,     WritePerm},
        {read,      ReadPerm}]).

hashing_algorithm(User, Version) ->
    case maps:get(hashing_algorithm, User, undefined) of
        undefined ->
            case Version of
                %% 3.6.1 and later versions are supposed to have
                %% the algorithm exported and thus not need a default
                <<"3.6.0">>          -> rabbit_password_hashing_sha256;
                <<"3.5.", _/binary>> -> rabbit_password_hashing_md5;
                <<"3.4.", _/binary>> -> rabbit_password_hashing_md5;
                <<"3.3.", _/binary>> -> rabbit_password_hashing_md5;
                <<"3.2.", _/binary>> -> rabbit_password_hashing_md5;
                <<"3.1.", _/binary>> -> rabbit_password_hashing_md5;
                <<"3.0.", _/binary>> -> rabbit_password_hashing_md5;
                _                    -> rabbit_password:hashing_mod()
            end;
        Alg       -> rabbit_data_coercion:to_atom(Alg, utf8)
    end.

is_over_connection_limit(Username) ->
    Fun = fun() ->
              rabbit_connection_tracking:count_tracked_items_in({user, Username})
          end,
    is_over_limit(Username, <<"max-connections">>, Fun).

is_over_channel_limit(Username) ->
    Fun = fun() ->
              rabbit_channel_tracking:count_tracked_items_in({user, Username})
          end,
    is_over_limit(Username, <<"max-channels">>, Fun).

is_over_limit(Username, LimitType, Fun) ->
    case get_user_limit(Username, LimitType) of
        undefined -> false;
        {ok, 0} -> {true, 0};
        {ok, Limit} ->
            case Fun() >= Limit of
                false -> false;
                true -> {true, Limit}
            end
    end.

get_user_limit(Username, LimitType) ->
    case lookup_user(Username) of
        {ok, User} ->
            case rabbit_misc:pget(LimitType, internal_user:get_limits(User)) of
                undefined -> undefined;
                N when N < 0  -> undefined;
                N when N >= 0 -> {ok, N}
            end;
        _ ->
            undefined
    end.

get_user_limits() ->
    [{internal_user:get_username(U), internal_user:get_limits(U)} ||
        U <- all_users(),
        internal_user:get_limits(U) =/= #{}].

get_user_limits(Username) ->
    case lookup_user(Username) of
        {ok, User} -> internal_user:get_limits(User);
        _ -> undefined
    end.

notify_limit_set(Username, ActingUser, Term) ->
    rabbit_event:notify(user_limits_set,
        [{name, <<"limits">>},  {user_who_performed_action, ActingUser},
        {username, Username}  | maps:to_list(Term)]).

notify_limit_clear(Username, ActingUser) ->
    rabbit_event:notify(user_limits_cleared,
        [{name, <<"limits">>}, {user_who_performed_action, ActingUser},
        {username, Username}]).
