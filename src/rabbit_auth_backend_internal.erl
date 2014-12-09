%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2014 GoPivotal, Inc.  All rights reserved.
%%

-module(rabbit_auth_backend_internal).
-include("rabbit.hrl").

-behaviour(rabbit_authn_backend).
-behaviour(rabbit_authz_backend).

-export([user_login_authentication/2, user_login_authorization/1,
         check_vhost_access/3, check_resource_access/3]).

-export([add_user/2, delete_user/1, lookup_user/1,
         change_password/2, clear_password/1,
         hash_password/1, change_password_hash/2,
         set_tags/2, set_permissions/5, clear_permissions/2]).
-export([user_info_keys/0, perms_info_keys/0,
         user_perms_info_keys/0, vhost_perms_info_keys/0,
         user_vhost_perms_info_keys/0,
         list_users/0, list_permissions/0,
         list_user_permissions/1, list_vhost_permissions/1,
         list_user_vhost_permissions/2]).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-type(regexp() :: binary()).

-spec(add_user/2 :: (rabbit_types:username(), rabbit_types:password()) -> 'ok').
-spec(delete_user/1 :: (rabbit_types:username()) -> 'ok').
-spec(lookup_user/1 :: (rabbit_types:username())
                       -> rabbit_types:ok(rabbit_types:internal_user())
                              | rabbit_types:error('not_found')).
-spec(change_password/2 :: (rabbit_types:username(), rabbit_types:password())
                           -> 'ok').
-spec(clear_password/1 :: (rabbit_types:username()) -> 'ok').
-spec(hash_password/1 :: (rabbit_types:password())
                         -> rabbit_types:password_hash()).
-spec(change_password_hash/2 :: (rabbit_types:username(),
                                 rabbit_types:password_hash()) -> 'ok').
-spec(set_tags/2 :: (rabbit_types:username(), [atom()]) -> 'ok').
-spec(set_permissions/5 ::(rabbit_types:username(), rabbit_types:vhost(),
                           regexp(), regexp(), regexp()) -> 'ok').
-spec(clear_permissions/2 :: (rabbit_types:username(), rabbit_types:vhost())
                             -> 'ok').
-spec(user_info_keys/0 :: () -> rabbit_types:info_keys()).
-spec(perms_info_keys/0 :: () -> rabbit_types:info_keys()).
-spec(user_perms_info_keys/0 :: () -> rabbit_types:info_keys()).
-spec(vhost_perms_info_keys/0 :: () -> rabbit_types:info_keys()).
-spec(user_vhost_perms_info_keys/0 :: () -> rabbit_types:info_keys()).
-spec(list_users/0 :: () -> [rabbit_types:infos()]).
-spec(list_permissions/0 :: () -> [rabbit_types:infos()]).
-spec(list_user_permissions/1 ::
        (rabbit_types:username()) -> [rabbit_types:infos()]).
-spec(list_vhost_permissions/1 ::
        (rabbit_types:vhost()) -> [rabbit_types:infos()]).
-spec(list_user_vhost_permissions/2 ::
        (rabbit_types:username(), rabbit_types:vhost())
        -> [rabbit_types:infos()]).

-endif.

%%----------------------------------------------------------------------------
%% Implementation of rabbit_auth_backend

user_login_authentication(Username, []) ->
    internal_check_user_login(Username, fun(_) -> true end);
user_login_authentication(Username, [{password, Cleartext}]) ->
    internal_check_user_login(
      Username,
      fun (#internal_user{password_hash = <<Salt:4/binary, Hash/binary>>}) ->
              Hash =:= salted_md5(Salt, Cleartext);
          (#internal_user{}) ->
              false
      end);
user_login_authentication(Username, AuthProps) ->
    exit({unknown_auth_props, Username, AuthProps}).

user_login_authorization(Username) ->
    case user_login_authentication(Username, []) of
        {ok, #auth_user{impl = Impl}} -> {ok, Impl};
        Else                          -> Else
    end.

internal_check_user_login(Username, Fun) ->
    Refused = {refused, "user '~s' - invalid credentials", [Username]},
    case lookup_user(Username) of
        {ok, User = #internal_user{tags = Tags}} ->
            case Fun(User) of
                true -> {ok, #auth_user{username = Username,
                                        tags     = Tags,
                                        impl     = none}};
                _    -> Refused
            end;
        {error, not_found} ->
            Refused
    end.

check_vhost_access(#auth_user{username = Username}, VHostPath, _Sock) ->
    case mnesia:dirty_read({rabbit_user_permission,
                            #user_vhost{username     = Username,
                                        virtual_host = VHostPath}}) of
        []   -> false;
        [_R] -> true
    end.

check_resource_access(#auth_user{username = Username},
                      #resource{virtual_host = VHostPath, name = Name},
                      Permission) ->
    case mnesia:dirty_read({rabbit_user_permission,
                            #user_vhost{username     = Username,
                                        virtual_host = VHostPath}}) of
        [] ->
            false;
        [#user_permission{permission = P}] ->
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

permission_index(configure) -> #permission.configure;
permission_index(write)     -> #permission.write;
permission_index(read)      -> #permission.read.

%%----------------------------------------------------------------------------
%% Manipulation of the user database

add_user(Username, Password) ->
    rabbit_log:info("Creating user '~s'~n", [Username]),
    R = rabbit_misc:execute_mnesia_transaction(
          fun () ->
                  case mnesia:wread({rabbit_user, Username}) of
                      [] ->
                          ok = mnesia:write(
                                 rabbit_user,
                                 #internal_user{username = Username,
                                                password_hash =
                                                    hash_password(Password),
                                                tags = []},
                                 write);
                      _ ->
                          mnesia:abort({user_already_exists, Username})
                  end
          end),
    rabbit_event:notify(user_created, [{name, Username}]),
    R.

delete_user(Username) ->
    rabbit_log:info("Deleting user '~s'~n", [Username]),
    R = rabbit_misc:execute_mnesia_transaction(
          rabbit_misc:with_user(
            Username,
            fun () ->
                    ok = mnesia:delete({rabbit_user, Username}),
                    [ok = mnesia:delete_object(
                            rabbit_user_permission, R, write) ||
                        R <- mnesia:match_object(
                               rabbit_user_permission,
                               #user_permission{user_vhost = #user_vhost{
                                                  username = Username,
                                                  virtual_host = '_'},
                                                permission = '_'},
                               write)],
                    ok
            end)),
    rabbit_event:notify(user_deleted, [{name, Username}]),
    R.

lookup_user(Username) ->
    rabbit_misc:dirty_read({rabbit_user, Username}).

change_password(Username, Password) ->
    rabbit_log:info("Changing password for '~s'~n", [Username]),
    R = change_password_hash(Username, hash_password(Password)),
    rabbit_event:notify(user_password_changed, [{name, Username}]),
    R.

clear_password(Username) ->
    rabbit_log:info("Clearing password for '~s'~n", [Username]),
    R = change_password_hash(Username, <<"">>),
    rabbit_event:notify(user_password_cleared, [{name, Username}]),
    R.

hash_password(Cleartext) ->
    {A1,A2,A3} = now(),
    random:seed(A1, A2, A3),
    Salt = random:uniform(16#ffffffff),
    SaltBin = <<Salt:32>>,
    Hash = salted_md5(SaltBin, Cleartext),
    <<SaltBin/binary, Hash/binary>>.

change_password_hash(Username, PasswordHash) ->
    update_user(Username, fun(User) ->
                                  User#internal_user{
                                    password_hash = PasswordHash }
                          end).

salted_md5(Salt, Cleartext) ->
    Salted = <<Salt/binary, Cleartext/binary>>,
    erlang:md5(Salted).

set_tags(Username, Tags) ->
    rabbit_log:info("Setting user tags for user '~s' to ~p~n",
                    [Username, Tags]),
    R = update_user(Username, fun(User) ->
                                      User#internal_user{tags = Tags}
                              end),
    rabbit_event:notify(user_tags_set, [{name, Username}, {tags, Tags}]),
    R.

set_permissions(Username, VHostPath, ConfigurePerm, WritePerm, ReadPerm) ->
    rabbit_log:info("Setting permissions for "
                    "'~s' in '~s' to '~s', '~s', '~s'~n",
                    [Username, VHostPath, ConfigurePerm, WritePerm, ReadPerm]),
    lists:map(
      fun (RegexpBin) ->
              Regexp = binary_to_list(RegexpBin),
              case re:compile(Regexp) of
                  {ok, _}         -> ok;
                  {error, Reason} -> throw({error, {invalid_regexp,
                                                    Regexp, Reason}})
              end
      end, [ConfigurePerm, WritePerm, ReadPerm]),
    R = rabbit_misc:execute_mnesia_transaction(
          rabbit_misc:with_user_and_vhost(
            Username, VHostPath,
            fun () -> ok = mnesia:write(
                             rabbit_user_permission,
                             #user_permission{user_vhost = #user_vhost{
                                                username     = Username,
                                                virtual_host = VHostPath},
                                              permission = #permission{
                                                configure = ConfigurePerm,
                                                write     = WritePerm,
                                                read      = ReadPerm}},
                             write)
            end)),
    rabbit_event:notify(permission_created, [{user,      Username}, 
                                             {vhost,     VHostPath},
                                             {configure, ConfigurePerm},
                                             {write,     WritePerm},
                                             {read,      ReadPerm}]),
    R.

clear_permissions(Username, VHostPath) ->
    R = rabbit_misc:execute_mnesia_transaction(
          rabbit_misc:with_user_and_vhost(
            Username, VHostPath,
            fun () ->
                    ok = mnesia:delete({rabbit_user_permission,
                                        #user_vhost{username     = Username,
                                                    virtual_host = VHostPath}})
            end)),
    rabbit_event:notify(permission_deleted, [{user,  Username},
                                             {vhost, VHostPath}]),
    R.


update_user(Username, Fun) ->
    rabbit_misc:execute_mnesia_transaction(
      rabbit_misc:with_user(
        Username,
        fun () ->
                {ok, User} = lookup_user(Username),
                ok = mnesia:write(rabbit_user, Fun(User), write)
        end)).

%%----------------------------------------------------------------------------
%% Listing

-define(PERMS_INFO_KEYS, [configure, write, read]).
-define(USER_INFO_KEYS, [user, tags]).

user_info_keys() -> ?USER_INFO_KEYS.

perms_info_keys()            -> [user, vhost | ?PERMS_INFO_KEYS].
vhost_perms_info_keys()      -> [user | ?PERMS_INFO_KEYS].
user_perms_info_keys()       -> [vhost | ?PERMS_INFO_KEYS].
user_vhost_perms_info_keys() -> ?PERMS_INFO_KEYS.

list_users() ->
    [[{user, Username}, {tags, Tags}] ||
        #internal_user{username = Username, tags = Tags} <-
            mnesia:dirty_match_object(rabbit_user, #internal_user{_ = '_'})].

list_permissions() ->
    list_permissions(perms_info_keys(), match_user_vhost('_', '_')).

list_permissions(Keys, QueryThunk) ->
    [filter_props(Keys, [{user,      Username},
                         {vhost,     VHostPath},
                         {configure, ConfigurePerm},
                         {write,     WritePerm},
                         {read,      ReadPerm}]) ||
        #user_permission{user_vhost = #user_vhost{username     = Username,
                                                  virtual_host = VHostPath},
                         permission = #permission{ configure = ConfigurePerm,
                                                   write     = WritePerm,
                                                   read      = ReadPerm}} <-
            %% TODO: use dirty ops instead
            rabbit_misc:execute_mnesia_transaction(QueryThunk)].

filter_props(Keys, Props) -> [T || T = {K, _} <- Props, lists:member(K, Keys)].

list_user_permissions(Username) ->
    list_permissions(
      user_perms_info_keys(),
      rabbit_misc:with_user(Username, match_user_vhost(Username, '_'))).

list_vhost_permissions(VHostPath) ->
    list_permissions(
      vhost_perms_info_keys(),
      rabbit_vhost:with(VHostPath, match_user_vhost('_', VHostPath))).

list_user_vhost_permissions(Username, VHostPath) ->
    list_permissions(
      user_vhost_perms_info_keys(),
      rabbit_misc:with_user_and_vhost(
        Username, VHostPath, match_user_vhost(Username, VHostPath))).

match_user_vhost(Username, VHostPath) ->
    fun () -> mnesia:match_object(
                rabbit_user_permission,
                #user_permission{user_vhost = #user_vhost{
                                   username     = Username,
                                   virtual_host = VHostPath},
                                 permission = '_'},
                read)
    end.
