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
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2007-2011 VMware, Inc.  All rights reserved.
%%

-module(rabbit_auth_backend_internal).
-include("rabbit.hrl").

-behaviour(rabbit_auth_backend).

-export([description/0]).
-export([check_user_login/2, check_vhost_access/3, check_resource_access/3]).

-export([add_user/2, delete_user/1, change_password/2, set_admin/1,
         clear_admin/1, list_users/0, lookup_user/1, clear_password/1]).
-export([make_salt/0, check_password/2, change_password_hash/2,
         hash_password/1]).
-export([set_permissions/5, clear_permissions/2,
         list_permissions/0, list_vhost_permissions/1, list_user_permissions/1,
         list_user_vhost_permissions/2]).

-include("rabbit_auth_backend_spec.hrl").

-ifdef(use_specs).

-type(regexp() :: binary()).

-spec(add_user/2 :: (rabbit_types:username(), rabbit_types:password()) -> 'ok').
-spec(delete_user/1 :: (rabbit_types:username()) -> 'ok').
-spec(change_password/2 :: (rabbit_types:username(), rabbit_types:password())
                           -> 'ok').
-spec(clear_password/1 :: (rabbit_types:username()) -> 'ok').
-spec(make_salt/0 :: () -> binary()).
-spec(check_password/2 :: (rabbit_types:password(),
                           rabbit_types:password_hash()) -> boolean()).
-spec(change_password_hash/2 :: (rabbit_types:username(),
                                 rabbit_types:password_hash()) -> 'ok').
-spec(hash_password/1 :: (rabbit_types:password())
                         -> rabbit_types:password_hash()).
-spec(set_admin/1 :: (rabbit_types:username()) -> 'ok').
-spec(clear_admin/1 :: (rabbit_types:username()) -> 'ok').
-spec(list_users/0 :: () -> [{rabbit_types:username(), boolean()}]).
-spec(lookup_user/1 :: (rabbit_types:username())
                       -> rabbit_types:ok(rabbit_types:internal_user())
                              | rabbit_types:error('not_found')).
-spec(set_permissions/5 ::(rabbit_types:username(), rabbit_types:vhost(),
                           regexp(), regexp(), regexp()) -> 'ok').
-spec(clear_permissions/2 :: (rabbit_types:username(), rabbit_types:vhost())
                             -> 'ok').
-spec(list_permissions/0 ::
        () -> [{rabbit_types:username(), rabbit_types:vhost(),
                regexp(), regexp(), regexp()}]).
-spec(list_vhost_permissions/1 ::
        (rabbit_types:vhost()) -> [{rabbit_types:username(),
                                    regexp(), regexp(), regexp()}]).
-spec(list_user_permissions/1 ::
        (rabbit_types:username()) -> [{rabbit_types:vhost(),
                                       regexp(), regexp(), regexp()}]).
-spec(list_user_vhost_permissions/2 ::
        (rabbit_types:username(), rabbit_types:vhost())
        -> [{regexp(), regexp(), regexp()}]).

-endif.

%%----------------------------------------------------------------------------

%% Implementation of rabbit_auth_backend

description() ->
    [{name, <<"Internal">>},
     {description, <<"Internal user / password database">>}].

check_user_login(Username, []) ->
    internal_check_user_login(Username, fun(_) -> true end);
check_user_login(Username, [{password, Password}]) ->
    internal_check_user_login(
      Username, fun(#internal_user{password_hash = Hash}) ->
                        check_password(Password, Hash)
                end);
check_user_login(Username, AuthProps) ->
    exit({unknown_auth_props, Username, AuthProps}).

internal_check_user_login(Username, Fun) ->
    Refused = {refused, "user '~s' - invalid credentials", [Username]},
    case lookup_user(Username) of
        {ok, User = #internal_user{is_admin = IsAdmin}} ->
            case Fun(User) of
                true -> {ok, #user{username     = Username,
                                   is_admin     = IsAdmin,
                                   auth_backend = ?MODULE,
                                   impl         = User}};
                _    -> Refused
            end;
        {error, not_found} ->
            Refused
    end.

check_vhost_access(#user{is_admin = true},    _VHostPath, read) ->
    true;

check_vhost_access(#user{username = Username}, VHostPath, _) ->
    %% TODO: use dirty ops instead
    rabbit_misc:execute_mnesia_transaction(
      fun () ->
              case mnesia:read({rabbit_user_permission,
                                #user_vhost{username     = Username,
                                            virtual_host = VHostPath}}) of
                  []   -> false;
                  [_R] -> true
              end
      end).

check_resource_access(#user{username = Username},
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
    R = rabbit_misc:execute_mnesia_transaction(
          fun () ->
                  case mnesia:wread({rabbit_user, Username}) of
                      [] ->
                          ok = mnesia:write(
                                 rabbit_user,
                                 #internal_user{username = Username,
                                                password_hash =
                                                    hash_password(Password),
                                                is_admin = false},
                                 write);
                      _ ->
                          mnesia:abort({user_already_exists, Username})
                  end
          end),
    rabbit_log:info("Created user ~p~n", [Username]),
    R.

delete_user(Username) ->
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
    rabbit_log:info("Deleted user ~p~n", [Username]),
    R.

change_password(Username, Password) ->
    change_password_hash(Username, hash_password(Password)).

clear_password(Username) ->
    change_password_hash(Username, <<"">>).

change_password_hash(Username, PasswordHash) ->
    R = update_user(Username, fun(User) ->
                                      User#internal_user{
                                        password_hash = PasswordHash }
                              end),
    rabbit_log:info("Changed password for user ~p~n", [Username]),
    R.

hash_password(Cleartext) ->
    Salt = make_salt(),
    Hash = salted_md5(Salt, Cleartext),
    <<Salt/binary, Hash/binary>>.

check_password(Cleartext, <<Salt:4/binary, Hash/binary>>) ->
    Hash =:= salted_md5(Salt, Cleartext).

make_salt() ->
    {A1,A2,A3} = now(),
    random:seed(A1, A2, A3),
    Salt = random:uniform(16#ffffffff),
    <<Salt:32>>.

salted_md5(Salt, Cleartext) ->
    Salted = <<Salt/binary, Cleartext/binary>>,
    erlang:md5(Salted).

set_admin(Username)   -> set_admin(Username, true).

clear_admin(Username) -> set_admin(Username, false).

set_admin(Username, IsAdmin) ->
    R = update_user(Username, fun(User) ->
                                      User#internal_user{is_admin = IsAdmin}
                              end),
    rabbit_log:info("Set user admin flag for user ~p to ~p~n",
                    [Username, IsAdmin]),
    R.

update_user(Username, Fun) ->
    rabbit_misc:execute_mnesia_transaction(
      rabbit_misc:with_user(
        Username,
        fun () ->
                {ok, User} = lookup_user(Username),
                ok = mnesia:write(rabbit_user, Fun(User), write)
        end)).

list_users() ->
    [{Username, IsAdmin} ||
        #internal_user{username = Username, is_admin = IsAdmin} <-
            mnesia:dirty_match_object(rabbit_user, #internal_user{_ = '_'})].

lookup_user(Username) ->
    rabbit_misc:dirty_read({rabbit_user, Username}).

validate_regexp(RegexpBin) ->
    Regexp = binary_to_list(RegexpBin),
    case re:compile(Regexp) of
        {ok, _}         -> ok;
        {error, Reason} -> throw({error, {invalid_regexp, Regexp, Reason}})
    end.

set_permissions(Username, VHostPath, ConfigurePerm, WritePerm, ReadPerm) ->
    lists:map(fun validate_regexp/1, [ConfigurePerm, WritePerm, ReadPerm]),
    rabbit_misc:execute_mnesia_transaction(
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
        end)).


clear_permissions(Username, VHostPath) ->
    rabbit_misc:execute_mnesia_transaction(
      rabbit_misc:with_user_and_vhost(
        Username, VHostPath,
        fun () ->
                ok = mnesia:delete({rabbit_user_permission,
                                    #user_vhost{username     = Username,
                                                virtual_host = VHostPath}})
        end)).

list_permissions() ->
    [{Username, VHostPath, ConfigurePerm, WritePerm, ReadPerm} ||
        {Username, VHostPath, ConfigurePerm, WritePerm, ReadPerm} <-
            list_permissions(match_user_vhost('_', '_'))].

list_vhost_permissions(VHostPath) ->
    [{Username, ConfigurePerm, WritePerm, ReadPerm} ||
        {Username, _, ConfigurePerm, WritePerm, ReadPerm} <-
            list_permissions(rabbit_vhost:with(
                               VHostPath, match_user_vhost('_', VHostPath)))].

list_user_permissions(Username) ->
    [{VHostPath, ConfigurePerm, WritePerm, ReadPerm} ||
        {_, VHostPath, ConfigurePerm, WritePerm, ReadPerm} <-
            list_permissions(rabbit_misc:with_user(
                               Username, match_user_vhost(Username, '_')))].

list_user_vhost_permissions(Username, VHostPath) ->
    [{ConfigurePerm, WritePerm, ReadPerm} ||
        {_, _, ConfigurePerm, WritePerm, ReadPerm} <-
            list_permissions(rabbit_misc:with_user_and_vhost(
                               Username, VHostPath,
                               match_user_vhost(Username, VHostPath)))].

list_permissions(QueryThunk) ->
    [{Username, VHostPath, ConfigurePerm, WritePerm, ReadPerm} ||
        #user_permission{user_vhost = #user_vhost{username     = Username,
                                                  virtual_host = VHostPath},
                         permission = #permission{ configure = ConfigurePerm,
                                                   write     = WritePerm,
                                                   read      = ReadPerm}} <-
            %% TODO: use dirty ops instead
            rabbit_misc:execute_mnesia_transaction(QueryThunk)].

match_user_vhost(Username, VHostPath) ->
    fun () -> mnesia:match_object(
                rabbit_user_permission,
                #user_permission{user_vhost = #user_vhost{
                                   username     = Username,
                                   virtual_host = VHostPath},
                                 permission = '_'},
                read)
    end.
