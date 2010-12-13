%%   The contents of this file are subject to the Mozilla Public License
%%   Version 1.1 (the "License"); you may not use this file except in
%%   compliance with the License. You may obtain a copy of the License at
%%   http://www.mozilla.org/MPL/
%%
%%   Software distributed under the License is distributed on an "AS IS"
%%   basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%%   License for the specific language governing rights and limitations
%%   under the License.
%%
%%   The Original Code is RabbitMQ.
%%
%%   The Initial Developers of the Original Code are LShift Ltd,
%%   Cohesive Financial Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created before 22-Nov-2008 00:00:00 GMT by LShift Ltd,
%%   Cohesive Financial Technologies LLC, or Rabbit Technologies Ltd
%%   are Copyright (C) 2007-2008 LShift Ltd, Cohesive Financial
%%   Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created by LShift Ltd are Copyright (C) 2007-2010 LShift
%%   Ltd. Portions created by Cohesive Financial Technologies LLC are
%%   Copyright (C) 2007-2010 Cohesive Financial Technologies
%%   LLC. Portions created by Rabbit Technologies Ltd are Copyright
%%   (C) 2007-2010 Rabbit Technologies Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%

-module(rabbit_access_control).
-include_lib("stdlib/include/qlc.hrl").
-include("rabbit.hrl").

-export([user_pass_login/2, check_user_pass_login/2, make_salt/0,
         check_vhost_access/2, check_resource_access/3]).
-export([add_user/2, delete_user/1, change_password/2, set_admin/1,
         clear_admin/1, list_users/0, lookup_user/1]).
-export([change_password_hash/2, hash_password/1]).
-export([add_vhost/1, delete_vhost/1, vhost_exists/1, list_vhosts/0]).
-export([set_permissions/5, clear_permissions/2,
         list_permissions/0, list_vhost_permissions/1, list_user_permissions/1,
         list_user_vhost_permissions/2]).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-export_type([username/0, password/0, password_hash/0]).

-type(permission_atom() :: 'configure' | 'read' | 'write').
-type(username() :: binary()).
-type(password() :: binary()).
-type(password_hash() :: binary()).
-type(regexp() :: binary()).
-spec(user_pass_login/2 ::
        (username(), password())
        -> rabbit_types:user() | rabbit_types:channel_exit()).
-spec(check_user_pass_login/2 ::
        (username(), password())
        -> {'ok', rabbit_types:user()} | {'refused', username()}).
-spec(make_salt/0 :: () -> binary()).
-spec(check_vhost_access/2 ::
        (rabbit_types:user(), rabbit_types:vhost())
        -> 'ok' | rabbit_types:channel_exit()).
-spec(check_resource_access/3 ::
        (username(), rabbit_types:r(atom()), permission_atom())
        -> 'ok' | rabbit_types:channel_exit()).
-spec(add_user/2 :: (username(), password()) -> 'ok').
-spec(delete_user/1 :: (username()) -> 'ok').
-spec(change_password/2 :: (username(), password()) -> 'ok').
-spec(change_password_hash/2 :: (username(), password_hash()) -> 'ok').
-spec(hash_password/1 :: (password()) -> password_hash()).
-spec(set_admin/1 :: (username()) -> 'ok').
-spec(clear_admin/1 :: (username()) -> 'ok').
-spec(list_users/0 :: () -> [{username(), boolean()}]).
-spec(lookup_user/1 ::
        (username()) -> rabbit_types:ok(rabbit_types:user())
                            | rabbit_types:error('not_found')).
-spec(add_vhost/1 :: (rabbit_types:vhost()) -> 'ok').
-spec(delete_vhost/1 :: (rabbit_types:vhost()) -> 'ok').
-spec(vhost_exists/1 :: (rabbit_types:vhost()) -> boolean()).
-spec(list_vhosts/0 :: () -> [rabbit_types:vhost()]).
-spec(set_permissions/5 ::(username(), rabbit_types:vhost(), regexp(),
                           regexp(), regexp()) -> 'ok').
-spec(clear_permissions/2 :: (username(), rabbit_types:vhost()) -> 'ok').
-spec(list_permissions/0 ::
        () -> [{username(), rabbit_types:vhost(), regexp(), regexp(), regexp()}]).
-spec(list_vhost_permissions/1 ::
        (rabbit_types:vhost()) -> [{username(), regexp(), regexp(), regexp()}]).
-spec(list_user_permissions/1 ::
        (username()) -> [{rabbit_types:vhost(), regexp(), regexp(), regexp()}]).
-spec(list_user_vhost_permissions/2 ::
        (username(), rabbit_types:vhost()) -> [{regexp(), regexp(), regexp()}]).

-endif.

%%----------------------------------------------------------------------------

user_pass_login(User, Pass) ->
    ?LOGDEBUG("Login with user ~p pass ~p~n", [User, Pass]),
    case check_user_pass_login(User, Pass) of
        {refused, _} ->
            rabbit_misc:protocol_error(
              access_refused, "login refused for user '~s'", [User]);
        {ok, U} ->
            U
    end.

check_user_pass_login(Username, Pass) ->
    Refused = {refused, io_lib:format("user '~s' - invalid credentials",
                                      [Username])},
    case lookup_user(Username) of
        {ok, User} ->
            case check_password(Pass, User#user.password_hash) of
                true -> {ok, User};
                _    -> Refused
            end;
        {error, not_found} ->
            Refused
    end.

internal_lookup_vhost_access(Username, VHostPath) ->
    %% TODO: use dirty ops instead
    rabbit_misc:execute_mnesia_transaction(
      fun () ->
              case mnesia:read({rabbit_user_permission,
                                #user_vhost{username     = Username,
                                            virtual_host = VHostPath}}) of
                  [] -> not_found;
                  [R] -> {ok, R}
              end
      end).

check_vhost_access(#user{username = Username}, VHostPath) ->
    ?LOGDEBUG("Checking VHost access for ~p to ~p~n", [Username, VHostPath]),
    case internal_lookup_vhost_access(Username, VHostPath) of
        {ok, _R} ->
            ok;
        not_found ->
            rabbit_misc:protocol_error(
              access_refused, "access to vhost '~s' refused for user '~s'",
              [VHostPath, Username])
    end.

permission_index(configure) -> #permission.configure;
permission_index(write)     -> #permission.write;
permission_index(read)      -> #permission.read.

check_resource_access(Username,
                      R = #resource{kind = exchange, name = <<"">>},
                      Permission) ->
    check_resource_access(Username,
                          R#resource{name = <<"amq.default">>},
                          Permission);
check_resource_access(Username,
                      R = #resource{virtual_host = VHostPath, name = Name},
                      Permission) ->
    Res = case mnesia:dirty_read({rabbit_user_permission,
                                  #user_vhost{username     = Username,
                                              virtual_host = VHostPath}}) of
              [] ->
                  false;
              [#user_permission{permission = P}] ->
                  PermRegexp =
                      case element(permission_index(Permission), P) of
                          %% <<"^$">> breaks Emacs' erlang mode
                          <<"">> -> <<$^, $$>>;
                          RE     -> RE
                      end,
                  case re:run(Name, PermRegexp, [{capture, none}]) of
                      match    -> true;
                      nomatch  -> false
                  end
          end,
    if Res  -> ok;
       true -> rabbit_misc:protocol_error(
                 access_refused, "access to ~s refused for user '~s'",
                 [rabbit_misc:rs(R), Username])
    end.

add_user(Username, Password) ->
    R = rabbit_misc:execute_mnesia_transaction(
          fun () ->
                  case mnesia:wread({rabbit_user, Username}) of
                      [] ->
                          ok = mnesia:write(rabbit_user,
                                            #user{username = Username,
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

change_password_hash(Username, PasswordHash) ->
    R = update_user(Username, fun(User) ->
                                      User#user{ password_hash = PasswordHash }
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

set_admin(Username) ->
    set_admin(Username, true).

clear_admin(Username) ->
    set_admin(Username, false).

set_admin(Username, IsAdmin) ->
    R = update_user(Username, fun(User) ->
                                      User#user{is_admin = IsAdmin}
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
        #user{username = Username, is_admin = IsAdmin} <-
            mnesia:dirty_match_object(rabbit_user, #user{_ = '_'})].

lookup_user(Username) ->
    rabbit_misc:dirty_read({rabbit_user, Username}).

add_vhost(VHostPath) ->
    R = rabbit_misc:execute_mnesia_transaction(
          fun () ->
                  case mnesia:wread({rabbit_vhost, VHostPath}) of
                      [] ->
                          ok = mnesia:write(rabbit_vhost,
                                            #vhost{virtual_host = VHostPath},
                                            write),
                          [rabbit_exchange:declare(
                             rabbit_misc:r(VHostPath, exchange, Name),
                             Type, true, false, []) ||
                              {Name,Type} <-
                                  [{<<"">>,           direct},
                                   {<<"amq.direct">>, direct},
                                   {<<"amq.topic">>,  topic},
                                   {<<"amq.match">>,  headers}, %% per 0-9-1 pdf
                                   {<<"amq.headers">>,  headers}, %% per 0-9-1 xml
                                   {<<"amq.fanout">>, fanout}]],
                          ok;
                      [_] ->
                          mnesia:abort({vhost_already_exists, VHostPath})
                  end
          end),
    rabbit_log:info("Added vhost ~p~n", [VHostPath]),
    R.

delete_vhost(VHostPath) ->
    %%FIXME: We are forced to delete the queues outside the TX below
    %%because queue deletion involves sending messages to the queue
    %%process, which in turn results in further mnesia actions and
    %%eventually the termination of that process.
    lists:foreach(fun (Q) ->
                          {ok,_} = rabbit_amqqueue:delete(Q, false, false)
                  end,
                  rabbit_amqqueue:list(VHostPath)),
    R = rabbit_misc:execute_mnesia_transaction(
          rabbit_misc:with_vhost(
            VHostPath,
            fun () ->
                    ok = internal_delete_vhost(VHostPath)
            end)),
    rabbit_log:info("Deleted vhost ~p~n", [VHostPath]),
    R.

internal_delete_vhost(VHostPath) ->
    lists:foreach(fun (#exchange{name = Name}) ->
                          ok = rabbit_exchange:delete(Name, false)
                  end,
                  rabbit_exchange:list(VHostPath)),
    lists:foreach(fun ({Username, _, _, _}) ->
                          ok = clear_permissions(Username, VHostPath)
                  end,
                  list_vhost_permissions(VHostPath)),
    ok = mnesia:delete({rabbit_vhost, VHostPath}),
    ok.

vhost_exists(VHostPath) ->
    mnesia:dirty_read({rabbit_vhost, VHostPath}) /= [].

list_vhosts() ->
    mnesia:dirty_all_keys(rabbit_vhost).

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
            list_permissions(rabbit_misc:with_vhost(
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
