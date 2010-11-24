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

-module(rabbit_auth_backend_internal).
-include("rabbit.hrl").

-behaviour(rabbit_auth_backend).

-export([description/0]).
-export([check_user_login/2, check_vhost_access/3, check_resource_access/3]).

-include("rabbit_auth_backend_spec.hrl").

%% Our internal user database

description() ->
    [{name, <<"Internal">>},
     {description, <<"Internal user / password database">>}].

check_user_login(Username, []) ->
    internal_check_user_login(Username, fun() -> true end);
check_user_login(Username, [{password, Password}]) ->
    internal_check_user_login(
      Username,
      fun(#internal_user{password_hash = Hash}) ->
              rabbit_access_control:check_password(Password, Hash)
      end);
check_user_login(Username, AuthProps) ->
    exit({unknown_auth_props, Username, AuthProps}).

internal_check_user_login(Username, Fun) ->
    case rabbit_access_control:lookup_user(Username) of
        {ok, User = #internal_user{is_admin = IsAdmin}} ->
            case Fun(User) of
                true -> {ok, #user{username     = Username,
                                   is_admin     = IsAdmin,
                                   auth_backend = ?MODULE,
                                   impl         = User}};
                _    -> {refused, Username}
            end;
        {error, not_found} ->
            {refused, Username}
    end.

check_vhost_access(#user{is_admin = true},    _VHostPath, read) ->
    true;

check_vhost_access(#user{username = Username}, VHostPath, write) ->
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
    end.

permission_index(configure) -> #permission.configure;
permission_index(write)     -> #permission.write;
permission_index(read)      -> #permission.read.
