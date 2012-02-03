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
%% Copyright (c) 2007-2012 VMware, Inc.  All rights reserved.
%%

-module(rabbit_access_control).

-include("rabbit.hrl").

-export([check_user_pass_login/2, check_user_login/2,
         check_vhost_access/2, check_resource_access/3]).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-export_type([permission_atom/0]).

-type(permission_atom() :: 'configure' | 'read' | 'write').

-spec(check_user_pass_login/2 ::
        (rabbit_types:username(), rabbit_types:password())
        -> {'ok', rabbit_types:user()} | {'refused', string(), [any()]}).
-spec(check_user_login/2 ::
        (rabbit_types:username(), [{atom(), any()}])
        -> {'ok', rabbit_types:user()} | {'refused', string(), [any()]}).
-spec(check_vhost_access/2 ::
        (rabbit_types:user(), rabbit_types:vhost())
        -> 'ok' | rabbit_types:channel_exit()).
-spec(check_resource_access/3 ::
        (rabbit_types:user(), rabbit_types:r(atom()), permission_atom())
        -> 'ok' | rabbit_types:channel_exit()).

-endif.

%%----------------------------------------------------------------------------

check_user_pass_login(Username, Password) ->
    check_user_login(Username, [{password, Password}]).

check_user_login(Username, AuthProps) ->
    {ok, Modules} = application:get_env(rabbit, auth_backends),
    lists:foldl(
      fun(Module, {refused, _, _}) ->
              case Module:check_user_login(Username, AuthProps) of
                  {error, E} ->
                      {refused, "~s failed authenticating ~s: ~p~n",
                       [Module, Username, E]};
                  Else ->
                      Else
              end;
         (_, {ok, User}) ->
              {ok, User}
      end, {refused, "No modules checked '~s'", [Username]}, Modules).

check_vhost_access(User = #user{ username     = Username,
                                 auth_backend = Module }, VHostPath) ->
    check_access(
      fun() ->
              rabbit_vhost:exists(VHostPath) andalso
                  Module:check_vhost_access(User, VHostPath)
      end,
      "~s failed checking vhost access to ~s for ~s: ~p~n",
      [Module, VHostPath, Username],
      "access to vhost '~s' refused for user '~s'",
      [VHostPath, Username]).

check_resource_access(User, R = #resource{kind = exchange, name = <<"">>},
                      Permission) ->
    check_resource_access(User, R#resource{name = <<"amq.default">>},
                          Permission);
check_resource_access(User = #user{username = Username, auth_backend = Module},
                      Resource, Permission) ->
    check_access(
      fun() -> Module:check_resource_access(User, Resource, Permission) end,
      "~s failed checking resource access to ~p for ~s: ~p~n",
      [Module, Resource, Username],
      "access to ~s refused for user '~s'",
      [rabbit_misc:rs(Resource), Username]).

check_access(Fun, ErrStr, ErrArgs, RefStr, RefArgs) ->
    Allow = case Fun() of
                {error, _} = E ->
                    rabbit_log:error(ErrStr, ErrArgs ++ [E]),
                    false;
                Else ->
                    Else
            end,
    case Allow of
        true ->
            ok;
        false ->
            rabbit_misc:protocol_error(access_refused, RefStr, RefArgs)
    end.
