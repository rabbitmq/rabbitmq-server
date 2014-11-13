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

-module(rabbit_access_control).

-include("rabbit.hrl").

-export([check_user_pass_login/2, check_user_login/2, check_user_loopback/2,
         check_vhost_access/3, check_resource_access/3]).

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
-spec(check_user_loopback/2 :: (rabbit_types:username(),
                                rabbit_net:socket() | inet:ip_address())
        -> 'ok' | 'not_allowed').
-spec(check_vhost_access/3 ::
        (rabbit_types:user(), rabbit_types:vhost(), rabbit_net:socket())
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
    R = lists:foldl(
          fun ({ModN, ModZ}, {refused, _, _}) ->
                  %% Different modules for authN vs authZ. So authenticate
                  %% with authN module, then if that succeeds do
                  %% passwordless (i.e pre-authenticated) login with authZ.
                  case try_authenticate(ModN, Username, AuthProps) of
                      {ok, User, _AuthZ} -> try_authorize(ModZ, User, []);
                      Else    -> Else
                  end;
              (Mod, {refused, _, _}) ->
                  %% Same module for authN and authZ. Just take the result
                  %% it gives us
                  try_authenticate(Mod, Username, AuthProps);
              (_, {ok, User, AuthZ}) ->
                  %% We've successfully authenticated. Skip to the end...
                  {ok, User, AuthZ}
          end, {refused, "No modules checked '~s'", [Username]}, Modules),

    case R of
        {ok, RUser, RAuthZ} ->
            rabbit_event:notify(user_authentication_success, [{name, Username}]),
            %% Store the list of authorization backends
            {ok, RUser#user{authZ_backends=RAuthZ}};
        _ ->
            rabbit_event:notify(user_authentication_failure, [{name, Username}]),
            R
    end.

try_authenticate(Module, Username, AuthProps) ->
    case Module:check_user_login(Username, AuthProps) of
        {ok, User, AuthZ} -> {ok, User, [{Module, AuthZ}]};
        {error, E} -> {refused, "~s failed authenticating ~s: ~p~n",
                       [Module, Username, E]};
        Else       -> Else
    end.

try_authorize(Modules, User, AuthZList) when is_list(Modules) ->
    lists:foldr(
        fun (Module, {ok, _User, AuthZList2}) -> try_authorize(Module, User, AuthZList2);
            (_, {refused, _, _} = Error) -> Error
    end, {ok, User, AuthZList}, Modules);

try_authorize(Module, User = #user{username = Username}, AuthZList) ->
    case Module:check_user_login(Username, []) of
        {ok, _User, AuthZ} -> {ok, User, [{Module, AuthZ}|AuthZList]};
        {error, E} -> {refused, "~s failed authorizing ~s: ~p~n",
                       [Module, Username, E]};
        Else       -> Else
    end.

check_user_loopback(Username, SockOrAddr) ->
    {ok, Users} = application:get_env(rabbit, loopback_users),
    case rabbit_net:is_loopback(SockOrAddr)
        orelse not lists:member(Username, Users) of
        true  -> ok;
        false -> not_allowed
    end.

check_vhost_access(User = #user{ username = Username,
                                 authZ_backends = Modules }, VHostPath, Sock) ->
    lists:foldl(
      fun({Module, Impl}, ok) ->
          check_access(
            fun() ->
                %% TODO this could be an andalso shortcut under >R13A
                case rabbit_vhost:exists(VHostPath) of
                    false -> false;
                    true  -> Module:check_vhost_access(User, Impl, VHostPath, Sock)
                end
            end,
            Module, "access to vhost '~s' refused for user '~s'",
            [VHostPath, Username]);

         (_, Else) -> Else
      end, ok, Modules).

check_resource_access(User, R = #resource{kind = exchange, name = <<"">>},
                      Permission) ->
    check_resource_access(User, R#resource{name = <<"amq.default">>},
                          Permission);
check_resource_access(User = #user{username = Username, authZ_backends = Modules},
                      Resource, Permission) ->
    lists:foldl(
      fun({Module, Impl}, ok) ->
          check_access(
            fun() -> Module:check_resource_access(User, Impl, Resource, Permission) end,
            Module, "access to ~s refused for user '~s'",
            [rabbit_misc:rs(Resource), Username]);

         (_, Else) -> Else
      end, ok, Modules).

check_access(Fun, Module, ErrStr, ErrArgs) ->
    Allow = case Fun() of
                {error, E}  ->
                    rabbit_log:error(ErrStr ++ " by ~s: ~p~n",
                                     ErrArgs ++ [Module, E]),
                    false;
                Else ->
                    Else
            end,
    case Allow of
        true ->
            ok;
        false ->
            rabbit_misc:protocol_error(access_refused, ErrStr, ErrArgs)
    end.
