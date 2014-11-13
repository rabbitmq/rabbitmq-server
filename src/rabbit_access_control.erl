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
          fun ({ModN, ModZs0}, {refused, _, _}) ->
                  ModZs = case ModZs0 of
                              A when is_atom(A) -> [A];
                              L when is_list(L) -> L
                          end,
                  %% Different modules for authN vs authZ. So authenticate
                  %% with authN module, then if that succeeds do
                  %% passwordless (i.e pre-authenticated) login with authZ.
                  case try_authenticate(ModN, Username, AuthProps) of
                      {ok, ModNUser = #auth_user{username = Username2}} ->
                          user(ModNUser, try_authorize(ModZs, Username2));
                      Else ->
                          Else
                  end;
              (Mod, {refused, _, _}) ->
                  %% Same module for authN and authZ. Just take the result
                  %% it gives us
                  case try_authenticate(Mod, Username, AuthProps) of
                      {ok, ModNUser} -> user(ModNUser, {ok, [{Mod, ModNUser}]});
                      Else           -> Else
                  end;
              (_, {ok, User}) ->
                  %% We've successfully authenticated. Skip to the end...
                  {ok, User}
          end, {refused, "No modules checked '~s'", [Username]}, Modules),
    rabbit_event:notify(case R of
                            {ok, _User} -> user_authentication_success;
                            _           -> user_authentication_failure
                        end, [{name, Username}]),
    R.

try_authenticate(Module, Username, AuthProps) ->
    case Module:check_user_login(Username, AuthProps) of
        {ok, AuthUser}  -> {ok, AuthUser};
        {error, E}      -> {refused, "~s failed authenticating ~s: ~p~n",
                            [Module, Username, E]};
        {refused, F, A} -> {refused, F, A}
    end.

try_authorize(Modules, Username) ->
    lists:foldr(
      fun (Module, {ok, AUsers}) ->
              case Module:check_user_login(Username, []) of
                  {ok, AUser}     -> {ok, [{Module, AUser} | AUsers]};
                  {error, E}      -> {refused, "~s failed authorizing ~s: ~p~n",
                                        [Module, Username, E]};
                  {refused, F, A} -> {refused, F, A}
              end;
          (_,      {refused, _, _} = Error) ->
              Error
      end, {ok, []}, Modules).

user(#auth_user{username = Username, tags = Tags}, {ok, ModZUsers}) ->
    {ok, #user{username       = Username,
               tags           = Tags,
               authz_backends = ModZUsers}};
user(_AuthUser, Error) ->
    Error.

check_user_loopback(Username, SockOrAddr) ->
    {ok, Users} = application:get_env(rabbit, loopback_users),
    case rabbit_net:is_loopback(SockOrAddr)
        orelse not lists:member(Username, Users) of
        true  -> ok;
        false -> not_allowed
    end.

check_vhost_access(#user{username       = Username,
                         authz_backends = Modules}, VHostPath, Sock) ->
    lists:foldl(
      fun({Mod, AUser}, ok) ->
              check_access(
                fun() ->
                        rabbit_vhost:exists(VHostPath) andalso
                            Mod:check_vhost_access(AUser, VHostPath, Sock)
                end,
                Mod, "access to vhost '~s' refused for user '~s'",
                [VHostPath, Username]);
         (_, Else) ->
              Else
      end, ok, Modules).

check_resource_access(User, R = #resource{kind = exchange, name = <<"">>},
                      Permission) ->
    check_resource_access(User, R#resource{name = <<"amq.default">>},
                          Permission);
check_resource_access(#user{username       = Username,
                            authz_backends = Modules},
                      Resource, Permission) ->
    lists:foldl(
      fun({Module, AUser}, ok) ->
              check_access(
                fun() -> Module:check_resource_access(
                           AUser, Resource, Permission) end,
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
