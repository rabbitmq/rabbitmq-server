%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_access_control).

-include_lib("rabbit_common/include/rabbit.hrl").

-export([check_user_pass_login/2, check_user_login/2, check_user_loopback/2,
         check_vhost_access/4, check_resource_access/4, check_topic_access/4]).

-export([permission_cache_can_expire/1, update_state/2]).

%%----------------------------------------------------------------------------

-export_type([permission_atom/0]).

-type permission_atom() :: 'configure' | 'read' | 'write'.

%%----------------------------------------------------------------------------

-spec check_user_pass_login
        (rabbit_types:username(), rabbit_types:password()) ->
            {'ok', rabbit_types:user()} |
            {'refused', rabbit_types:username(), string(), [any()]}.

check_user_pass_login(Username, Password) ->
    check_user_login(Username, [{password, Password}]).

-spec check_user_login
        (rabbit_types:username(), [{atom(), any()}]) ->
            {'ok', rabbit_types:user()} |
            {'refused', rabbit_types:username(), string(), [any()]}.

check_user_login(Username, AuthProps) ->
    %% extra auth properties like MQTT client id are in AuthProps
    {ok, Modules} = application:get_env(rabbit, auth_backends),
    try 
        lists:foldl(
            fun (rabbit_auth_backend_cache=ModN, {refused, _, _, _}) ->
                    %% It is possible to specify authn/authz within the cache module settings,
                    %% so we have to do both auth steps here
                    %% See this rabbitmq-users discussion:
                    %% https://groups.google.com/d/topic/rabbitmq-users/ObqM7MQdA3I/discussion
                    try_authenticate_and_try_authorize(ModN, ModN, Username, AuthProps);
                ({ModN, ModZs}, {refused, _, _, _}) ->
                    %% Different modules for authN vs authZ. So authenticate
                    %% with authN module, then if that succeeds do
                    %% passwordless (i.e pre-authenticated) login with authZ.
                    try_authenticate_and_try_authorize(ModN, ModZs, Username, AuthProps);
                (Mod, {refused, _, _, _}) ->
                    %% Same module for authN and authZ. Just take the result
                    %% it gives us
                    case try_authenticate(Mod, Username, AuthProps) of
                        {ok, ModNUser = #auth_user{username = Username2, impl = Impl}} ->
                            rabbit_log:debug("User '~s' authenticated successfully by backend ~s", [Username2, Mod]),
                            user(ModNUser, {ok, [{Mod, Impl}], []});
                        Else ->
                            rabbit_log:debug("User '~s' failed authenticatation by backend ~s", [Username, Mod]),
                            Else
                    end;
                (_, {ok, User}) ->
                    %% We've successfully authenticated. Skip to the end...
                    {ok, User}
            end,
            {refused, Username, "No modules checked '~s'", [Username]}, Modules)
        catch 
            Type:Error:Stacktrace -> 
                rabbit_log:debug("User '~s' authentication failed with ~s:~p:~n~p", [Username, Type, Error, Stacktrace]),
                {refused, Username, "User '~s' authentication failed with internal error. "
                                    "Enable debug logs to see the real error.", [Username]}

        end.

try_authenticate_and_try_authorize(ModN, ModZs0, Username, AuthProps) ->
    ModZs = case ModZs0 of
                A when is_atom(A) -> [A];
                L when is_list(L) -> L
            end,
    case try_authenticate(ModN, Username, AuthProps) of
        {ok, ModNUser = #auth_user{username = Username2}} ->
            rabbit_log:debug("User '~s' authenticated successfully by backend ~s", [Username2, ModN]),
            user(ModNUser, try_authorize(ModZs, Username2, AuthProps));
        Else ->
            Else
    end.

try_authenticate(Module, Username, AuthProps) ->
    case Module:user_login_authentication(Username, AuthProps) of
        {ok, AuthUser}  -> {ok, AuthUser};
        {error, E}      -> {refused, Username,
                            "~s failed authenticating ~s: ~p",
                            [Module, Username, E]};
        {refused, F, A} -> {refused, Username, F, A}
    end.

try_authorize(Modules, Username, AuthProps) ->
    lists:foldr(
      fun (Module, {ok, ModsImpls, ModsTags}) ->
              case Module:user_login_authorization(Username, AuthProps) of
                  {ok, Impl, Tags}-> {ok, [{Module, Impl} | ModsImpls], ModsTags ++ Tags};
                  {ok, Impl}      -> {ok, [{Module, Impl} | ModsImpls], ModsTags};
                  {error, E}      -> {refused, Username,
                                        "~s failed authorizing ~s: ~p",
                                        [Module, Username, E]};
                  {refused, F, A} -> {refused, Username, F, A}
              end;
          (_,      {refused, F, A}) ->
              {refused, Username, F, A}
      end, {ok, [], []}, Modules).

user(#auth_user{username = Username, tags = Tags}, {ok, ModZImpls, ModZTags}) ->
    {ok, #user{username       = Username,
               tags           = Tags ++ ModZTags,
               authz_backends = ModZImpls}};
user(_AuthUser, Error) ->
    Error.

auth_user(#user{username = Username, tags = Tags}, Impl) ->
    #auth_user{username = Username,
               tags     = Tags,
               impl     = Impl}.

-spec check_user_loopback
        (rabbit_types:username(), rabbit_net:socket() | inet:ip_address()) ->
            'ok' | 'not_allowed'.

check_user_loopback(Username, SockOrAddr) ->
    {ok, Users} = application:get_env(rabbit, loopback_users),
    case rabbit_net:is_loopback(SockOrAddr)
        orelse not lists:member(Username, Users) of
        true  -> ok;
        false -> not_allowed
    end.

get_authz_data_from({ip, Address}) ->
    #{peeraddr => Address};
get_authz_data_from({socket, Sock}) ->
    {ok, {Address, _Port}} = rabbit_net:peername(Sock),
    #{peeraddr => Address};
get_authz_data_from(undefined) ->
    undefined.

% Note: ip can be either a tuple or, a binary if reverse_dns_lookups
% is enabled and it's a direct connection.
-spec check_vhost_access(User :: rabbit_types:user(),
                         VHostPath :: rabbit_types:vhost(),
                         AuthzRawData :: {socket, rabbit_net:socket()} | {ip, inet:ip_address() | binary()} | undefined,
                         AuthzContext :: map()) ->
    'ok' | rabbit_types:channel_exit().
check_vhost_access(User = #user{username       = Username,
                                authz_backends = Modules}, VHostPath, AuthzRawData, AuthzContext) ->
    AuthzData = get_authz_data_from(AuthzRawData),
    FullAuthzContext = create_vhost_access_authz_data(AuthzData, AuthzContext),
    lists:foldl(
      fun({Mod, Impl}, ok) ->
              check_access(
                fun() ->
                        rabbit_vhost:exists(VHostPath) andalso
                            Mod:check_vhost_access(
                              auth_user(User, Impl), VHostPath, FullAuthzContext)
                end,
                Mod, "access to vhost '~s' refused for user '~s'",
                [VHostPath, Username], not_allowed);
         (_, Else) ->
              Else
      end, ok, Modules).

create_vhost_access_authz_data(undefined, Context) when map_size(Context) == 0 ->
    undefined;
create_vhost_access_authz_data(undefined, Context) ->
    Context;
create_vhost_access_authz_data(PeerAddr, Context) when map_size(Context) == 0 ->
    PeerAddr;
create_vhost_access_authz_data(PeerAddr, Context) ->
    maps:merge(PeerAddr, Context).

-spec check_resource_access
        (rabbit_types:user(), rabbit_types:r(atom()), permission_atom(), rabbit_types:authz_context()) ->
            'ok' | rabbit_types:channel_exit().

check_resource_access(User, R = #resource{kind = exchange, name = <<"">>},
                      Permission, Context) ->
    check_resource_access(User, R#resource{name = <<"amq.default">>},
                          Permission, Context);
check_resource_access(User = #user{username       = Username,
                                   authz_backends = Modules},
                      Resource, Permission, Context) ->
    lists:foldl(
      fun({Module, Impl}, ok) ->
              check_access(
                fun() -> Module:check_resource_access(
                           auth_user(User, Impl), Resource, Permission, Context) end,
                Module, "access to ~s refused for user '~s'",
                [rabbit_misc:rs(Resource), Username]);
         (_, Else) -> Else
      end, ok, Modules).

check_topic_access(User = #user{username = Username,
                                authz_backends = Modules},
                            Resource, Permission, Context) ->
    lists:foldl(
        fun({Module, Impl}, ok) ->
            check_access(
                fun() -> Module:check_topic_access(
                    auth_user(User, Impl), Resource, Permission, Context) end,
                Module, "access to topic '~s' in exchange ~s refused for user '~s'",
                [maps:get(routing_key, Context), rabbit_misc:rs(Resource), Username]);
            (_, Else) -> Else
        end, ok, Modules).

check_access(Fun, Module, ErrStr, ErrArgs) ->
    check_access(Fun, Module, ErrStr, ErrArgs, access_refused).

check_access(Fun, Module, ErrStr, ErrArgs, ErrName) ->
    case Fun() of
        true ->
            ok;
        false ->
            rabbit_misc:protocol_error(ErrName, ErrStr, ErrArgs);
        {error, E}  ->
            FullErrStr = ErrStr ++ ", backend ~s returned an error: ~p",
            FullErrArgs = ErrArgs ++ [Module, E],
            rabbit_log:error(FullErrStr, FullErrArgs),
            rabbit_misc:protocol_error(ErrName, FullErrStr, FullErrArgs)
    end.

-spec update_state(User :: rabbit_types:user(), NewState :: term()) ->
    {'ok', rabbit_types:auth_user()} |
    {'refused', string()} |
    {'error', any()}.

update_state(User = #user{authz_backends = Backends0}, NewState) ->
    %% N.B.: we use foldl/3 and prepending, so the final list of
    %% backends is in reverse order from the original list.
    Backends = lists:foldl(
                fun({Module, Impl}, {ok, Acc}) ->
                        case Module:state_can_expire() of
                          true  ->
                            case Module:update_state(auth_user(User, Impl), NewState) of
                              {ok, #auth_user{impl = Impl1}} ->
                                {ok, [{Module, Impl1} | Acc]};
                              Else -> Else
                            end;
                          false ->
                            {ok, [{Module, Impl} | Acc]}
                        end;
                   (_, {error, _} = Err)      -> Err;
                   (_, {refused, _, _} = Err) -> Err
                end, {ok, []}, Backends0),
    case Backends of
      {ok, Pairs} -> {ok, User#user{authz_backends = lists:reverse(Pairs)}};
      Else        -> Else
    end.

-spec permission_cache_can_expire(User :: rabbit_types:user()) -> boolean().

%% Returns true if any of the backends support credential expiration,
%% otherwise returns false.
permission_cache_can_expire(#user{authz_backends = Backends}) ->
    lists:any(fun ({Module, _State}) -> Module:state_can_expire() end, Backends).
