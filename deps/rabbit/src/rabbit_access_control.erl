%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_access_control).

-include_lib("rabbit_common/include/rabbit.hrl").

-export([check_user_pass_login/2, check_user_login/2, check_user_loopback/2,
         check_vhost_access/4, check_resource_access/4, check_topic_access/4,
         check_user_id/2]).

-export([permission_cache_can_expire/1, update_state/2, expiry_timestamp/1]).

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
                            rabbit_log:debug("User '~ts' authenticated successfully by backend ~ts", [Username2, Mod]),
                            user(ModNUser, {ok, [{Mod, Impl}], []});
                        Else ->
                            rabbit_log:debug("User '~ts' failed authentication by backend ~ts", [Username, Mod]),
                            Else
                    end;
                (_, {ok, User}) ->
                    %% We've successfully authenticated. Skip to the end...
                    {ok, User}
            end,
            {refused, Username, "No modules checked '~ts'", [Username]}, Modules)
        catch
            Type:Error:Stacktrace ->
                rabbit_log:debug("User '~ts' authentication failed with ~ts:~tp:~n~tp", [Username, Type, Error, Stacktrace]),
                {refused, Username, "User '~ts' authentication failed with internal error. "
                                    "Enable debug logs to see the real error.", [Username]}

        end.

try_authenticate_and_try_authorize(ModN, ModZs0, Username, AuthProps) ->
    ModZs = case ModZs0 of
                A when is_atom(A) -> [A];
                L when is_list(L) -> L
            end,
    case try_authenticate(ModN, Username, AuthProps) of
        {ok, ModNUser = #auth_user{username = Username2}} ->
            rabbit_log:debug("User '~ts' authenticated successfully by backend ~ts", [Username2, ModN]),
            user(ModNUser, try_authorize(ModZs, Username2, AuthProps));
        Else ->
            Else
    end.

try_authenticate(Module, Username, AuthProps) ->
    case Module:user_login_authentication(Username, AuthProps) of
        {ok, AuthUser}  -> {ok, AuthUser};
        {error, E}      -> {refused, Username,
                            "~ts failed authenticating ~ts: ~tp",
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
                                        "~ts failed authorizing ~ts: ~tp",
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
                Mod, "access to vhost '~ts' refused for user '~ts'",
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
        (rabbit_types:user(), rabbit_types:r(atom()), rabbit_types:permission_atom(), rabbit_types:authz_context()) ->
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
                Module, "~s access to ~ts refused for user '~ts'",
                [Permission, rabbit_misc:rs(Resource), Username]);
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
                Module, "~s access to topic '~ts' in exchange ~ts refused for user '~ts'",
                [Permission, maps:get(routing_key, Context), rabbit_misc:rs(Resource), Username]);
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
            FullErrStr = ErrStr ++ ", backend ~ts returned an error: ~tp",
            FullErrArgs = ErrArgs ++ [Module, E],
            rabbit_log:error(FullErrStr, FullErrArgs),
            rabbit_misc:protocol_error(ErrName, FullErrStr, FullErrArgs)
    end.

-spec check_user_id(mc:state(), rabbit_types:user()) ->
    ok | {refused, string(), [term()]}.
check_user_id(Message, ActualUser) ->
    case mc:user_id(Message) of
        undefined ->
            ok;
        {binary, ClaimedUserName} ->
            check_user_id0(ClaimedUserName, ActualUser)
    end.

check_user_id0(Username, #user{username = Username}) ->
    ok;
check_user_id0(_, #user{authz_backends = [{rabbit_auth_backend_dummy, _}]}) ->
    ok;
check_user_id0(ClaimedUserName, #user{username = ActualUserName,
                                      tags = Tags}) ->
    case lists:member(impersonator, Tags) of
        true ->
            ok;
        false ->
            {refused,
             "user_id property set to '~ts' but authenticated user was '~ts'",
             [ClaimedUserName, ActualUserName]}
    end.

-spec update_state(User :: rabbit_types:user(), NewState :: term()) ->
<<<<<<< HEAD
    {'ok', rabbit_types:auth_user()} |
=======
    {'ok', rabbit_types:user()} |
>>>>>>> 8d7535e0b (amqqueue_process: adopt new `is_duplicate` backing queue callback)
    {'refused', string()} |
    {'error', any()}.

update_state(User = #user{authz_backends = Backends0}, NewState) ->
    %% N.B.: we use foldl/3 and prepending, so the final list of
    %% backends is in reverse order from the original list.
    Backends = lists:foldl(
                fun({Module, Impl}, {ok, Acc}) ->
                        AuthUser = auth_user(User, Impl),
                        case Module:expiry_timestamp(AuthUser) of
                          never ->
                            {ok, [{Module, Impl} | Acc]};
                          _  ->
                            case Module:update_state(AuthUser, NewState) of
                              {ok, #auth_user{impl = Impl1}} ->
                                {ok, [{Module, Impl1} | Acc]};
                              Else -> Else
                            end
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
permission_cache_can_expire(User) ->
    expiry_timestamp(User) =/= never.

-spec expiry_timestamp(User :: rabbit_types:user()) -> integer() | never.
expiry_timestamp(User = #user{authz_backends = Modules}) ->
    lists:foldl(fun({Module, Impl}, Ts0) ->
                        case Module:expiry_timestamp(auth_user(User, Impl)) of
                            Ts1 when is_integer(Ts0) andalso is_integer(Ts1)
                                     andalso Ts1 > Ts0 ->
                                Ts0;
                            Ts1 when is_integer(Ts1) ->
                                Ts1;
                            _ ->
                                Ts0
                        end
                end, never, Modules).
