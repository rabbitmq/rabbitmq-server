%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_access_control).

-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("kernel/include/logger.hrl").

-export([ensure_auth_backends_are_enabled/0]).
-export([check_user_pass_login/2, check_user_login/2, check_user_login/3, check_user_loopback/2,
         check_vhost_access/4, check_resource_access/4, check_topic_access/4,
         check_user_id/2]).

-export([permission_cache_can_expire/1, update_state/2, expiry_timestamp/1]).

%%----------------------------------------------------------------------------

-spec ensure_auth_backends_are_enabled() -> Ret when
      Ret :: ok | {error, Reason},
      Reason :: string().

ensure_auth_backends_are_enabled() ->
    {ok, AuthBackends} = application:get_env(rabbit, auth_backends),
    ValidAuthBackends = filter_valid_auth_backend_configuration(
                          AuthBackends, []),
    case ValidAuthBackends of
        AuthBackends ->
            ok;
        [_ | _] ->
            %% Some auth backend modules were filtered out because their
            %% corresponding plugin is either unavailable or disabled. We
            %% update the application environment variable so that
            %% authentication and authorization do not try to use them.
            ?LOG_WARNING(
               "Some configured backends were dropped because their "
               "corresponding plugins are disabled. Please look at the "
               "info messages above to learn which plugin(s) should be "
               "enabled. Here is the list of auth backends kept after "
               "filering:~n~p", [ValidAuthBackends]),
            ok = application:set_env(rabbit, auth_backends, ValidAuthBackends),
            ok;
        [] ->
            %% None of the auth backend modules are usable. Log an error and
            %% abort the boot of RabbitMQ.
            ?LOG_ERROR(
               "None of the configured auth backends are usable because "
               "their corresponding plugins were not enabled. Please look "
               "at the info messages above to learn which plugin(s) should "
               "be enabled."),
            {error,
             "Authentication/authorization backends require plugins to be "
             "enabled; see logs for details"}
    end.

filter_valid_auth_backend_configuration(
  [Mod | Rest], ValidAuthBackends)
  when is_atom(Mod) ->
    case is_auth_backend_module_enabled(Mod) of
        true ->
            ValidAuthBackends1 = [Mod | ValidAuthBackends],
            filter_valid_auth_backend_configuration(Rest, ValidAuthBackends1);
        false ->
            filter_valid_auth_backend_configuration(Rest, ValidAuthBackends)
    end;
filter_valid_auth_backend_configuration(
  [{ModN, ModZ} = Mod | Rest], ValidAuthBackends)
  when is_atom(ModN) andalso is_atom(ModZ) ->
    %% Both auth backend modules must be usable to keep the entire pair.
    IsModNEnabled = is_auth_backend_module_enabled(ModN),
    IsModZEnabled = is_auth_backend_module_enabled(ModZ),
    case IsModNEnabled andalso IsModZEnabled of
        true ->
            ValidAuthBackends1 = [Mod | ValidAuthBackends],
            filter_valid_auth_backend_configuration(Rest, ValidAuthBackends1);
        false ->
            filter_valid_auth_backend_configuration(Rest, ValidAuthBackends)
    end;
filter_valid_auth_backend_configuration(
  [{ModN, ModZs} | Rest], ValidAuthBackends)
  when is_atom(ModN) andalso is_list(ModZs) ->
    %% The authentication backend module and at least on of the authorization
    %% backend module must be usable to keep the entire pair.
    %%
    %% The list of authorization backend modules may be shorter than the
    %% configured one after the filtering.
    IsModNEnabled = is_auth_backend_module_enabled(ModN),
    EnabledModZs = lists:filter(fun is_auth_backend_module_enabled/1, ModZs),
    case IsModNEnabled andalso EnabledModZs =/= [] of
        true ->
            Mod1 = {ModN, EnabledModZs},
            ValidAuthBackends1 = [Mod1 | ValidAuthBackends],
            filter_valid_auth_backend_configuration(Rest, ValidAuthBackends1);
        false ->
            filter_valid_auth_backend_configuration(Rest, ValidAuthBackends)
    end;
filter_valid_auth_backend_configuration([], ValidAuthBackends) ->
    lists:reverse(ValidAuthBackends).

is_auth_backend_module_enabled(Mod) when is_atom(Mod) ->
    %% We check if the module is provided by the core of RabbitMQ or a plugin,
    %% and if that plugin is enabled.
    {ok, Modules} = application:get_key(rabbit, modules),
    case lists:member(Mod, Modules) of
        true ->
            true;
        false ->
            %% The module is not provided by RabbitMQ core. Let's query
            %% plugins then.
            case rabbit_plugins:which_plugin(Mod) of
                {ok, PluginName} ->
                    %% FIXME: The definition of an "enabled plugin" in
                    %% `rabbit_plugins' varies from funtion to function.
                    %% Sometimes, it means the "rabbitmq-plugin enable
                    %% <plugin>" was executed, sometimes it means the plugin
                    %% is running.
                    %%
                    %% This function is a boot step and is executed before
                    %% plugin are started. Therefore, we can't rely on
                    %% `rabbit_plugins:is_enabled/1' because it uses the
                    %% latter definition of "the plugin is running, regardless
                    %% of if it is enabled or not".
                    %%
                    %% Therefore, we use `rabbit_plugins:enabled_plugins/0'
                    %% which lists explicitly enabled plugins. Unfortunately,
                    %% it won't include the implicitly enabled plugins (i.e,
                    %% plugins that are dependencies of explicitly enabled
                    %% plugins).
                    EnabledPlugins = rabbit_plugins:enabled_plugins(),
                    case lists:member(PluginName, EnabledPlugins) of
                        true ->
                            true;
                        false ->
                            ?LOG_INFO(
                               "The `~ts` auth backend module is configured. "
                               "However, the `~ts` plugin must be enabled in "
                               "order to use this auth backend. Until then "
                               "it will be skipped during "
                               "authentication/authorization",
                               [Mod, PluginName]),
                            false
                    end;
                {error, no_provider} ->
                    ?LOG_INFO(
                       "The `~ts` auth backend module is configured. "
                       "However, no plugins available provide this "
                       "module. Until then it will be skipped during "
                       "authentication/authorization",
                       [Mod]),
                    false
            end
    end.

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
    check_user_login(Username, AuthProps, Modules).

-spec check_user_login
        (rabbit_types:username(), [{atom(), any()}], term()) ->
            {'ok', rabbit_types:user()} |
            {'refused', rabbit_types:username(), string(), [any()]}.

check_user_login(Username, AuthProps, Modules) ->
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
                            ?LOG_DEBUG("User '~ts' authenticated successfully by backend ~ts", [Username2, Mod]),
                            user(ModNUser, {ok, [{Mod, Impl}], []});
                        Else ->
                            ?LOG_DEBUG("User '~ts' failed authentication by backend ~ts", [Username, Mod]),
                            Else
                    end;
                (_, {ok, User}) ->
                    %% We've successfully authenticated. Skip to the end...
                    {ok, User}
            end,
            {refused, Username, "No modules checked '~ts'", [Username]}, Modules)
        catch
            Type:Error:Stacktrace ->
                ?LOG_DEBUG("User '~ts' authentication failed with ~ts:~tp:~n~tp", [Username, Type, Error, Stacktrace]),
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
            ?LOG_DEBUG("User '~ts' authenticated successfully by backend ~ts", [Username2, ModN]),
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
            ?LOG_ERROR(FullErrStr, FullErrArgs),
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
    {'ok', rabbit_types:user()} |
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
