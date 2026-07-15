%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_federation_parameters).
-behaviour(rabbit_runtime_parameter).
-behaviour(rabbit_policy_validator).

-include("rabbit_federation.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-export([validate/5, notify/5, notify_clear/4]).
-export([register/0, unregister/0, validate_policy/1, adjust/1]).

-define(RUNTIME_PARAMETERS,
        [{runtime_parameter, <<"federation">>},
         {runtime_parameter, <<"federation-upstream">>},
         {runtime_parameter, <<"federation-upstream-set">>},
         {policy_validator,  <<"federation-upstream">>},
         {policy_validator,  <<"federation-upstream-pattern">>},
         {policy_validator,  <<"federation-upstream-set">>}]).

-rabbit_boot_step({?MODULE,
                   [{description, "federation parameters"},
                    {mfa, {rabbit_federation_parameters, register, []}},
                    {requires, rabbit_registry},
                    {cleanup, {rabbit_federation_parameters, unregister, []}},
                    {enables, recovery}]}).

register() ->
    [rabbit_registry:register(Class, Name, ?MODULE) ||
        {Class, Name} <- ?RUNTIME_PARAMETERS],
    ok.

unregister() ->
    [rabbit_registry:unregister(Class, Name) ||
        {Class, Name} <- ?RUNTIME_PARAMETERS],
    ok.

validate(_VHost, <<"federation-upstream-set">>, Name, Term0, _User) ->
    Term = [rabbit_data_coercion:to_proplist(Upstream) || Upstream <- Term0],
    [rabbit_parameter_validation:proplist(
       Name,
       [{<<"upstream">>, fun rabbit_parameter_validation:binary/2, mandatory} |
        shared_validation()], Upstream)
     || Upstream <- Term];

validate(_VHost, <<"federation-upstream">>, Name, Term0, User) ->
    Term = rabbit_data_coercion:to_proplist(Term0),
    rabbit_parameter_validation:proplist(
      Name, [{<<"uri">>, validate_uri(User), mandatory} |
            shared_validation()], Term);

validate(_VHost, _Component, Name, _Term, _User) ->
    {error, "name not recognised: ~tp", [Name]}.

notify(_VHost, <<"federation-upstream-set">>, Name, _Term, _Username) ->
    adjust({upstream_set, Name});

notify(_VHost, <<"federation-upstream">>, Name, _Term, _Username) ->
    adjust({upstream, Name}).

notify_clear(_VHost, <<"federation-upstream-set">>, Name, _Username) ->
    adjust({clear_upstream_set, Name});

notify_clear(VHost, <<"federation-upstream">>, Name, _Username) ->
    adjust({clear_upstream, VHost, Name}).

adjust(Thing) ->
    Plugins = ets:tab2list(?FEDERATION_ETS),
    _ = [Module:adjust(Thing) || {_Name, #{link_module := Module}}  <- Plugins],
    ok.

%%----------------------------------------------------------------------------

shared_validation() ->
    [{<<"exchange">>,       fun rabbit_parameter_validation:binary/2, optional},
     {<<"queue">>,          fun rabbit_parameter_validation:binary/2, optional},
     {<<"consumer-tag">>,   fun rabbit_parameter_validation:binary/2, optional},
     {<<"prefetch-count">>, fun rabbit_parameter_validation:number/2, optional},
     {<<"reconnect-delay">>,fun rabbit_parameter_validation:number/2, optional},
     {<<"max-hops">>,       fun rabbit_parameter_validation:number/2, optional},
     {<<"expires">>,        fun rabbit_parameter_validation:number/2, optional},
     {<<"message-ttl">>,    fun rabbit_parameter_validation:number/2, optional},
     {<<"trust-user-id">>,  fun rabbit_parameter_validation:boolean/2, optional},
     {<<"ack-mode">>,       rabbit_parameter_validation:enum(
                              ['no-ack', 'on-publish', 'on-confirm']), optional},
     {<<"resource-cleanup-mode">>, rabbit_parameter_validation:enum(
                              ['default', 'never']), optional},
     {<<"queue-type">>,       rabbit_parameter_validation:enum(
                              ['classic', 'quorum']), optional},
     {<<"bind-nowait">>,    fun rabbit_parameter_validation:boolean/2, optional},
     {<<"channel-use-mode">>, rabbit_parameter_validation:enum(
                              ['multiple', 'single']), optional}].

validate_uri(User) ->
    fun (Name, Term) -> validate_uri(Name, Term, User) end.

validate_uri(Name, Term, User) when is_binary(Term) ->
    case rabbit_parameter_validation:binary(Name, Term) of
        ok -> case amqp_uri:parse(binary_to_list(Term)) of
                  {ok, Params} -> validate_uri_vhost_access(Params, User);
                  {error, E} -> {error, "\"~ts\" not a valid URI: ~tp", [Term, E]}
              end;
        E  -> E
    end;
validate_uri(Name, Term, User) ->
    case rabbit_parameter_validation:list(Name, Term) of
        ok -> case [V || U <- Term,
                         V <- [validate_uri(Name, U, User)],
                         element(1, V) =:= error] of
                  []      -> ok;
                  [E | _] -> E
              end;
        E  -> E
    end.

%% A direct URI carries its own virtual host; check access to it here.
validate_uri_vhost_access(#amqp_params_direct{}, none) ->
    ok;
validate_uri_vhost_access(#amqp_params_direct{virtual_host = VHost}, User = #user{}) ->
    try rabbit_access_control:check_vhost_access(User, VHost, undefined, #{}) of
        ok -> ok
    catch
        _:_ -> {error, "user \"~ts\" may not connect to vhost \"~ts\"",
                [User#user.username, VHost]}
    end;
validate_uri_vhost_access(_Params, _User) ->
    ok.

%%----------------------------------------------------------------------------

validate_policy([{<<"federation-upstream-set">>, Value}])
  when is_binary(Value) ->
    ok;
validate_policy([{<<"federation-upstream-set">>, Value}]) ->
    {error, "~tp is not a valid federation upstream set name", [Value]};

validate_policy([{<<"federation-upstream-pattern">>, Value}])
  when is_binary(Value) ->
    case rabbit_re:compile(Value) of
        {ok, _}                  -> ok;
        {error, pattern_too_long} ->
            {error, "federation-upstream-pattern must not exceed ~b bytes",
             [rabbit_re:max_pattern_length()]};
        {error, Reason}          ->
            {error, "could not compile pattern ~ts to a regular expression. "
                    "Error: ~tp", [Value, Reason]}
    end;
validate_policy([{<<"federation-upstream-pattern">>, Value}]) ->
    {error, "~tp is not a valid federation upstream pattern name", [Value]};

validate_policy([{<<"federation-upstream">>, Value}])
  when is_binary(Value) ->
    ok;
validate_policy([{<<"federation-upstream">>, Value}]) ->
    {error, "~tp is not a valid federation upstream name", [Value]};

validate_policy(L) when length(L) >= 2 ->
    {error, "cannot specify federation-upstream, federation-upstream-set "
            "or federation-upstream-pattern together", []}.
