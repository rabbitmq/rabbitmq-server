%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_federation_parameters).
-behaviour(rabbit_runtime_parameter).
-behaviour(rabbit_policy_validator).

-include_lib("rabbit_common/include/rabbit.hrl").

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

validate(_VHost, <<"federation-upstream">>, Name, Term0, _User) ->
    Term = rabbit_data_coercion:to_proplist(Term0),
    rabbit_parameter_validation:proplist(
      Name, [{<<"uri">>, fun validate_uri/2, mandatory} |
            shared_validation()], Term);

validate(_VHost, _Component, Name, _Term, _User) ->
    {error, "name not recognised: ~p", [Name]}.

notify(_VHost, <<"federation-upstream-set">>, Name, _Term, _Username) ->
    adjust({upstream_set, Name});

notify(_VHost, <<"federation-upstream">>, Name, _Term, _Username) ->
    adjust({upstream, Name}).

notify_clear(_VHost, <<"federation-upstream-set">>, Name, _Username) ->
    adjust({clear_upstream_set, Name});

notify_clear(VHost, <<"federation-upstream">>, Name, _Username) ->
    rabbit_federation_exchange_link_sup_sup:adjust({clear_upstream, VHost, Name}),
    rabbit_federation_queue_link_sup_sup:adjust({clear_upstream, VHost, Name}).

adjust(Thing) ->
    rabbit_federation_exchange_link_sup_sup:adjust(Thing),
    rabbit_federation_queue_link_sup_sup:adjust(Thing).

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
     {<<"ha-policy">>,      fun rabbit_parameter_validation:binary/2, optional},
     {<<"bind-nowait">>,    fun rabbit_parameter_validation:boolean/2, optional},
     {<<"channel-use-mode">>, rabbit_parameter_validation:enum(
                              ['multiple', 'single']), optional}].

validate_uri(Name, Term) when is_binary(Term) ->
    case rabbit_parameter_validation:binary(Name, Term) of
        ok -> case amqp_uri:parse(binary_to_list(Term)) of
                  {ok, _}    -> ok;
                  {error, E} -> {error, "\"~s\" not a valid URI: ~p", [Term, E]}
              end;
        E  -> E
    end;
validate_uri(Name, Term) ->
    case rabbit_parameter_validation:list(Name, Term) of
        ok -> case [V || U <- Term,
                         V <- [validate_uri(Name, U)],
                         element(1, V) =:= error] of
                  []      -> ok;
                  [E | _] -> E
              end;
        E  -> E
    end.

%%----------------------------------------------------------------------------

validate_policy([{<<"federation-upstream-set">>, Value}])
  when is_binary(Value) ->
    ok;
validate_policy([{<<"federation-upstream-set">>, Value}]) ->
    {error, "~p is not a valid federation upstream set name", [Value]};

validate_policy([{<<"federation-upstream-pattern">>, Value}])
  when is_binary(Value) ->
    case re:compile(Value) of
        {ok, _}         -> ok;
        {error, Reason} -> {error, "could not compile pattern ~s to a regular expression. "
                                   "Error: ~p", [Value, Reason]}
    end;
validate_policy([{<<"federation-upstream-pattern">>, Value}]) ->
    {error, "~p is not a valid federation upstream pattern name", [Value]};

validate_policy([{<<"federation-upstream">>, Value}])
  when is_binary(Value) ->
    ok;
validate_policy([{<<"federation-upstream">>, Value}]) ->
    {error, "~p is not a valid federation upstream name", [Value]};

validate_policy(L) when length(L) >= 2 ->
    {error, "cannot specify federation-upstream, federation-upstream-set "
            "or federation-upstream-pattern together", []}.
