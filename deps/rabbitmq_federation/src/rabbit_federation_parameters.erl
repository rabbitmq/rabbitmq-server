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
%% Copyright (c) 2007-2013 GoPivotal, Inc.  All rights reserved.
%%

-module(rabbit_federation_parameters).
-behaviour(rabbit_runtime_parameter).
-behaviour(rabbit_policy_validator).

-include_lib("rabbit_common/include/rabbit.hrl").

-export([validate/4, notify/4, notify_clear/3]).
-export([register/0, validate_policy/1]).

-rabbit_boot_step({?MODULE,
                   [{description, "federation parameters"},
                    {mfa, {rabbit_federation_parameters, register, []}},
                    {requires, rabbit_registry},
                    {enables, recovery}]}).

register() ->
    [rabbit_registry:register(Class, Name, ?MODULE) ||
        {Class, Name} <- [{runtime_parameter, <<"federation">>},
                          {runtime_parameter, <<"federation-upstream">>},
                          {runtime_parameter, <<"federation-upstream-set">>},
                          {policy_validator,  <<"federation-upstream">>},
                          {policy_validator,  <<"federation-upstream-set">>}]],
    ok.

validate(_VHost, <<"federation-upstream-set">>, Name, Term) ->
    [rabbit_parameter_validation:proplist(
       Name,
       [{<<"upstream">>, fun rabbit_parameter_validation:binary/2, mandatory} |
        shared_validation()], Upstream)
     || Upstream <- Term];

validate(_VHost, <<"federation-upstream">>, Name, Term) ->
    rabbit_parameter_validation:proplist(
      Name, [{<<"uri">>, fun validate_uri/2, mandatory} |
            shared_validation()], Term);

validate(_VHost, <<"federation">>, <<"local-nodename">>, Term) ->
    rabbit_parameter_validation:binary(<<"local-nodename">>, Term);

validate(_VHost, <<"federation">>, <<"local-username">>, Term) ->
    rabbit_parameter_validation:binary(<<"local-username">>, Term);

validate(_VHost, _Component, Name, _Term) ->
    {error, "name not recognised: ~p", [Name]}.

notify(_VHost, <<"federation-upstream-set">>, Name, _Term) ->
    adjust({upstream_set, Name});

notify(_VHost, <<"federation-upstream">>, Name, _Term) ->
    adjust({upstream, Name});

notify(_VHost, <<"federation">>, <<"local-nodename">>, _Term) ->
    adjust(everything);

notify(_VHost, <<"federation">>, <<"local-username">>, _Term) ->
    adjust(everything).

notify_clear(_VHost, <<"federation-upstream-set">>, Name) ->
    adjust({clear_upstream_set, Name});

notify_clear(_VHost, <<"federation-upstream">>, Name) ->
    adjust({clear_upstream, Name});

notify_clear(_VHost, <<"federation">>, <<"local-nodename">>) ->
    adjust(everything);

notify_clear(_VHost, <<"federation">>, <<"local-username">>) ->
    adjust(everything).

adjust(Thing) ->
    rabbit_federation_exchange_link_sup_sup:adjust(Thing),
    rabbit_federation_queue_link_sup_sup:adjust(Thing).

%%----------------------------------------------------------------------------

shared_validation() ->
    [{<<"exchange">>,       fun rabbit_parameter_validation:binary/2, optional},
     {<<"queue">>,          fun rabbit_parameter_validation:binary/2, optional},
     {<<"prefetch-count">>, fun rabbit_parameter_validation:number/2, optional},
     {<<"reconnect-delay">>,fun rabbit_parameter_validation:number/2, optional},
     {<<"max-hops">>,       fun rabbit_parameter_validation:number/2, optional},
     {<<"expires">>,        fun rabbit_parameter_validation:number/2, optional},
     {<<"message-ttl">>,    fun rabbit_parameter_validation:number/2, optional},
     {<<"trust-user-id">>,  fun rabbit_parameter_validation:boolean/2, optional},
     {<<"ack-mode">>,       rabbit_parameter_validation:enum(
                              ['no-ack', 'on-publish', 'on-confirm']), optional},
     {<<"ha-policy">>,      fun rabbit_parameter_validation:binary/2, optional}].

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

validate_policy([{<<"federation-upstream">>, Value}])
  when is_binary(Value) ->
    ok;
validate_policy([{<<"federation-upstream">>, Value}]) ->
    {error, "~p is not a valid federation upstream name", [Value]};

validate_policy(L) when length(L) =:= 2 ->
    {error, "cannot specify federation-upstream and federation-upstream-set "
     "together", []}.

