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

-module(rabbit_federation_parameters).
-behaviour(rabbit_runtime_parameter).

-include_lib("rabbit_common/include/rabbit.hrl").

-export([validate/3, validate_clear/2, notify/3, notify_clear/2]).
-export([register/0]).

-rabbit_boot_step({?MODULE,
                   [{description, "federation parameters"},
                    {mfa, {rabbit_federation_parameters, register, []}},
                    {requires, rabbit_registry},
                    {enables, recovery}]}).

register() ->
    [rabbit_registry:register(runtime_parameter, Name, ?MODULE) ||
        Name <- [<<"federation">>,
                 <<"federation-upstream">>,
                 <<"federation-upstream-set">>]].

validate(<<"federation-upstream-set">>, Key, Term) ->
    [rabbit_parameter_validation:proplist(
       Key,
       [{<<"upstream">>, fun rabbit_parameter_validation:binary/2, mandatory},
        {<<"exchange">>, fun rabbit_parameter_validation:binary/2, optional} |
        shared_validation()], Upstream)
     || Upstream <- Term];

validate(<<"federation-upstream">>, Key, Term) ->
    rabbit_parameter_validation:proplist(
      Key, [{<<"uri">>, fun validate_uri/2, mandatory} |
            shared_validation()], Term);

validate(<<"federation">>, <<"local-nodename">>, Term) ->
    rabbit_parameter_validation:binary(<<"local-nodename">>, Term);

validate(<<"federation">>, <<"local-username">>, Term) ->
    rabbit_parameter_validation:binary(<<"local-username">>, Term);

validate(_Component, Key, _Term) ->
    {error, "key not recognised: ~p", [Key]}.

validate_clear(<<"federation-upstream-set">>, _Key) ->
    ok;

validate_clear(<<"federation-upstream">>, _Key) ->
    ok;

validate_clear(<<"federation">>, <<"local-nodename">>) ->
    ok;

validate_clear(<<"federation">>, <<"local-username">>) ->
    ok;

validate_clear(_Component, Key) ->
    {error, "key not recognised: ~p", [Key]}.

notify(<<"federation-upstream-set">>, Key, _Term) ->
    rabbit_federation_link_sup_sup:adjust({upstream_set, Key});

notify(<<"federation-upstream">>, Key, _Term) ->
    rabbit_federation_link_sup_sup:adjust({upstream, Key});

notify(<<"federation">>, <<"local-nodename">>, _Term) ->
    rabbit_federation_link_sup_sup:adjust(everything);

notify(<<"federation">>, <<"local-username">>, _Term) ->
    rabbit_federation_link_sup_sup:adjust(everything).

notify_clear(<<"federation-upstream-set">>, Key) ->
    rabbit_federation_link_sup_sup:adjust({clear_upstream_set, Key});

notify_clear(<<"federation-upstream">>, Key) ->
    rabbit_federation_link_sup_sup:adjust({clear_upstream, Key});

notify_clear(<<"federation">>, <<"local-nodename">>) ->
    rabbit_federation_link_sup_sup:adjust(everything);

notify_clear(<<"federation">>, <<"local-username">>) ->
    rabbit_federation_link_sup_sup:adjust(everything).

%%----------------------------------------------------------------------------

shared_validation() ->
    [{<<"exchange">>,       fun rabbit_parameter_validation:binary/2, optional},
     {<<"prefetch-count">>, fun rabbit_parameter_validation:number/2, optional},
     {<<"reconnect-delay">>,fun rabbit_parameter_validation:number/2, optional},
     {<<"max-hops">>,       fun rabbit_parameter_validation:number/2, optional},
     {<<"expires">>,        fun rabbit_parameter_validation:number/2, optional},
     {<<"message-ttl">>,    fun rabbit_parameter_validation:number/2, optional},
     {<<"ha-policy">>,      fun rabbit_parameter_validation:binary/2, optional}].

validate_uri(Name, Term) ->
    case rabbit_parameter_validation:binary(Name, Term) of
        ok -> case amqp_uri:parse(binary_to_list(Term)) of
                  {ok, _}    -> ok;
                  {error, E} -> {error, "\"~s\" not a valid URI: ~p", [Term, E]}
              end;
        E  -> E
    end.
