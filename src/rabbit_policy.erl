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

-module(rabbit_policy).

%% TODO specs

-behaviour(rabbit_runtime_parameter).

-include("rabbit.hrl").

-import(rabbit_misc, [pget/2, pget/3]).

-export([register/0]).
-export([name/1, get/2, set/1]).
-export([validate/4, validate_clear/3, notify/4, notify_clear/3]).

-rabbit_boot_step({?MODULE,
                   [{description, "policy parameters"},
                    {mfa, {rabbit_policy, register, []}},
                    {requires, rabbit_registry},
                    {enables, recovery}]}).

register() ->
    rabbit_registry:register(runtime_parameter, <<"policy">>, ?MODULE).

name(#amqqueue{policy = Policy}) -> name0(Policy);
name(#exchange{policy = Policy}) -> name0(Policy).

name0(undefined) -> none;
name0(Policy)    -> pget(<<"name">>, Policy).

set(Q = #amqqueue{name = Name}) -> Q#amqqueue{policy = set0(Name)};
set(X = #exchange{name = Name}) -> X#exchange{policy = set0(Name)}.

set0(Name = #resource{virtual_host = VHost}) -> match(Name, list(VHost)).

get(Name, #amqqueue{policy = Policy}) -> get0(Name, Policy);
get(Name, #exchange{policy = Policy}) -> get0(Name, Policy);
%% Caution - SLOW.
get(Name, EntityName = #resource{virtual_host = VHost}) ->
    get0(Name, match(EntityName, list(VHost))).

get0(_Name, undefined) -> {error, not_found};
get0(Name, List)       -> case pget(<<"policy">>, List) of
                              undefined -> {error, not_found};
                              Policy    -> case pget(Name, Policy) of
                                               undefined -> {error, not_found};
                                               Value    -> {ok, Value}
                                           end
                          end.

%%----------------------------------------------------------------------------

validate(_VHost, <<"policy">>, Name, Term) ->
    rabbit_parameter_validation:proplist(
      Name, policy_validation(), Term).

validate_clear(_VHost, <<"policy">>, _Name) ->
    ok.

notify(VHost, <<"policy">>, _Name, _Term) ->
    update_policies(VHost).

notify_clear(VHost, <<"policy">>, _Name) ->
    update_policies(VHost).

%%----------------------------------------------------------------------------

list(VHost) ->
    [[{<<"name">>, pget(key, P)} | pget(value, P)]
     || P <- rabbit_runtime_parameters:list(VHost, <<"policy">>)].

update_policies(VHost) ->
    Policies = list(VHost),
    {Xs, Qs} = rabbit_misc:execute_mnesia_transaction(
                 fun() ->
                         {[update_exchange(X, Policies) ||
                              X <- rabbit_exchange:list(VHost)],
                          [update_queue(Q, Policies) ||
                              Q <- rabbit_amqqueue:list(VHost)]}
                 end),
    [notify(X) || X <- Xs],
    [notify(Q) || Q <- Qs],
    ok.

update_exchange(X = #exchange{name = XName, policy = OldPolicy}, Policies) ->
    NewPolicy = match(XName, Policies),
    case NewPolicy of
        OldPolicy -> no_change;
        _         -> rabbit_exchange:update(
                       XName, fun(X1) -> X1#exchange{policy = NewPolicy} end),
                     {X, X#exchange{policy = NewPolicy}}
    end.

update_queue(Q = #amqqueue{name = QName, policy = OldPolicy}, Policies) ->
    NewPolicy = match(QName, Policies),
    case NewPolicy of
        OldPolicy -> no_change;
        _         -> rabbit_amqqueue:update(
                       QName, fun(Q1) -> Q1#amqqueue{policy = NewPolicy} end),
                     {Q, Q#amqqueue{policy = NewPolicy}}
    end.

notify(no_change)->
    ok;
notify({X1 = #exchange{}, X2 = #exchange{}}) ->
    rabbit_exchange:policy_changed(X1, X2);
notify({Q1 = #amqqueue{}, Q2 = #amqqueue{}}) ->
    rabbit_amqqueue:policy_changed(Q1, Q2).

match(Name, Policies) ->
    case lists:sort(fun sort_pred/2, [P || P <- Policies, matches(Name, P)]) of
        []               -> undefined;
        [Policy | _Rest] -> Policy
    end.

matches(#resource{name = Name}, Policy) ->
    match =:= re:run(Name, pget(<<"pattern">>, Policy), [{capture, none}]).

sort_pred(A, B) ->
    pget(<<"priority">>, A, 0) >= pget(<<"priority">>, B, 0).

%%----------------------------------------------------------------------------

policy_validation() ->
    [{<<"priority">>, fun rabbit_parameter_validation:number/2, optional},
     {<<"pattern">>, fun rabbit_parameter_validation:regex/2, mandatory},
     {<<"policy">>, fun rabbit_parameter_validation:list/2, mandatory}].
