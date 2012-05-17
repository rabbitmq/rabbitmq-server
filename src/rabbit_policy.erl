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

-behaviour(rabbit_runtime_parameter).

-include("rabbit.hrl").

-import(rabbit_misc, [pget/2]).

-export([register/0]).
-export([name/1, get/2, set/1]).
-export([validate/3, validate_clear/2, notify/3, notify_clear/2]).

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

set0(Name) -> match(Name, list()).

get(Name, #amqqueue{policy = Policy}) -> get0(Name, Policy);
get(Name, #exchange{policy = Policy}) -> get0(Name, Policy).

get0(_Name, undefined) -> {error, not_found};
get0(Name, List)       -> case pget(<<"policy">>, List) of
                              undefined -> {error, not_found};
                              Policy    -> case pget(Name, Policy) of
                                               undefined -> {error, not_found};
                                               Value    -> {ok, Value}
                                           end
                          end.

%%----------------------------------------------------------------------------

validate(<<"policy">>, _Name, Term) ->
    assert_contents(policy_validation(), Term).

validate_clear(<<"policy">>, _Name) ->
    ok.

notify(<<"policy">>, _Name, _Term) ->
    update_policies().

notify_clear(<<"policy">>, _Name) ->
    update_policies().

%%----------------------------------------------------------------------------

list() ->
    [[{<<"name">>, pget(key, P)} | pget(value, P)]
     || P <- rabbit_runtime_parameters:list(<<"policy">>)].

update_policies() ->
    Policies = list(),
    {Xs, Qs} = rabbit_misc:execute_mnesia_transaction(
                 fun() ->
                         {[update_exchange(X, Policies) ||
                              VHost <- rabbit_vhost:list(),
                              X     <- rabbit_exchange:list(VHost)],
                          [update_queue(Q, Policies) ||
                              VHost <- rabbit_vhost:list(),
                              Q     <- rabbit_amqqueue:list(VHost)]}
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

matches(#resource{name = Name, virtual_host = VHost}, Policy) ->
    Prefix = pget(<<"prefix">>, Policy),
    case pget(<<"vhost">>, Policy) of
        undefined -> prefix(Prefix, Name);
        VHost     -> prefix(Prefix, Name);
        _         -> false
    end.

prefix(A, B) -> lists:prefix(binary_to_list(A), binary_to_list(B)).

sort_pred(A, B) ->
    R = size(pget(<<"prefix">>, A)) >= size(pget(<<"prefix">>, B)),
    case {pget(<<"vhost">>, A), pget(<<"vhost">>, B)} of
        {undefined, undefined} -> R;
        {undefined, _}         -> true;
        {_, undefined}         -> false;
        _                      -> R
    end.

%%----------------------------------------------------------------------------

policy_validation() ->
    [{<<"vhost">>,  binary, optional},
     {<<"prefix">>, binary, mandatory},
     {<<"policy">>, list,   mandatory}].

%% TODO this is mostly duplicated from
%% rabbit_federation_parameters. Sort that out in some way.

assert_type(Name, {Type, Opts}, Term) ->
    assert_type(Name, Type, Term),
    case lists:member(Term, Opts) of
        true  -> ok;
        false -> {error, "~s must be one of ~p", [Name, Opts]}
    end;

assert_type(_Name, number, Term) when is_number(Term) ->
    ok;

assert_type(Name, number, Term) ->
    {error, "~s should be number, actually was ~p", [Name, Term]};

assert_type(_Name, binary, Term) when is_binary(Term) ->
    ok;

assert_type(Name, binary, Term) ->
    {error, "~s should be binary, actually was ~p", [Name, Term]};

assert_type(_Name, list, Term) when is_list(Term) ->
    ok;

assert_type(Name, list, Term) ->
    {error, "~s should be list, actually was ~p", [Name, Term]}.

assert_contents(Constraints, Term) when is_list(Term) ->
    {Results, Remainder}
        = lists:foldl(
            fun ({Name, Constraint, Needed}, {Results0, Term0}) ->
                    case {lists:keytake(Name, 1, Term0), Needed} of
                        {{value, {Name, Value}, Term1}, _} ->
                            {[assert_type(Name, Constraint, Value) | Results0],
                             Term1};
                        {false, mandatory} ->
                            {[{error, "Key \"~s\" not found", [Name]} |
                              Results0], Term0};
                        {false, optional} ->
                            {Results0, Term0}
                    end
            end, {[], Term}, Constraints),
    case Remainder of
        [] -> Results;
        _  -> [{error, "Unrecognised terms ~p", [Remainder]} | Results]
    end;

assert_contents(_Constraints, Term) ->
    {error, "Not a list ~p", [Term]}.
