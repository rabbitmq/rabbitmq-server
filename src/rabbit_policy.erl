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
-export([parse_add/5, add/5, delete/2, lookup/2, list/0, list/1,
         list_formatted/1, info_keys/0]).

-define(TABLE, rabbit_runtime_parameters).

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
get0(Name, List)       -> case pget(definition, List) of
                              undefined -> {error, not_found};
                              Policy    -> case pget(Name, Policy) of
                                               undefined -> {error, not_found};
                                               Value    -> {ok, Value}
                                           end
                          end.

%%----------------------------------------------------------------------------

parse_add(VHost, Key, Pattern, Definition, undefined) ->
    parse_add_policy0(VHost, Key, Pattern, Definition, []);
parse_add(VHost, Key, Pattern, Definition, Priority) ->
    try list_to_integer(Priority) of
        Num -> parse_add_policy0(VHost, Key, Pattern, Definition,
                                 [{<<"priority">>, Num}])
    catch
        error:badarg -> {error, "~p priority must be a number", [Priority]}
    end.

parse_add_policy0(VHost, Key, Pattern, Defn, Priority) ->
    case rabbit_misc:json_decode(Defn) of
        {ok, JSON} ->
            add0(VHost, Key, [{<<"pattern">>, list_to_binary(Pattern)},
                              {<<"policy">>, rabbit_misc:json_to_term(JSON)}] ++
                             Priority);
        error ->
            {error_string, "JSON decoding error"}
    end.

add(VHost, Key, Pattern, Definition, Priority) ->
    PolicyProps = [{<<"pattern">>, Pattern}, {<<"policy">>, Definition}],
    add0(VHost, Key, case Priority of
                         undefined -> [];
                         _         -> [{<<"priority">>, Priority}]
                     end ++ PolicyProps).

add0(VHost, Key, Term) ->
    rabbit_runtime_parameters:set_any(VHost, <<"policy">>, Key, Term).

delete(VHost, Key) ->
    rabbit_runtime_parameters:clear_any(VHost, <<"policy">>, Key).

lookup(VHost, Key) ->
    case mnesia:dirty_read(?TABLE, {VHost, <<"policy">>, Key}) of
        []  -> not_found;
        [P] -> p(P, fun ident/1)
    end.

list() ->
    list('_').

list(VHost) ->
    list0(VHost, fun ident/1).

list_formatted(VHost) ->
    order_policies(list0(VHost, fun format/1)).

list0(VHost, DefnFun) ->
    Match = #runtime_parameters{key = {VHost, <<"policy">>, '_'}, _ = '_'},
    [p(P, DefnFun) || P <- mnesia:dirty_match_object(?TABLE, Match)].

order_policies(PropList) ->
    lists:sort(fun (A, B) -> pget(priority, A, 0) < pget(priority, B, 0) end,
               PropList).

p(#runtime_parameters{key = {VHost, <<"policy">>, Key}, value = Value},
  DefnFun) ->
    [{vhost,      VHost},
     {key,        Key},
     {pattern,    pget(<<"pattern">>, Value)},
     {definition, DefnFun(pget(<<"policy">>,  Value))}] ++
    case pget(<<"priority">>, Value) of
        undefined -> [];
        Priority  -> [{priority, Priority}]
    end.

format(Term) ->
    {ok, JSON} = rabbit_misc:json_encode(rabbit_misc:term_to_json(Term)),
    list_to_binary(JSON).

ident(X) -> X.

info_keys() -> [vhost, key, pattern, definition, priority].

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
    case match(XName, Policies) of
        OldPolicy -> no_change;
        NewPolicy -> rabbit_exchange:update(
                       XName, fun(X1) -> X1#exchange{policy = NewPolicy} end),
                     {X, X#exchange{policy = NewPolicy}}
    end.

update_queue(Q = #amqqueue{name = QName, policy = OldPolicy}, Policies) ->
    case match(QName, Policies) of
        OldPolicy -> no_change;
        NewPolicy -> rabbit_amqqueue:update(
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
    match =:= re:run(Name, pget(pattern, Policy), [{capture, none}]).

sort_pred(A, B) -> pget(priority, A, 0) >= pget(priority, B, 0).

%%----------------------------------------------------------------------------

policy_validation() ->
    [{<<"priority">>, fun rabbit_parameter_validation:number/2, optional},
     {<<"pattern">>,  fun rabbit_parameter_validation:regex/2,  mandatory},
     {<<"policy">>,   fun validation/2,                         mandatory}].

validation(_Name, []) ->
    {error, "no policy provided", []};
validation(_Name, Terms) when is_list(Terms) ->
    {Keys, Modules} = lists:unzip(
                        rabbit_registry:lookup_all(policy_validator)),
    [] = dups(Keys), %% ASSERTION
    Validators = lists:zipwith(fun (M, K) ->  {M, a2b(K)} end, Modules, Keys),
    {TermKeys, _} = lists:unzip(Terms),
    case dups(TermKeys) of
        []   -> validation0(Validators, Terms);
        Dup  -> {error, "~p duplicate keys not allowed", [Dup]}
    end;
validation(_Name, Term) ->
    {error, "parse error while reading policy: ~p", [Term]}.

validation0(Validators, Terms) ->
    case lists:foldl(
           fun (Mod, {ok, TermsLeft}) ->
                   ModKeys = proplists:get_all_values(Mod, Validators),
                   case [T || {Key, _} = T <- TermsLeft,
                              lists:member(Key, ModKeys)] of
                       []    -> {ok, TermsLeft};
                       Scope -> {Mod:validate_policy(Scope), TermsLeft -- Scope}
                   end;
               (_, Acc) ->
                   Acc
           end, {ok, Terms}, proplists:get_keys(Validators)) of
         {ok, []} ->
             ok;
         {ok, Unvalidated} ->
             {error, "~p are not recognised policy settings", [Unvalidated]};
         {Error, _} ->
             Error
    end.

a2b(A) -> list_to_binary(atom_to_list(A)).

dups(L) -> L -- lists:usort(L).
