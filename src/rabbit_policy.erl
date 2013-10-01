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

-module(rabbit_policy).

%% TODO specs

-behaviour(rabbit_runtime_parameter).

-include("rabbit.hrl").

-import(rabbit_misc, [pget/2]).

-export([register/0]).
-export([invalidate/0, recover/0]).
-export([name/1, get/2, get_arg/3, set/1]).
-export([validate/4, notify/4, notify_clear/3]).
-export([parse_set/6, set/6, delete/2, lookup/2, list/0, list/1,
         list_formatted/1, info_keys/0]).

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
name0(Policy)    -> pget(name, Policy).

set(Q = #amqqueue{name = Name}) -> rabbit_queue_decorator:set(
                                     Q#amqqueue{policy = set0(Name)});
set(X = #exchange{name = Name}) -> rabbit_exchange_decorator:set(
                                     X#exchange{policy = set0(Name)}).

set0(Name = #resource{virtual_host = VHost}) -> match(Name, list(VHost)).

set(Q = #amqqueue{name = Name}, Ps) -> Q#amqqueue{policy = match(Name, Ps)};
set(X = #exchange{name = Name}, Ps) -> rabbit_exchange_decorator:set(
                                         X#exchange{policy = match(Name, Ps)}).

get(Name, #amqqueue{policy = Policy}) -> get0(Name, Policy);
get(Name, #exchange{policy = Policy}) -> get0(Name, Policy);
%% Caution - SLOW.
get(Name, EntityName = #resource{virtual_host = VHost}) ->
    get0(Name, match(EntityName, list(VHost))).

get0(_Name, undefined) -> undefined;
get0(Name, List)       -> case pget(definition, List) of
                              undefined -> undefined;
                              Policy    -> pget(Name, Policy)
                          end.

%% Many heads for optimisation
get_arg(_AName, _PName,     #exchange{arguments = [], policy = undefined}) ->
    undefined;
get_arg(_AName,  PName, X = #exchange{arguments = []}) ->
    get(PName, X);
get_arg(AName,   PName, X = #exchange{arguments = Args}) ->
    case rabbit_misc:table_lookup(Args, AName) of
        undefined    -> get(PName, X);
        {_Type, Arg} -> Arg
    end.

%%----------------------------------------------------------------------------

%% Gets called during upgrades - therefore must not assume anything about the
%% state of Mnesia
invalidate() ->
    rabbit_file:write_file(invalid_file(), <<"">>).

recover() ->
    case rabbit_file:is_file(invalid_file()) of
        true  -> recover0(),
                 rabbit_file:delete(invalid_file());
        false -> ok
    end.

%% To get here we have to have just completed an Mnesia upgrade - i.e. we are
%% the first node starting. So we can rewrite the whole database.  Note that
%% recovery has not yet happened; we must work with the rabbit_durable_<thing>
%% variants.
recover0() ->
    Xs = mnesia:dirty_match_object(rabbit_durable_exchange, #exchange{_ = '_'}),
    Qs = mnesia:dirty_match_object(rabbit_durable_queue,    #amqqueue{_ = '_'}),
    Policies = list(),
    [rabbit_misc:execute_mnesia_transaction(
       fun () ->
               mnesia:write(rabbit_durable_exchange, set(X, Policies), write)
       end) || X <- Xs],
    [rabbit_misc:execute_mnesia_transaction(
       fun () ->
               mnesia:write(rabbit_durable_queue, set(Q, Policies), write)
       end) || Q <- Qs],
    ok.

invalid_file() ->
    filename:join(rabbit_mnesia:dir(), "policies_are_invalid").

%%----------------------------------------------------------------------------

parse_set(VHost, Name, Pattern, Definition, Priority, ApplyTo) ->
    try list_to_integer(Priority) of
        Num -> parse_set0(VHost, Name, Pattern, Definition, Num, ApplyTo)
    catch
        error:badarg -> {error, "~p priority must be a number", [Priority]}
    end.

parse_set0(VHost, Name, Pattern, Defn, Priority, ApplyTo) ->
    case rabbit_misc:json_decode(Defn) of
        {ok, JSON} ->
            set0(VHost, Name,
                 [{<<"pattern">>,    list_to_binary(Pattern)},
                  {<<"definition">>, rabbit_misc:json_to_term(JSON)},
                  {<<"priority">>,   Priority},
                  {<<"apply-to">>,   ApplyTo}]);
        error ->
            {error_string, "JSON decoding error"}
    end.

set(VHost, Name, Pattern, Definition, Priority, ApplyTo) ->
    PolicyProps = [{<<"pattern">>,    Pattern},
                   {<<"definition">>, Definition},
                   {<<"priority">>,   case Priority of
                                          undefined -> 0;
                                          _         -> Priority
                                      end},
                   {<<"apply-to">>,   case ApplyTo of
                                          undefined -> <<"all">>;
                                          _         -> ApplyTo
                                      end}],
    set0(VHost, Name, PolicyProps).

set0(VHost, Name, Term) ->
    rabbit_runtime_parameters:set_any(VHost, <<"policy">>, Name, Term).

delete(VHost, Name) ->
    rabbit_runtime_parameters:clear_any(VHost, <<"policy">>, Name).

lookup(VHost, Name) ->
    case rabbit_runtime_parameters:lookup(VHost, <<"policy">>, Name) of
        not_found  -> not_found;
        P          -> p(P, fun ident/1)
    end.

list() ->
    list('_').

list(VHost) ->
    list0(VHost, fun ident/1).

list_formatted(VHost) ->
    order_policies(list0(VHost, fun format/1)).

list0(VHost, DefnFun) ->
    [p(P, DefnFun) || P <- rabbit_runtime_parameters:list(VHost, <<"policy">>)].

order_policies(PropList) ->
    lists:sort(fun (A, B) -> pget(priority, A) < pget(priority, B) end,
               PropList).

p(Parameter, DefnFun) ->
    Value = pget(value, Parameter),
    [{vhost,      pget(vhost, Parameter)},
     {name,       pget(name, Parameter)},
     {pattern,    pget(<<"pattern">>, Value)},
     {'apply-to', pget(<<"apply-to">>, Value)},
     {definition, DefnFun(pget(<<"definition">>, Value))},
     {priority,   pget(<<"priority">>, Value)}].

format(Term) ->
    {ok, JSON} = rabbit_misc:json_encode(rabbit_misc:term_to_json(Term)),
    list_to_binary(JSON).

ident(X) -> X.

info_keys() -> [vhost, name, 'apply-to', pattern, definition, priority].

%%----------------------------------------------------------------------------

validate(_VHost, <<"policy">>, Name, Term) ->
    rabbit_parameter_validation:proplist(
      Name, policy_validation(), Term).

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
    [catch notify(X) || X <- Xs],
    [catch notify(Q) || Q <- Qs],
    ok.

update_exchange(X = #exchange{name = XName, policy = OldPolicy}, Policies) ->
    case match(XName, Policies) of
        OldPolicy -> no_change;
        NewPolicy -> case rabbit_exchange:update(
                            XName, fun (X0) ->
                                           rabbit_exchange_decorator:set(
                                             X0 #exchange{policy = NewPolicy})
                                   end) of
                         #exchange{} = X1 -> {X, X1};
                         not_found        -> {X, X }
                     end
    end.

update_queue(Q = #amqqueue{name = QName, policy = OldPolicy}, Policies) ->
    case match(QName, Policies) of
        OldPolicy -> no_change;
        NewPolicy -> case rabbit_amqqueue:update(
                       QName, fun(Q1) ->
                                      rabbit_queue_decorator:set(
                                        Q1#amqqueue{policy = NewPolicy})
                              end) of
                         #amqqueue{} = Q1 -> {Q, Q1};
                         not_found        -> {Q, Q }
                     end
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

matches(#resource{name = Name, kind = Kind, virtual_host = VHost}, Policy) ->
    matches_type(Kind, pget('apply-to', Policy)) andalso
        match =:= re:run(Name, pget(pattern, Policy), [{capture, none}]) andalso
        VHost =:= pget(vhost, Policy).

matches_type(exchange, <<"exchanges">>) -> true;
matches_type(queue,    <<"queues">>)    -> true;
matches_type(exchange, <<"all">>)       -> true;
matches_type(queue,    <<"all">>)       -> true;
matches_type(_,        _)               -> false.

sort_pred(A, B) -> pget(priority, A) >= pget(priority, B).

%%----------------------------------------------------------------------------

policy_validation() ->
    [{<<"priority">>,   fun rabbit_parameter_validation:number/2, mandatory},
     {<<"pattern">>,    fun rabbit_parameter_validation:regex/2,  mandatory},
     {<<"apply-to">>,   fun apply_to_validation/2,                optional},
     {<<"definition">>, fun validation/2,                         mandatory}].

validation(_Name, []) ->
    {error, "no policy provided", []};
validation(_Name, Terms) when is_list(Terms) ->
    {Keys, Modules} = lists:unzip(
                        rabbit_registry:lookup_all(policy_validator)),
    [] = dups(Keys), %% ASSERTION
    Validators = lists:zipwith(fun (M, K) ->  {M, a2b(K)} end, Modules, Keys),
    case is_proplist(Terms) of
        true  -> {TermKeys, _} = lists:unzip(Terms),
                 case dups(TermKeys) of
                     []   -> validation0(Validators, Terms);
                     Dup  -> {error, "~p duplicate keys not allowed", [Dup]}
                 end;
        false -> {error, "definition must be a dictionary: ~p", [Terms]}
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

is_proplist(L) -> length(L) =:= length([I || I = {_, _} <- L]).

apply_to_validation(_Name, <<"all">>)       -> ok;
apply_to_validation(_Name, <<"exchanges">>) -> ok;
apply_to_validation(_Name, <<"queues">>)    -> ok;
apply_to_validation(_Name, Term) ->
    {error, "apply-to '~s' unrecognised; should be 'queues', 'exchanges' "
     "or 'all'", [Term]}.
