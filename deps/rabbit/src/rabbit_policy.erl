%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_policy).

%% Policies is a way to apply optional arguments ("x-args")
%% to exchanges and queues in bulk, using name matching.
%%
%% Only one policy can apply to a given queue or exchange
%% at a time. Priorities help determine what policy should
%% take precedence.
%%
%% Policies build on runtime parameters. Policy-driven parameters
%% are well known and therefore validated.
%%
%% See also:
%%
%%  * rabbit_runtime_parameters
%%  * rabbit_policies
%%  * rabbit_registry

%% TODO specs

-behaviour(rabbit_runtime_parameter).

-include_lib("rabbit_common/include/rabbit.hrl").
-include("amqqueue.hrl").

-import(rabbit_misc, [pget/2, pget/3]).

-export([register/0]).
-export([invalidate/0, recover/0]).
-export([name/1, name_op/1, effective_definition/1, merge_operator_definitions/2, get/2, get_arg/3, set/1]).
-export([validate/5, notify/5, notify_clear/4]).
-export([parse_set/7, set/7, delete/3, lookup/2, list/0, list/1,
         list_formatted/1, list_formatted/3, info_keys/0]).
-export([parse_set_op/7, set_op/7, delete_op/3, lookup_op/2, list_op/0, list_op/1,
         list_formatted_op/1, list_formatted_op/3]).

-rabbit_boot_step({?MODULE,
                   [{description, "policy parameters"},
                    {mfa, {rabbit_policy, register, []}},
                    {requires, rabbit_registry},
                    {enables, recovery}]}).

register() ->
    rabbit_registry:register(runtime_parameter, <<"policy">>, ?MODULE),
    rabbit_registry:register(runtime_parameter, <<"operator_policy">>, ?MODULE).

name(Q) when ?is_amqqueue(Q) ->
    Policy = amqqueue:get_policy(Q),
    name0(Policy);
name(#exchange{policy = Policy}) -> name0(Policy).

name_op(Q) when ?is_amqqueue(Q) ->
    OpPolicy = amqqueue:get_operator_policy(Q),
    name0(OpPolicy);
name_op(#exchange{operator_policy = Policy}) -> name0(Policy).

name0(undefined) -> none;
name0(Policy)    -> pget(name, Policy).

effective_definition(Q) when ?is_amqqueue(Q) ->
    Policy = amqqueue:get_policy(Q),
    OpPolicy = amqqueue:get_operator_policy(Q),
    merge_operator_definitions(Policy, OpPolicy);
effective_definition(#exchange{policy = Policy, operator_policy = OpPolicy}) ->
    merge_operator_definitions(Policy, OpPolicy).

merge_operator_definitions(undefined, undefined) -> undefined;
merge_operator_definitions(Policy, undefined)    -> pget(definition, Policy);
merge_operator_definitions(undefined, OpPolicy)  -> pget(definition, OpPolicy);
merge_operator_definitions(Policy, OpPolicy) ->
    OpDefinition = rabbit_data_coercion:to_map(pget(definition, OpPolicy, [])),
    Definition   = rabbit_data_coercion:to_map(pget(definition, Policy, [])),
    Keys = maps:keys(Definition),
    OpKeys = maps:keys(OpDefinition),
    lists:map(fun(Key) ->
        case {maps:get(Key, Definition, undefined), maps:get(Key, OpDefinition, undefined)} of
            {Val, undefined}   -> {Key, Val};
            {undefined, OpVal} -> {Key, OpVal};
            {Val, OpVal}       -> {Key, merge_policy_value(Key, Val, OpVal)}
        end
    end,
    lists:umerge(Keys, OpKeys)).

set(Q0) when ?is_amqqueue(Q0) ->
    Name = amqqueue:get_name(Q0),
    Policy = match(Name),
    OpPolicy = match_op(Name),
    Q1 = amqqueue:set_policy(Q0, Policy),
    Q2 = amqqueue:set_operator_policy(Q1, OpPolicy),
    Q2;
set(X = #exchange{name = Name}) ->
    X#exchange{policy = match(Name), operator_policy = match_op(Name)}.

match(Name = #resource{virtual_host = VHost}) ->
    match(Name, list(VHost)).

match_op(Name = #resource{virtual_host = VHost}) ->
    match(Name, list_op(VHost)).

get(Name, Q) when ?is_amqqueue(Q) ->
    Policy = amqqueue:get_policy(Q),
    OpPolicy = amqqueue:get_operator_policy(Q),
    get0(Name, Policy, OpPolicy);
get(Name, #exchange{policy = Policy, operator_policy = OpPolicy}) ->
    get0(Name, Policy, OpPolicy);

%% Caution - SLOW.
get(Name, EntityName = #resource{virtual_host = VHost}) ->
    get0(Name,
         match(EntityName, list(VHost)),
         match(EntityName, list_op(VHost))).

get0(_Name, undefined, undefined) -> undefined;
get0(Name, undefined, OpPolicy) -> pget(Name, pget(definition, OpPolicy, []));
get0(Name, Policy, undefined) -> pget(Name, pget(definition, Policy, []));
get0(Name, Policy, OpPolicy) ->
    OpDefinition = pget(definition, OpPolicy, []),
    Definition = pget(definition, Policy, []),
    case {pget(Name, Definition), pget(Name, OpDefinition)} of
        {undefined, undefined} -> undefined;
        {Val, undefined}       -> Val;
        {undefined, Val}       -> Val;
        {Val, OpVal}           -> merge_policy_value(Name, Val, OpVal)
    end.

merge_policy_value(Name, PolicyVal, OpVal) ->
    case policy_merge_strategy(Name) of
        {ok, Module}       -> Module:merge_policy_value(Name, PolicyVal, OpVal);
        {error, not_found} -> rabbit_policies:merge_policy_value(Name, PolicyVal, OpVal)
    end.

policy_merge_strategy(Name) ->
    case rabbit_registry:binary_to_type(rabbit_data_coercion:to_binary(Name)) of
        {error, not_found} ->
            {error, not_found};
        T                  ->
            rabbit_registry:lookup_module(policy_merge_strategy, T)
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
    Qs = rabbit_amqqueue:list_with_possible_retry(
           fun() ->
                   mnesia:dirty_match_object(
                     rabbit_durable_queue, amqqueue:pattern_match_all())
           end),
    Policies = list(),
    OpPolicies = list_op(),
    [rabbit_misc:execute_mnesia_transaction(
       fun () ->
               mnesia:write(
                 rabbit_durable_exchange,
                 rabbit_exchange_decorator:set(
                   X#exchange{policy = match(Name, Policies),
                              operator_policy = match(Name, OpPolicies)}),
                 write)
       end) || X = #exchange{name = Name} <- Xs],
    [begin
         QName = amqqueue:get_name(Q0),
         Policy1 = match(QName, Policies),
         Q1 = amqqueue:set_policy(Q0, Policy1),
         OpPolicy1 = match(QName, OpPolicies),
         Q2 = amqqueue:set_operator_policy(Q1, OpPolicy1),
         Q3 = rabbit_queue_decorator:set(Q2),
         ?try_mnesia_tx_or_upgrade_amqqueue_and_retry(
            rabbit_misc:execute_mnesia_transaction(
              fun () ->
                      mnesia:write(rabbit_durable_queue, Q3, write)
              end),
            begin
                Q4 = amqqueue:upgrade(Q3),
                rabbit_misc:execute_mnesia_transaction(
                  fun () ->
                          mnesia:write(rabbit_durable_queue, Q4, write)
                  end)
            end)
     end || Q0 <- Qs],
    ok.

invalid_file() ->
    filename:join(rabbit_mnesia:dir(), "policies_are_invalid").

%%----------------------------------------------------------------------------

parse_set_op(VHost, Name, Pattern, Definition, Priority, ApplyTo, ActingUser) ->
    parse_set(<<"operator_policy">>, VHost, Name, Pattern, Definition, Priority,
              ApplyTo, ActingUser).

parse_set(VHost, Name, Pattern, Definition, Priority, ApplyTo, ActingUser) ->
    parse_set(<<"policy">>, VHost, Name, Pattern, Definition, Priority, ApplyTo,
              ActingUser).

parse_set(Type, VHost, Name, Pattern, Definition, Priority, ApplyTo, ActingUser) ->
    try rabbit_data_coercion:to_integer(Priority) of
        Num -> parse_set0(Type, VHost, Name, Pattern, Definition, Num, ApplyTo,
                          ActingUser)
    catch
        error:badarg -> {error, "~p priority must be a number", [Priority]}
    end.

parse_set0(Type, VHost, Name, Pattern, Defn, Priority, ApplyTo, ActingUser) ->
    case rabbit_json:try_decode(Defn) of
        {ok, Term} ->
            R = set0(Type, VHost, Name,
                     [{<<"pattern">>,    Pattern},
                      {<<"definition">>, maps:to_list(Term)},
                      {<<"priority">>,   Priority},
                      {<<"apply-to">>,   ApplyTo}],
                     ActingUser),
            _ = rabbit_log:info("Successfully set policy '~s' matching ~s names in virtual host '~s' using pattern '~s'",
                            [Name, ApplyTo, VHost, Pattern]),
            R;
        {error, Reason} ->
            {error_string,
                rabbit_misc:format("JSON decoding error. Reason: ~ts", [Reason])}
    end.

set_op(VHost, Name, Pattern, Definition, Priority, ApplyTo, ActingUser) ->
    set(<<"operator_policy">>, VHost, Name, Pattern, Definition, Priority, ApplyTo, ActingUser).

set(VHost, Name, Pattern, Definition, Priority, ApplyTo, ActingUser) ->
    set(<<"policy">>, VHost, Name, Pattern, Definition, Priority, ApplyTo, ActingUser).

set(Type, VHost, Name, Pattern, Definition, Priority, ApplyTo, ActingUser) ->
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
    set0(Type, VHost, Name, PolicyProps, ActingUser).

set0(Type, VHost, Name, Term, ActingUser) ->
    rabbit_runtime_parameters:set_any(VHost, Type, Name, Term, ActingUser).

delete_op(VHost, Name, ActingUser) ->
    rabbit_runtime_parameters:clear_any(VHost, <<"operator_policy">>, Name, ActingUser).

delete(VHost, Name, ActingUser) ->
    rabbit_runtime_parameters:clear_any(VHost, <<"policy">>, Name, ActingUser).

lookup_op(VHost, Name) ->
    case rabbit_runtime_parameters:lookup(VHost, <<"operator_policy">>, Name) of
        not_found  -> not_found;
        P          -> p(P, fun ident/1)
    end.

lookup(VHost, Name) ->
    case rabbit_runtime_parameters:lookup(VHost, <<"policy">>, Name) of
        not_found  -> not_found;
        P          -> p(P, fun ident/1)
    end.

list_op() ->
    list_op('_').

list_op(VHost) ->
    list0_op(VHost, fun ident/1).

list_formatted_op(VHost) ->
    order_policies(list0_op(VHost, fun rabbit_json:encode/1)).

list_formatted_op(VHost, Ref, AggregatorPid) ->
    rabbit_control_misc:emitting_map(AggregatorPid, Ref,
                                     fun(P) -> P end, list_formatted_op(VHost)).

list0_op(VHost, DefnFun) ->
    [p(P, DefnFun)
     || P <- rabbit_runtime_parameters:list(VHost, <<"operator_policy">>)].


list() ->
    list('_').

list(VHost) ->
    list0(VHost, fun ident/1).

list_formatted(VHost) ->
    order_policies(list0(VHost, fun rabbit_json:encode/1)).

list_formatted(VHost, Ref, AggregatorPid) ->
    rabbit_control_misc:emitting_map(AggregatorPid, Ref,
                                     fun(P) -> P end, list_formatted(VHost)).

list0(VHost, DefnFun) ->
    [p(P, DefnFun) || P <- rabbit_runtime_parameters:list(VHost, <<"policy">>)].

order_policies(PropList) ->
    lists:sort(fun (A, B) -> not sort_pred(A, B) end, PropList).

p(Parameter, DefnFun) ->
    Value = pget(value, Parameter),
    [{vhost,      pget(vhost, Parameter)},
     {name,       pget(name, Parameter)},
     {pattern,    pget(<<"pattern">>, Value)},
     {'apply-to', pget(<<"apply-to">>, Value)},
     {definition, DefnFun(pget(<<"definition">>, Value))},
     {priority,   pget(<<"priority">>, Value)}].

ident(X) -> X.

info_keys() -> [vhost, name, 'apply-to', pattern, definition, priority].

%%----------------------------------------------------------------------------

validate(_VHost, <<"policy">>, Name, Term, _User) ->
    rabbit_parameter_validation:proplist(
      Name, policy_validation(), Term);
validate(_VHost, <<"operator_policy">>, Name, Term, _User) ->
    rabbit_parameter_validation:proplist(
      Name, operator_policy_validation(), Term).

notify(VHost, <<"policy">>, Name, Term, ActingUser) ->
    rabbit_event:notify(policy_set, [{name, Name}, {vhost, VHost},
                                     {user_who_performed_action, ActingUser} | Term]),
    update_policies(VHost);
notify(VHost, <<"operator_policy">>, Name, Term, ActingUser) ->
    rabbit_event:notify(policy_set, [{name, Name}, {vhost, VHost},
                                     {user_who_performed_action, ActingUser} | Term]),
    update_policies(VHost).

notify_clear(VHost, <<"policy">>, Name, ActingUser) ->
    rabbit_event:notify(policy_cleared, [{name, Name}, {vhost, VHost},
                                         {user_who_performed_action, ActingUser}]),
    update_policies(VHost);
notify_clear(VHost, <<"operator_policy">>, Name, ActingUser) ->
    rabbit_event:notify(operator_policy_cleared,
                        [{name, Name}, {vhost, VHost},
                         {user_who_performed_action, ActingUser}]),
    update_policies(VHost).

%%----------------------------------------------------------------------------

%% [1] We need to prevent this from becoming O(n^2) in a similar
%% manner to rabbit_binding:remove_for_{source,destination}. So see
%% the comment in rabbit_binding:lock_route_tables/0 for more rationale.
%% [2] We could be here in a post-tx fun after the vhost has been
%% deleted; in which case it's fine to do nothing.
update_policies(VHost) ->
    Tabs = [rabbit_queue,    rabbit_durable_queue,
            rabbit_exchange, rabbit_durable_exchange],
    {Xs, Qs} = rabbit_misc:execute_mnesia_transaction(
        fun() ->
            [mnesia:lock({table, T}, write) || T <- Tabs], %% [1]
            case catch {list(VHost), list_op(VHost)} of
                {'EXIT', {throw, {error, {no_such_vhost, _}}}} ->
                    {[], []}; %% [2]
                {'EXIT', Exit} ->
                    exit(Exit);
                {Policies, OpPolicies} ->
                    {[update_exchange(X, Policies, OpPolicies) ||
                        X <- rabbit_exchange:list(VHost)],
                    [update_queue(Q, Policies, OpPolicies) ||
                        Q <- rabbit_amqqueue:list(VHost)]}
                end
        end),
    [catch notify(X) || X <- Xs],
    [catch notify(Q) || Q <- Qs],
    ok.

update_exchange(X = #exchange{name = XName,
                              policy = OldPolicy,
                              operator_policy = OldOpPolicy},
                Policies, OpPolicies) ->
    case {match(XName, Policies), match(XName, OpPolicies)} of
        {OldPolicy, OldOpPolicy} -> no_change;
        {NewPolicy, NewOpPolicy} ->
            NewExchange = rabbit_exchange:update(
                XName,
                fun(X0) ->
                    rabbit_exchange_decorator:set(
                        X0 #exchange{policy = NewPolicy,
                                     operator_policy = NewOpPolicy})
                    end),
            case NewExchange of
                #exchange{} = X1 -> {X, X1};
                not_found        -> {X, X }
            end
    end.

update_queue(Q0, Policies, OpPolicies) when ?is_amqqueue(Q0) ->
    QName = amqqueue:get_name(Q0),
    OldPolicy = amqqueue:get_policy(Q0),
    OldOpPolicy = amqqueue:get_operator_policy(Q0),
    case {match(QName, Policies), match(QName, OpPolicies)} of
        {OldPolicy, OldOpPolicy} -> no_change;
        {NewPolicy, NewOpPolicy} ->
            F = fun (QFun0) ->
                    QFun1 = amqqueue:set_policy(QFun0, NewPolicy),
                    QFun2 = amqqueue:set_operator_policy(QFun1, NewOpPolicy),
                    NewPolicyVersion = amqqueue:get_policy_version(QFun2) + 1,
                    QFun3 = amqqueue:set_policy_version(QFun2, NewPolicyVersion),
                    rabbit_queue_decorator:set(QFun3)
                end,
            NewQueue = rabbit_amqqueue:update(QName, F),
            case NewQueue of
                 Q1 when ?is_amqqueue(Q1) ->
                    {Q0, Q1};
                 not_found ->
                    {Q0, Q0}
             end
    end.

notify(no_change)->
    ok;
notify({X1 = #exchange{}, X2 = #exchange{}}) ->
    rabbit_exchange:policy_changed(X1, X2);
notify({Q1, Q2}) when ?is_amqqueue(Q1), ?is_amqqueue(Q2) ->
    rabbit_amqqueue:policy_changed(Q1, Q2).

match(Name, Policies) ->
    case match_all(Name, Policies) of
        []           -> undefined;
        [Policy | _] -> Policy
    end.

match_all(Name, Policies) ->
   lists:sort(fun sort_pred/2, [P || P <- Policies, matches(Name, P)]).

matches(#resource{name = Name, kind = Kind, virtual_host = VHost} = Resource, Policy) ->
    matches_type(Kind, pget('apply-to', Policy)) andalso
        is_applicable(Resource, pget(definition, Policy)) andalso
        match =:= re:run(Name, pget(pattern, Policy), [{capture, none}]) andalso
        VHost =:= pget(vhost, Policy).

matches_type(exchange, <<"exchanges">>) -> true;
matches_type(queue,    <<"queues">>)    -> true;
matches_type(exchange, <<"all">>)       -> true;
matches_type(queue,    <<"all">>)       -> true;
matches_type(_,        _)               -> false.

sort_pred(A, B) -> pget(priority, A) >= pget(priority, B).

is_applicable(#resource{kind = queue} = Resource, Policy) ->
    rabbit_amqqueue:is_policy_applicable(Resource, Policy);
is_applicable(_, _) ->
    true.

%%----------------------------------------------------------------------------

operator_policy_validation() ->
    [{<<"priority">>,   fun rabbit_parameter_validation:number/2, mandatory},
     {<<"pattern">>,    fun rabbit_parameter_validation:regex/2,  mandatory},
     {<<"apply-to">>,   fun apply_to_validation/2,                optional},
     {<<"definition">>, fun validation_op/2,                      mandatory}].

policy_validation() ->
    [{<<"priority">>,   fun rabbit_parameter_validation:number/2, mandatory},
     {<<"pattern">>,    fun rabbit_parameter_validation:regex/2,  mandatory},
     {<<"apply-to">>,   fun apply_to_validation/2,                optional},
     {<<"definition">>, fun validation/2,                         mandatory}].

validation_op(Name, Terms) ->
    validation(Name, Terms, operator_policy_validator).

validation(Name, Terms) ->
    validation(Name, Terms, policy_validator).

validation(_Name, [], _Validator) ->
    {error, "no policy provided", []};
validation(Name, Terms0, Validator) when is_map(Terms0) ->
    Terms = maps:to_list(Terms0),
    validation(Name, Terms, Validator);
validation(_Name, Terms, Validator) when is_list(Terms) ->
    {Keys, Modules} = lists:unzip(
                        rabbit_registry:lookup_all(Validator)),
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
validation(Name, Term, Validator) ->
    {error, "parse error while reading policy ~s: ~p. Validator: ~p.",
     [Name, Term, Validator]}.

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
