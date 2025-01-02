%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
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
-export([name/1, name_op/1, effective_definition/1, merge_operator_definitions/2, get/2, get_arg/3, set/1]).
-export([validate/5, notify/5, notify_clear/4]).
-export([parse_set/7, set/7, delete/3, lookup/2, list/0, list/1,
         list_formatted/1, list_formatted/3, info_keys/0]).
-export([parse_set_op/7, set_op/7, delete_op/3, lookup_op/2, list_op/0, list_op/1, list_op/2,
         list_formatted_op/1, list_formatted_op/3,
         match_all/1, match_all/2, match_as_map/1, match_op_as_map/1, definition_keys/1,
         list_in/1, list_in/2, list_as_maps/0, list_as_maps/1, list_op_as_maps/0, list_op_as_maps/1
        ]).
-export([sort_by_priority/1]).

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
    %% On queue.declare the queue record doesn't exist yet, so the later lookup in
    %% `rabbit_amqqueue:is_policy_applicable` fails. Thus, let's send the whole record
    %% through the match functions. `match_all` still needs to support resources only,
    %% as that is used by some plugins. Maybe `match` too, difficult to figure out
    %% what is public API and what is not, so let's support both `amqqueue` and `resource`
    %% records as arguments.
    Policy = match(Q0),
    OpPolicy = match_op(Q0),
    Q1 = amqqueue:set_policy(Q0, Policy),
    Q2 = amqqueue:set_operator_policy(Q1, OpPolicy),
    Q2;
set(X = #exchange{name = Name}) ->
    X#exchange{policy = match(Name), operator_policy = match_op(Name)}.


list() ->
    list('_').

list(VHost) ->
    list0(VHost, fun ident/1).

list_in(VHost) ->
    list(VHost).

list_in(VHost, DefinitionKeys) ->
    [P || P <- list_in(VHost), keys_overlap(definition_keys(P), DefinitionKeys)].

list_as_maps() ->
    list_as_maps('_').

list_as_maps(VHost) ->
    [maps:from_list(PL) || PL <- sort_by_priority(list0(VHost, fun maps:from_list/1))].

list_op_as_maps() ->
    list_op_as_maps('_').

list_op_as_maps(VHost) ->
    [maps:from_list(PL) || PL <- sort_by_priority(list0_op(VHost, fun maps:from_list/1))].

list_formatted(VHost) ->
    sort_by_priority(list0(VHost, fun rabbit_json:encode/1)).

list_formatted(VHost, Ref, AggregatorPid) ->
    rabbit_control_misc:emitting_map(AggregatorPid, Ref,
                                     fun(P) -> P end, list_formatted(VHost)).

list_op() ->
    list_op('_').

list_op(VHost) ->
    list0_op(VHost, fun ident/1).

list_op(VHost, DefinitionKeys) ->
    [P || P <- list_op(VHost), keys_overlap(definition_keys(P), DefinitionKeys)].

list_formatted_op(VHost) ->
    sort_by_priority(list0_op(VHost, fun rabbit_json:encode/1)).

list_formatted_op(VHost, Ref, AggregatorPid) ->
    rabbit_control_misc:emitting_map(AggregatorPid, Ref,
                                     fun(P) -> P end, list_formatted_op(VHost)).

match(Q) when ?is_amqqueue(Q) ->
    #resource{virtual_host = VHost} = amqqueue:get_name(Q),
    match(Q, list(VHost));
match(Name = #resource{virtual_host = VHost}) ->
    match(Name, list(VHost)).

match_op(Q) when ?is_amqqueue(Q) ->
    #resource{virtual_host = VHost} = amqqueue:get_name(Q),
    match(Q, list_op(VHost));
match_op(Name = #resource{virtual_host = VHost}) ->
    match(Name, list_op(VHost)).

match_as_map(Name = #resource{virtual_host = VHost}) ->
    [maps:from_list(PL) || PL <- match(Name, list(VHost))].

match_op_as_map(Name = #resource{virtual_host = VHost}) ->
    [maps:from_list(PL) || PL <- match(Name, list_op(VHost))].

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

%% It's exported, so give it a default until all khepri transformation is sorted
match(NameOrQueue, Policies) ->
    match(NameOrQueue, Policies, is_policy_applicable).

match(NameOrQueue, Policies, Function) ->
    case match_all(NameOrQueue, Policies, Function) of
        []           -> undefined;
        [Policy | _] -> Policy
    end.

%% It's exported, so give it a default until all khepri transformation is sorted
match_all(NameOrQueue) ->
    match_all(NameOrQueue, list()).

match_all(NameOrQueue, Policies) ->
    match_all(NameOrQueue, Policies, is_policy_applicable).

match_all(NameOrQueue, Policies, Function) ->
   lists:sort(fun priority_comparator/2, [P || P <- Policies, matches(NameOrQueue, P, Function)]).

matches(Q, Policy, Function) when ?is_amqqueue(Q) ->
    #resource{name = Name, virtual_host = VHost} = amqqueue:get_name(Q),
    matches_queue_type(queue, amqqueue:get_type(Q), pget('apply-to', Policy)) andalso
        is_applicable(Q, pget(definition, Policy), Function) andalso
        match =:= re:run(Name, pget(pattern, Policy), [{capture, none}]) andalso
        VHost =:= pget(vhost, Policy);
matches(#resource{kind = queue} = Resource, Policy, Function) ->
    {ok, Q} = rabbit_amqqueue:lookup(Resource),
    matches(Q, Policy, Function);
matches(#resource{name = Name, kind = Kind, virtual_host = VHost} = Resource, Policy, Function) ->
    matches_type(Kind, pget('apply-to', Policy)) andalso
        is_applicable(Resource, pget(definition, Policy), Function) andalso
        match =:= re:run(Name, pget(pattern, Policy), [{capture, none}]) andalso
        VHost =:= pget(vhost, Policy).

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
        error:badarg -> {error, "~tp priority must be a number", [Priority]}
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
            rabbit_log:info("Successfully set policy '~ts' matching ~ts names in virtual host '~ts' using pattern '~ts'",
                            [Name, ApplyTo, VHost, Pattern]),
            R;
        {error, Reason} ->
            {error_string,
                rabbit_misc:format("Could not parse JSON document: ~tp", [Reason])}
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

list0_op(VHost, DefnFun) ->
    [p(P, DefnFun)
     || P <- rabbit_runtime_parameters:list(VHost, <<"operator_policy">>)].

list0(VHost, DefnFun) ->
    [p(P, DefnFun) || P <- rabbit_runtime_parameters:list(VHost, <<"policy">>)].

sort_by_priority(PropList) ->
    lists:sort(fun (A, B) -> not priority_comparator(A, B) end, PropList).

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

definition_keys(Policy) ->
    case rabbit_data_coercion:to_map(Policy) of
        #{definition := Def} ->
            maps:keys(rabbit_data_coercion:to_map(Def));
        _ -> []
    end.

keys_overlap(A, B) ->
    lists:any(fun(Item) -> lists:member(Item, B) end, A).

%%----------------------------------------------------------------------------

validate(_VHost, <<"policy">>, Name, Term, _User) ->
    rabbit_parameter_validation:proplist(
      Name, policy_validation(), Term);
validate(_VHost, <<"operator_policy">>, Name, Term, _User) ->
    rabbit_parameter_validation:proplist(
      Name, operator_policy_validation(), Term).

notify(VHost, <<"policy">>, Name, Term0, ActingUser) ->
    Term = rabbit_data_coercion:atomize_keys(Term0),
    update_matched_objects(VHost, Term, ActingUser),
    rabbit_quorum_queue_periodic_membership_reconciliation:policy_set(),
    rabbit_event:notify(policy_set, [{name, Name}, {vhost, VHost},
                                     {user_who_performed_action, ActingUser} | Term]);
notify(VHost, <<"operator_policy">>, Name, Term0, ActingUser) ->
    Term = rabbit_data_coercion:atomize_keys(Term0),
    update_matched_objects(VHost, Term, ActingUser),
    rabbit_quorum_queue_periodic_membership_reconciliation:policy_set(),
    rabbit_event:notify(policy_set, [{name, Name}, {vhost, VHost},
                                     {user_who_performed_action, ActingUser} | Term]).

notify_clear(VHost, <<"policy">>, Name, ActingUser) ->
    update_matched_objects(VHost, undefined, ActingUser),
    rabbit_event:notify(policy_cleared, [{name, Name}, {vhost, VHost},
                                         {user_who_performed_action, ActingUser}]);
notify_clear(VHost, <<"operator_policy">>, Name, ActingUser) ->
    update_matched_objects(VHost, undefined, ActingUser),
    rabbit_event:notify(operator_policy_cleared,
                        [{name, Name}, {vhost, VHost},
                         {user_who_performed_action, ActingUser}]).

%%----------------------------------------------------------------------------

%% [2] We could be here in a post-tx fun after the vhost has been
%% deleted; in which case it's fine to do nothing.
update_matched_objects(VHost, PolicyDef, ActingUser) ->
    {XUpdateResults, QUpdateResults} =
        case catch {list(VHost), list_op(VHost)} of
            {'EXIT', {throw, {error, {no_such_vhost, _}}}} ->
                {[], []}; %% [2]
            {'EXIT', Exit} ->
                exit(Exit);
            {Policies, OpPolicies} ->
                rabbit_db_policy:update(VHost,
                                        get_updated_exchange(Policies, OpPolicies),
                                        get_updated_queue(Policies, OpPolicies))
        end,
    _ = [catch maybe_notify_of_policy_change(XRes, PolicyDef, ActingUser) || XRes <- XUpdateResults],
    _ = [catch maybe_notify_of_policy_change(QRes, PolicyDef, ActingUser) || QRes <- QUpdateResults],
    ok.

get_updated_exchange(Policies, OpPolicies) ->
    fun(X = #exchange{name = XName,
                                   policy = OldPolicy,
                                   operator_policy = OldOpPolicy}) ->
            case {match(XName, Policies), match(XName, OpPolicies)} of
                {OldPolicy, OldOpPolicy} -> no_change;
                {NewPolicy, NewOpPolicy} ->
                    Decorators = rabbit_exchange_decorator:active(
                                   X#exchange{policy = NewPolicy,
                                              operator_policy = NewOpPolicy}),
                    #{
                      exchange => X,
                      update_function =>
                          fun(X0) ->
                                  X0#exchange{policy = NewPolicy,
                                              operator_policy = NewOpPolicy,
                                              decorators = Decorators}
                          end
                     }
            end
    end.

get_updated_queue(Policies, OpPolicies) ->
    fun(Q0) when ?is_amqqueue(Q0) ->
            OldPolicy = amqqueue:get_policy(Q0),
            OldOpPolicy = amqqueue:get_operator_policy(Q0),
            case {match(Q0, Policies), match(Q0, OpPolicies)} of
                {OldPolicy, OldOpPolicy} -> no_change;
                {NewPolicy, NewOpPolicy} ->
                    Q = amqqueue:set_operator_policy(amqqueue:set_policy(Q0, NewPolicy),
                                                     NewOpPolicy),
                    Decorators = rabbit_queue_decorator:active(Q),
                    #{
                      queue => Q0,
                      update_function => update_queue(NewPolicy, NewOpPolicy, Decorators)
                     }
            end
    end.

update_queue(Policy, OpPolicy, Decorators) ->
    fun (Queue0) ->
            Queue1 = amqqueue:set_policy(Queue0, Policy),
            Queue2 = amqqueue:set_operator_policy(Queue1, OpPolicy),
            PolicyVersion = amqqueue:get_policy_version(Queue2) + 1,
            Queue3 = amqqueue:set_policy_version(Queue2, PolicyVersion),
            amqqueue:set_decorators(Queue3, Decorators)
    end.

maybe_notify_of_policy_change({X1 = #exchange{}, X2 = #exchange{}}, _PolicyDef, _ActingUser) ->
    rabbit_exchange:policy_changed(X1, X2);
%% policy has been cleared
maybe_notify_of_policy_change({Q1, Q2}, undefined, ActingUser) when ?is_amqqueue(Q1), ?is_amqqueue(Q2) ->
    rabbit_event:notify(queue_policy_cleared, [
        {name, amqqueue:get_name(Q2)},
        {vhost, amqqueue:get_vhost(Q2)},
        {type, amqqueue:get_type(Q2)},
        {user_who_performed_action, ActingUser}
    ]),
    rabbit_amqqueue:policy_changed(Q1, Q2);
%% policy has been added or updated
maybe_notify_of_policy_change({Q1, Q2}, PolicyDef, ActingUser) when ?is_amqqueue(Q1), ?is_amqqueue(Q2) ->
    rabbit_event:notify(queue_policy_updated, [
        {name, amqqueue:get_name(Q2)},
        {vhost, amqqueue:get_vhost(Q2)},
        {type, amqqueue:get_type(Q2)},
        {user_who_performed_action, ActingUser} | PolicyDef
    ]),
    rabbit_amqqueue:policy_changed(Q1, Q2).

matches_type(exchange, <<"exchanges">>) -> true;
matches_type(exchange, <<"all">>)       -> true;
matches_type(_,        _)               -> false.

matches_queue_type(queue, _, <<"all">>)    -> true;
matches_queue_type(queue, _, <<"queues">>) -> true;
matches_queue_type(queue, rabbit_classic_queue, <<"classic_queues">>) -> true;
matches_queue_type(queue, rabbit_quorum_queue,  <<"quorum_queues">>)  -> true;
matches_queue_type(queue, rabbit_stream_queue,  <<"streams">>)        -> true;
matches_queue_type(queue, _, _) -> false.

priority_comparator(A, B) -> pget(priority, A) >= pget(priority, B).

is_applicable(Q, Policy, Function) when ?is_amqqueue(Q) ->
    rabbit_amqqueue:Function(Q, rabbit_data_coercion:to_list(Policy));
is_applicable(#resource{kind = queue} = Resource, Policy, Function) ->
    rabbit_amqqueue:Function(Resource, rabbit_data_coercion:to_list(Policy));
is_applicable(_, _, _) ->
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
                     Dup  -> {error, "~tp duplicate keys not allowed", [Dup]}
                 end;
        false -> {error, "definition must be a dictionary: ~tp", [Terms]}
    end;
validation(Name, Term, Validator) ->
    {error, "parse error while reading policy ~ts: ~tp. Validator: ~tp.",
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
             {error, "~tp are not recognised policy settings", [Unvalidated]};
         {Error, _} ->
             Error
    end.

a2b(A) -> list_to_binary(atom_to_list(A)).

dups(L) -> L -- lists:usort(L).

is_proplist(L) -> length(L) =:= length([I || I = {_, _} <- L]).

apply_to_validation(_Name, <<"all">>)       -> ok;
apply_to_validation(_Name, <<"exchanges">>) -> ok;
apply_to_validation(_Name, <<"queues">>)    -> ok;
apply_to_validation(_Name, <<"classic_queues">>)    -> ok;
apply_to_validation(_Name, <<"quorum_queues">>)    -> ok;
apply_to_validation(_Name, <<"streams">>)    -> ok;
apply_to_validation(_Name, Term) ->
    {error, "apply-to '~ts' unrecognised; should be one of: 'queues', 'classic_queues', "
     " 'quorum_queues', 'streams', 'exchanges', or 'all'", [Term]}.
