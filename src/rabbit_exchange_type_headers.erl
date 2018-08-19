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
%% Copyright (c) 2007-2017 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_exchange_type_headers).
-include_lib("stdlib/include/qlc.hrl").
-include("rabbit.hrl").
-include("rabbit_framing.hrl").

-behaviour(rabbit_exchange_type).

-export([description/0, serialise_events/0, route/2]).
-export([validate/1, validate_binding/2,
         create/2, delete/3, policy_changed/2, add_binding/3,
         remove_bindings/3, assert_args_equivalence/2]).
-export([info/1, info/2]).
-export([upgrade_headers_bindings/0]).

-rabbit_boot_step({?MODULE,
                   [{description, "exchange type headers"},
                    {mfa,         {rabbit_registry, register,
                                   [exchange, <<"headers">>, ?MODULE]}},
                    {requires,    rabbit_registry},
                    {enables,     kernel_ready}]}).

info(_X) -> [].
info(_X, _) -> [].

description() ->
    [{description, <<"AMQP headers exchange, as per the AMQP specification">>}].

serialise_events() -> false.

route(#exchange{name = Name},
      #delivery{message = #basic_message{content = Content}}) ->
    Headers = case (Content#content.properties)#'P_basic'.headers of
                  undefined -> [];
                  H         -> rabbit_misc:sort_field_table(H)
              end,
    CurrentBindings = case ets:lookup(rabbit_headers_bindings, Name) of
        [] -> [];
        [#headers_bindings{bindings = E}] -> E
    end,
    get_routes(Headers, CurrentBindings, ordsets:new()).

get_routes(_, [], ResDests) -> ordsets:to_list(ResDests);
get_routes(Headers, [ {BindingType, DAT, DAF, Args, _} | T ], ResDests) ->
    case headers_match(BindingType, Args, Headers) of
        true  -> get_routes(Headers, T, ordsets:union(DAT, ResDests));
        _ -> get_routes(Headers, T, ordsets:union(DAF, ResDests))
    end.

headers_match(all, Args, Headers) ->
    headers_match_all(Args, Headers);
headers_match(any, Args, Headers) ->
    headers_match_any(Args, Headers).


validate_binding(_X, #binding{args = Args}) ->
    case rabbit_misc:table_lookup(Args, <<"x-match">>) of
        {longstr, <<"all">>} -> validate_list_type_usage(all, Args);
        {longstr, <<"any">>} -> validate_list_type_usage(any, Args);
        {longstr, Other}     -> {error,
                                 {binding_invalid,
                                  "Invalid x-match field value ~p; "
                                  "expected all or any", [Other]}};
        {Type,    Other}     -> {error,
                                 {binding_invalid,
                                  "Invalid x-match field type ~p (value ~p); "
                                  "expected longstr", [Type, Other]}};
        undefined            -> validate_list_type_usage(all, Args)
    end.

%% We don't invalidate bindings having legacy headers keys of list "Type" because of backward compatibility..
%% Maybe we should ?
validate_list_type_usage(_BindingType, []) -> ok;
validate_list_type_usage(_BindingType, [ {<<"x-?<= ", _/binary>>, array, _} | _ ]) ->
    {error, {binding_invalid, "Invalid use of List type with <=, <, >= or > operators", []}};
validate_list_type_usage(_BindingType, [ {<<"x-?< ", _/binary>>, array, _} | _ ]) ->
    {error, {binding_invalid, "Invalid use of List type with <=, <, >= or > operators", []}};
validate_list_type_usage(_BindingType, [ {<<"x-?>= ", _/binary>>, array, _} | _ ]) ->
    {error, {binding_invalid, "Invalid use of List type with <=, <, >= or > operators", []}};
validate_list_type_usage(_BindingType, [ {<<"x-?> ", _/binary>>, array, _} | _ ]) ->
    {error, {binding_invalid, "Invalid use of List type with <=, <, >= or > operators", []}};
validate_list_type_usage(all, [ {<<"x-?= ", _/binary>>, array, _} | _ ]) ->
    {error, {binding_invalid, "Invalid use of List type with = operator with binding type 'all'", []}};
validate_list_type_usage(any, [ {<<"x-?!= ", _/binary>>, array, _} | _ ]) ->
    {error, {binding_invalid, "Invalid use of List type with != operator with binding type 'any'", []}};
validate_list_type_usage(BindingType, [ _ | Tail ]) ->
    validate_list_type_usage(BindingType, Tail).


%% [0] spec is vague on whether it can be omitted but in practice it's
%% useful to allow people to do this

parse_x_match({longstr, <<"all">>}) -> all;
parse_x_match({longstr, <<"any">>}) -> any;
parse_x_match(_)                    -> all. %% legacy; we didn't validate

%%
%% !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
%% REQUIRES BOTH PATTERN AND DATA TO BE SORTED ASCENDING BY KEY.
%% !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
%%

%% Binding type 'all' match

% No more match operator to check; return true
headers_match_all([], _) -> true;
% Purge nx op on no data as all these are true
headers_match_all([{_, nx, _} | BNext], []) ->
    headers_match_all(BNext, []);
% No more message header but still match operator to check; return false
headers_match_all(_, []) -> false;
% Current header key not in match operators; go next header with current match operator
headers_match_all(BCur = [{BK, _, _} | _], [{HK, _, _} | HNext])
    when BK > HK -> headers_match_all(BCur, HNext);
% Current binding key must not exist in data, go next binding
headers_match_all([{BK, nx, _} | BNext], HCur = [{HK, _, _} | _])
    when BK < HK -> headers_match_all(BNext, HCur);
% Current match operator does not exist in message; return false
headers_match_all([{BK, _, _} | _], [{HK, _, _} | _])
    when BK < HK -> false;
%
% From here, BK == HK (keys are the same)
%
% Current values must match and do match; ok go next
headers_match_all([{_, eq, BV} | BNext], [{_, _, HV} | HNext])
    when BV == HV -> headers_match_all(BNext, HNext);
% Current values must match but do not match; return false
headers_match_all([{_, eq, _} | _], _) -> false;
% Key must not exist, return false
headers_match_all([{_, nx, _} | _], _) -> false;
% Current header key must exist; ok go next
headers_match_all([{_, ex, _} | BNext], [ _ | HNext]) ->
    headers_match_all(BNext, HNext);
% <= < = != > >=
headers_match_all([{_, ne, BV} | BNext], HCur = [{_, _, HV} | _])
    when BV /= HV -> headers_match_all(BNext, HCur);
headers_match_all([{_, ne, _} | _], _) -> false;
headers_match_all([{_, gt, BV} | BNext], HCur = [{_, _, HV} | _])
    when HV > BV -> headers_match_all(BNext, HCur);
headers_match_all([{_, gt, _} | _], _) -> false;
headers_match_all([{_, ge, BV} | BNext], HCur = [{_, _, HV} | _])
    when HV >= BV -> headers_match_all(BNext, HCur);
headers_match_all([{_, ge, _} | _], _) -> false;
headers_match_all([{_, lt, BV} | BNext], HCur = [{_, _, HV} | _])
    when HV < BV -> headers_match_all(BNext, HCur);
headers_match_all([{_, lt, _} | _], _) -> false;
headers_match_all([{_, le, BV} | BNext], HCur = [{_, _, HV} | _])
    when HV =< BV -> headers_match_all(BNext, HCur);
headers_match_all([{_, le, _} | _], _) -> false.



%% Binding type 'any' match

% No more match operator to check; return false
headers_match_any([], _) -> false;
% On no data left, only nx operator can return true
headers_match_any([{_, nx, _} | _], []) -> true;
% No more message header but still match operator to check; return false
headers_match_any(_, []) -> false;
% Current header key not in match operators; go next header with current match operator
headers_match_any(BCur = [{BK, _, _} | _], [{HK, _, _} | HNext])
    when BK > HK -> headers_match_any(BCur, HNext);
% nx operator : current binding key must not exist in data, return true
headers_match_any([{BK, nx, _} | _], [{HK, _, _} | _])
    when BK < HK -> true;
% Current binding key does not exist in message; go next binding
headers_match_any([{BK, _, _} | BNext], HCur = [{HK, _, _} | _])
    when BK < HK -> headers_match_any(BNext, HCur);
%
% From here, BK == HK
%
% Current values must match and do match; return true
headers_match_any([{_, eq, BV} | _], [{_, _, HV} | _]) when BV == HV -> true;
% Current header key must exist; return true
headers_match_any([{_, ex, _} | _], _) -> true;
headers_match_any([{_, ne, BV} | _], [{_, _, HV} | _]) when HV /= BV -> true;
headers_match_any([{_, gt, BV} | _], [{_, _, HV} | _]) when HV > BV -> true;
headers_match_any([{_, ge, BV} | _], [{_, _, HV} | _]) when HV >= BV -> true;
headers_match_any([{_, lt, BV} | _], [{_, _, HV} | _]) when HV < BV -> true;
headers_match_any([{_, le, BV} | _], [{_, _, HV} | _]) when HV =< BV -> true;
% No match yet; go next
headers_match_any([_ | BNext], HCur) ->
    headers_match_any(BNext, HCur).


get_match_operators(BindingArgs) ->
    MatchOperators = get_match_operators(BindingArgs, []),
    rabbit_misc:sort_field_table(MatchOperators).

get_match_operators([], Result) -> Result;
%% It's not properly specified, but a "no value" in a
%% pattern field is supposed to mean simple presence of
%% the corresponding data field. I've interpreted that to
%% mean a type of "void" for the pattern field.
%
% Maybe should we consider instead a "no value" as beeing a real no value of type longstr ?
% In other words, from where does the "void" type appears ?
get_match_operators([ {K, void, _V} | T ], Res) ->
    get_match_operators (T, [ {K, ex, nil} | Res]);
% the new match operator is 'ex' (like in << must EXist >>)
get_match_operators([ {<<"x-?ex">>, longstr, K} | Tail ], Res) ->
    get_match_operators (Tail, [ {K, ex, nil} | Res]);
% operator "key not exist"
get_match_operators([ {<<"x-?nx">>, longstr, K} | Tail ], Res) ->
    get_match_operators (Tail, [ {K, nx, nil} | Res]);
% operators <= < = != > >=
get_match_operators([ {<<"x-?<= ", K/binary>>, _, V} | Tail ], Res) ->
    get_match_operators (Tail, [ {K, le, V} | Res]);
get_match_operators([ {<<"x-?< ", K/binary>>, _, V} | Tail ], Res) ->
    get_match_operators (Tail, [ {K, lt, V} | Res]);
get_match_operators([ {<<"x-?= ", K/binary>>, _, V} | Tail ], Res) ->
    get_match_operators (Tail, [ {K, eq, V} | Res]);
get_match_operators([ {<<"x-?!= ", K/binary>>, _, V} | Tail ], Res) ->
    get_match_operators (Tail, [ {K, ne, V} | Res]);
get_match_operators([ {<<"x-?> ", K/binary>>, _, V} | Tail ], Res) ->
    get_match_operators (Tail, [ {K, gt, V} | Res]);
get_match_operators([ {<<"x-?>= ", K/binary>>, _, V} | Tail ], Res) ->
    get_match_operators (Tail, [ {K, ge, V} | Res]);
% skip all x-* args..
get_match_operators([ {<<"x-", _/binary>>, _, _} | T ], Res) ->
    get_match_operators (T, Res);
% for all other cases, the match operator is 'eq'
get_match_operators([ {K, _, V} | T ], Res) ->
    get_match_operators (T, [ {K, eq, V} | Res]).


%% DAT : Destinations to Add on True
%% DAF : Destinations to Add on False
get_dests_operators(VHost, Args) ->
    get_dests_operators(VHost, Args, ordsets:new(), ordsets:new()).

get_dests_operators(_, [], DAT,DAF) -> {DAT,DAF};
get_dests_operators(VHost, [{<<"x-match-addq-ontrue">>, longstr, D} | T], DAT,DAF) ->
    R = rabbit_misc:r(VHost, queue, D),
    get_dests_operators(VHost, T, ordsets:add_element(R,DAT), DAF);
get_dests_operators(VHost, [{<<"x-match-adde-ontrue">>, longstr, D} | T], DAT,DAF) ->
    R = rabbit_misc:r(VHost, exchange, D),
    get_dests_operators(VHost, T, ordsets:add_element(R,DAT), DAF);
get_dests_operators(VHost, [{<<"x-match-addq-onfalse">>, longstr, D} | T], DAT,DAF) ->
    R = rabbit_misc:r(VHost, queue, D),
    get_dests_operators(VHost, T, DAT, ordsets:add_element(R,DAF));
get_dests_operators(VHost, [{<<"x-match-adde-onfalse">>, longstr, D} | T], DAT,DAF) ->
    R = rabbit_misc:r(VHost, exchange, D),
    get_dests_operators(VHost, T, DAT, ordsets:add_element(R,DAF));
get_dests_operators(VHost, [_ | T], DAT,DAF) ->
    get_dests_operators(VHost, T, DAT,DAF).


%% Flatten one level for list of values (array)
flatten_binding_args(Args) ->
        flatten_binding_args(Args, []).

flatten_binding_args([], Result) -> Result;
flatten_binding_args ([ {K, array, Vs} | Tail ], Result) ->
        Res = [ { K, T, V } || {T, V} <- Vs ],
        flatten_binding_args (Tail, lists:append ([ Res , Result ]));
flatten_binding_args ([ {K, T, V} | Tail ], Result) ->
        flatten_binding_args (Tail, [ {K, T, V} | Result ]).


validate(_X) -> ok.
create(_Tx, _X) -> ok.

delete(transaction, #exchange{name = XName}, _) ->
    ok = mnesia:delete (rabbit_headers_bindings, XName, write);
delete(_, _, _) -> ok.

policy_changed(_X1, _X2) -> ok.

add_binding(transaction, #exchange{name = #resource{virtual_host = VHost} = XName}, BindingToAdd = #binding{destination = Dest, args = BindingArgs}) ->
% BindingId is used to track original binding definition so that it is used when deleting later
    BindingId = crypto:hash(md5, term_to_binary(BindingToAdd)),
% Let's doing that heavy lookup one time only
    BindingType = parse_x_match(rabbit_misc:table_lookup(BindingArgs, <<"x-match">>)),
    FlattenedBindindArgs = flatten_binding_args(BindingArgs),
    MatchOperators = get_match_operators(FlattenedBindindArgs),
    {DAT, DAF} = get_dests_operators(VHost, FlattenedBindindArgs),
    CurrentBindings = case mnesia:read(rabbit_headers_bindings, XName, write) of
        [] -> [];
        [#headers_bindings{bindings = E}] -> E
    end,
    NewBinding = {BindingType, ordsets:add_element(Dest, DAT), DAF, MatchOperators, BindingId},
    NewBindings = [NewBinding | CurrentBindings],
    NewRecord = #headers_bindings{exchange_name = XName, bindings = NewBindings},
    ok = mnesia:write(rabbit_headers_bindings, NewRecord, write);
add_binding(_, _, _) ->
    ok.

remove_bindings(transaction, #exchange{name = XName}, BindingsToDelete) ->
    CurrentBindings = case mnesia:read(rabbit_headers_bindings, XName, write) of
        [] -> [];
        [#headers_bindings{bindings = E}] -> E
    end,
    BindingIdsToDelete = [crypto:hash(md5, term_to_binary(B)) || B <- BindingsToDelete],
    NewBindings = remove_bindings_ids(BindingIdsToDelete, CurrentBindings, []),
    NewRecord = #headers_bindings{exchange_name = XName, bindings = NewBindings},
    ok = mnesia:write(rabbit_headers_bindings, NewRecord, write);
remove_bindings(_, _, _) ->
    ok.

remove_bindings_ids(_, [], Res) -> Res;
remove_bindings_ids(BindingIdsToDelete, [Bind = {_,_,_,_,BId} | T], Res) ->
    case lists:member(BId, BindingIdsToDelete) of
        true -> remove_bindings_ids(BindingIdsToDelete, T, Res);
        _    -> remove_bindings_ids(BindingIdsToDelete, T, lists:append(Res, [Bind]))
    end.


assert_args_equivalence(X, Args) ->
    rabbit_exchange:assert_args_equivalence(X, Args).



upgrade_headers_bindings() ->
    MatchHeadersExchanges = #exchange{type = headers, name = '$1', _ = '_'},
    Transaction = fun() ->
    HeadersExchanges = mnesia:select(rabbit_exchange, [{MatchHeadersExchanges ,[],['$1']}]),
    Query = qlc:q([RR || RR <- mnesia:table(rabbit_route), lists:member(RR#route.binding#binding.source, HeadersExchanges)]),
    BindingsToUpgrade = qlc:eval(Query),
    UpgradeBindingFun = fun(BindingToUpgrade) ->
        Bind = BindingToUpgrade#route.binding,
        Exch = #exchange{name = Bind#binding.source },
        rabbit_exchange_type_headers:add_binding(transaction, Exch, Bind)
        end,
    lists:foreach (UpgradeBindingFun, BindingsToUpgrade)
    end,
    mnesia:transaction(Transaction).

