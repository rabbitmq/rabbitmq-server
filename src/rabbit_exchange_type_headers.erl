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
-include("rabbit.hrl").
-include("rabbit_framing.hrl").

-behaviour(rabbit_exchange_type).

-export([description/0, serialise_events/0, route/2]).
-export([validate/1, validate_binding/2,
         create/2, delete/3, policy_changed/2, add_binding/3,
         remove_bindings/3, assert_args_equivalence/2]).
-export([info/1, info/2]).

-rabbit_boot_step({?MODULE,
                   [{description, "exchange type headers"},
                    {mfa,         {rabbit_registry, register,
                                   [exchange, <<"headers">>, ?MODULE]}},
                    {requires,    rabbit_registry},
                    {enables,     kernel_ready}]}).

-spec headers_match
        (rabbit_framing:amqp_table(), rabbit_framing:amqp_table()) ->
            boolean().

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
    rabbit_router:match_bindings(
      Name, fun (#binding{args = Spec}) -> headers_match(Spec, Headers) end).

validate_binding(_X, #binding{args = Args}) ->
    case rabbit_misc:table_lookup(Args, <<"x-match">>) of
        {longstr, <<"all">>} -> ok;
        {longstr, <<"any">>} -> ok;
        {longstr, Other}     -> {error,
                                 {binding_invalid,
                                  "Invalid x-match field value ~p; "
                                  "expected all or any", [Other]}};
        {Type,    Other}     -> {error,
                                 {binding_invalid,
                                  "Invalid x-match field type ~p (value ~p); "
                                  "expected longstr", [Type, Other]}};
        undefined            -> ok %% [0]
    end.
%% [0] spec is vague on whether it can be omitted but in practice it's
%% useful to allow people to do this

parse_x_match({longstr, <<"all">>}) -> all;
parse_x_match({longstr, <<"any">>}) -> any;
parse_x_match(_)                    -> all. %% legacy; we didn't validate

%% Horrendous matching algorithm. Depends for its merge-like
%% (linear-time) behaviour on the lists:keysort
%% (rabbit_misc:sort_field_table) that route/1 and
%% rabbit_binding:{add,remove}/2 do.
%%
%%                 !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
%% In other words: REQUIRES BOTH PATTERN AND DATA TO BE SORTED ASCENDING BY KEY.
%%                 !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
%%
headers_match(Args, Data) ->
    MK = parse_x_match(rabbit_misc:table_lookup(Args, <<"x-match">>)),
    headers_match(Args, Data, true, false, MK).

% A bit less horrendous algorithm :)
headers_match(_, _, false, _, all) -> false;
headers_match(_, _, _, true, any) -> true;

% No more bindings, return current state
headers_match([], _Data, AllMatch, _AnyMatch, all) -> AllMatch;
headers_match([], _Data, _AllMatch, AnyMatch, any) -> AnyMatch;

% Delete bindings starting with x-
headers_match([{<<"x-", _/binary>>, _PT, _PV} | PRest], Data,
              AllMatch, AnyMatch, MatchKind) ->
    headers_match(PRest, Data, AllMatch, AnyMatch, MatchKind);

% No more data, but still bindings, false with all
headers_match(_Pattern, [], _AllMatch, AnyMatch, MatchKind) ->
    headers_match([], [], false, AnyMatch, MatchKind);

% Data key header not in binding, go next data
headers_match(Pattern = [{PK, _PT, _PV} | _], [{DK, _DT, _DV} | DRest],
              AllMatch, AnyMatch, MatchKind) when PK > DK ->
    headers_match(Pattern, DRest, AllMatch, AnyMatch, MatchKind);

% Binding key header not in data, false with all, go next binding
headers_match([{PK, _PT, _PV} | PRest], Data = [{DK, _DT, _DV} | _],
              _AllMatch, AnyMatch, MatchKind) when PK < DK ->
    headers_match(PRest, Data, false, AnyMatch, MatchKind);

get_match_operators(BindingArgs) ->
    MatchOperators = get_match_operators(BindingArgs, []),
    rabbit_misc:sort_field_table(MatchOperators).

get_match_operators([], Result) -> Result;
%% It's not properly specified, but a "no value" in a
%% pattern field is supposed to mean simple presence of
%% the corresponding data field. I've interpreted that to
%% mean a type of "void" for the pattern field.
headers_match([{PK, void, _PV} | PRest], [{DK, _DT, _DV} | DRest],
              AllMatch, _AnyMatch, MatchKind) when PK == DK ->
    headers_match(PRest, DRest, AllMatch, true, MatchKind);

% Complete match, true with any, go next
headers_match([{PK, _PT, PV} | PRest], [{DK, _DT, DV} | DRest],
              AllMatch, _AnyMatch, MatchKind) when PK == DK andalso PV == DV ->
    headers_match(PRest, DRest, AllMatch, true, MatchKind);

% Value does not match, false with all, go next
headers_match([{PK, _PT, _PV} | PRest], [{DK, _DT, _DV} | DRest],
              _AllMatch, AnyMatch, MatchKind) when PK == DK ->
    headers_match(PRest, DRest, false, AnyMatch, MatchKind).


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
