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
%% Copyright (c) 2007-2013 VMware, Inc.  All rights reserved.
%%

-module(rabbit_exchange_type_headers).
-include("rabbit.hrl").
-include("rabbit_framing.hrl").

-behaviour(rabbit_exchange_type).

-export([description/0, serialise_events/0, route/2]).
-export([validate/1, validate_binding/2,
         create/2, delete/3, policy_changed/2, add_binding/3,
         remove_bindings/3, assert_args_equivalence/2]).

-rabbit_boot_step({?MODULE,
                   [{description, "exchange type headers"},
                    {mfa,         {rabbit_registry, register,
                                   [exchange, <<"headers">>, ?MODULE]}},
                    {requires,    rabbit_registry},
                    {enables,     kernel_ready}]}).

-ifdef(use_specs).
-spec(headers_match/2 :: (rabbit_framing:amqp_table(),
                          rabbit_framing:amqp_table()) -> boolean()).
-endif.

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

headers_match([], _Data, AllMatch, _AnyMatch, all) ->
    AllMatch;
headers_match([], _Data, _AllMatch, AnyMatch, any) ->
    AnyMatch;
headers_match([{<<"x-", _/binary>>, _PT, _PV} | PRest], Data,
              AllMatch, AnyMatch, MatchKind) ->
    headers_match(PRest, Data, AllMatch, AnyMatch, MatchKind);
headers_match(_Pattern, [], _AllMatch, AnyMatch, MatchKind) ->
    headers_match([], [], false, AnyMatch, MatchKind);
headers_match(Pattern = [{PK, _PT, _PV} | _], [{DK, _DT, _DV} | DRest],
              AllMatch, AnyMatch, MatchKind) when PK > DK ->
    headers_match(Pattern, DRest, AllMatch, AnyMatch, MatchKind);
headers_match([{PK, _PT, _PV} | PRest], Data = [{DK, _DT, _DV} | _],
              _AllMatch, AnyMatch, MatchKind) when PK < DK ->
    headers_match(PRest, Data, false, AnyMatch, MatchKind);
headers_match([{PK, PT, PV} | PRest], [{DK, DT, DV} | DRest],
              AllMatch, AnyMatch, MatchKind) when PK == DK ->
    {AllMatch1, AnyMatch1} =
        if
            %% It's not properly specified, but a "no value" in a
            %% pattern field is supposed to mean simple presence of
            %% the corresponding data field. I've interpreted that to
            %% mean a type of "void" for the pattern field.
            PT == void -> {AllMatch, true};
            %% Similarly, it's not specified, but I assume that a
            %% mismatched type causes a mismatched value.
            PT =/= DT  -> {false, AnyMatch};
            PV == DV   -> {AllMatch, true};
            true       -> {false, AnyMatch}
        end,
    headers_match(PRest, DRest, AllMatch1, AnyMatch1, MatchKind).

validate(_X) -> ok.
create(_Tx, _X) -> ok.
delete(_Tx, _X, _Bs) -> ok.
policy_changed(_X1, _X2) -> ok.
add_binding(_Tx, _X, _B) -> ok.
remove_bindings(_Tx, _X, _Bs) -> ok.
assert_args_equivalence(X, Args) ->
    rabbit_exchange:assert_args_equivalence(X, Args).
