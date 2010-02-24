%%   The contents of this file are subject to the Mozilla Public License
%%   Version 1.1 (the "License"); you may not use this file except in
%%   compliance with the License. You may obtain a copy of the License at
%%   http://www.mozilla.org/MPL/
%%
%%   Software distributed under the License is distributed on an "AS IS"
%%   basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%%   License for the specific language governing rights and limitations
%%   under the License.
%%
%%   The Original Code is RabbitMQ.
%%
%%   The Initial Developers of the Original Code are LShift Ltd,
%%   Cohesive Financial Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created before 22-Nov-2008 00:00:00 GMT by LShift Ltd,
%%   Cohesive Financial Technologies LLC, or Rabbit Technologies Ltd
%%   are Copyright (C) 2007-2008 LShift Ltd, Cohesive Financial
%%   Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created by LShift Ltd are Copyright (C) 2007-2009 LShift
%%   Ltd. Portions created by Cohesive Financial Technologies LLC are
%%   Copyright (C) 2007-2009 Cohesive Financial Technologies
%%   LLC. Portions created by Rabbit Technologies Ltd are Copyright
%%   (C) 2007-2009 Rabbit Technologies Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%

-module(rabbit_exchange_type_headers).
-include("rabbit.hrl").
-include("rabbit_framing.hrl").

-behaviour(rabbit_exchange_type).

-export([description/0, publish/2]).
-export([validate/1, create/1, recover/2, delete/2,
         add_binding/2, remove_bindings/2]).
-include("rabbit_exchange_type_spec.hrl").

-rabbit_boot_step({?MODULE,
                   [{description, "exchange type headers"},
                    {mfa,         {rabbit_exchange_type_registry, register,
                                   [<<"headers">>, ?MODULE]}},
                    {requires,    rabbit_exchange_type_registry},
                    {enables,     kernel_ready}]}).

-ifdef(use_specs).
-spec(headers_match/2 :: (amqp_table(), amqp_table()) -> boolean()).
-endif.

description() ->
    [{name, <<"headers">>},
     {description, <<"AMQP headers exchange, as per the AMQP specification">>}].

publish(#exchange{name = Name},
        Delivery = #delivery{message = #basic_message{content = Content}}) ->
    Headers = case (Content#content.properties)#'P_basic'.headers of
                  undefined -> [];
                  H         -> rabbit_misc:sort_field_table(H)
              end,
    rabbit_router:deliver(rabbit_router:match_bindings(
                            Name, fun (#binding{args = Spec}) ->
                                          headers_match(Spec, Headers)
                                  end),
                          Delivery).

default_headers_match_kind() -> all.

parse_x_match(<<"all">>) -> all;
parse_x_match(<<"any">>) -> any;
parse_x_match(Other) ->
    rabbit_log:warning("Invalid x-match field value ~p; expected all or any",
                       [Other]),
    default_headers_match_kind().

%% Horrendous matching algorithm. Depends for its merge-like
%% (linear-time) behaviour on the lists:keysort
%% (rabbit_misc:sort_field_table) that route/3 and
%% rabbit_exchange:{add,delete}_binding/4 do.
%%
%%                 !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
%% In other words: REQUIRES BOTH PATTERN AND DATA TO BE SORTED ASCENDING BY KEY.
%%                 !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
%%
headers_match(Pattern, Data) ->
    MatchKind = case lists:keysearch(<<"x-match">>, 1, Pattern) of
                    {value, {_, longstr, MK}} -> parse_x_match(MK);
                    {value, {_, Type, MK}} ->
                        rabbit_log:warning("Invalid x-match field type ~p "
                                           "(value ~p); expected longstr",
                                           [Type, MK]),
                        default_headers_match_kind();
                    _ -> default_headers_match_kind()
                end,
    headers_match(Pattern, Data, true, false, MatchKind).

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
create(_X) -> ok.
recover(_X, _Bs) -> ok.
delete(_X, _Bs) -> ok.
add_binding(_X, _B) -> ok.
remove_bindings(_X, _Bs) -> ok.
