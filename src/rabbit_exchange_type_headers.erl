-module(rabbit_exchange_type_headers).
-include("rabbit.hrl").
-include("rabbit_framing.hrl").

-behaviour(rabbit_exchange_behaviour).

-export([description/0, route/3]).
-export([recover/1, init/1, delete/1, add_binding/2, delete_binding/2]).

-ifdef(use_specs).
-spec(headers_match/2 :: (amqp_table(), amqp_table()) -> boolean()).
-endif.

description() ->
    [{name, <<"headers">>},
     {description, <<"AMQP headers exchange, as per the AMQP specification">>}].

route(#exchange{name = Name}, _RoutingKey, Content) ->
    Headers = case (Content#content.properties)#'P_basic'.headers of
                  undefined -> [];
                  H         -> rabbit_misc:sort_arguments(H)
              end,
    rabbit_router:match_bindings(Name, fun (#binding{args = Spec}) ->
                                               headers_match(Spec, Headers)
                                       end).

default_headers_match_kind() -> all.

parse_x_match(<<"all">>) -> all;
parse_x_match(<<"any">>) -> any;
parse_x_match(Other) ->
    rabbit_log:warning("Invalid x-match field value ~p; expected all or any",
                       [Other]),
    default_headers_match_kind().

%% Horrendous matching algorithm. Depends for its merge-like
%% (linear-time) behaviour on the lists:keysort
%% (rabbit_misc:sort_arguments) that route/3 and
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

recover(_X) -> ok.
init(_X) -> ok.
delete(_X) -> ok.
add_binding(_X, _B) -> ok.
delete_binding(_X, _B) -> ok.
