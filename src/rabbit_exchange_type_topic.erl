-module(rabbit_exchange_type_topic).
-include("rabbit.hrl").

-behaviour(rabbit_exchange_behaviour).

-export([description/0, route/3]).
-export([recover/1, init/1, delete/1, add_binding/2, delete_binding/2]).

-export([topic_matches/2]).

-ifdef(use_specs).
-spec(topic_matches/2 :: (binary(), binary()) -> boolean()).
-endif.

description() ->
    [{name, <<"topic">>},
     {description, <<"AMQP topic exchange, as per the AMQP specification">>}].

route(#exchange{name = Name}, RoutingKey, _Content) ->
    rabbit_router:match_bindings(Name, fun (#binding{key = BindingKey}) ->
                                               topic_matches(BindingKey, RoutingKey)
                                       end).

split_topic_key(Key) ->
    {ok, KeySplit} = regexp:split(binary_to_list(Key), "\\."),
    KeySplit.

topic_matches(PatternKey, RoutingKey) ->
    P = split_topic_key(PatternKey),
    R = split_topic_key(RoutingKey),
    topic_matches1(P, R).

topic_matches1(["#"], _R) ->
    true;
topic_matches1(["#" | PTail], R) ->
    last_topic_match(PTail, [], lists:reverse(R));
topic_matches1([], []) ->
    true;
topic_matches1(["*" | PatRest], [_ | ValRest]) ->
    topic_matches1(PatRest, ValRest);
topic_matches1([PatElement | PatRest], [ValElement | ValRest]) when PatElement == ValElement ->
    topic_matches1(PatRest, ValRest);
topic_matches1(_, _) ->
    false.

last_topic_match(P, R, []) ->
    topic_matches1(P, R);
last_topic_match(P, R, [BacktrackNext | BacktrackList]) ->
    topic_matches1(P, R) or last_topic_match(P, [BacktrackNext | R], BacktrackList).

recover(_X) -> ok.
init(_X) -> ok.
delete(_X) -> ok.
add_binding(_X, _B) -> ok.
delete_binding(_X, _B) -> ok.
