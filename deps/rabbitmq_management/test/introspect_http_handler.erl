-module(introspect_http_handler).
-behavior(cowboy_handler).

-export([init/2, terminate/3]).

init(Req, State) ->
    case cowboy_req:parse_header(<<"authorization">>, Req) of
        {bearer, <<"active">>} -> 
            Body = rabbit_json:encode([{"active", true}, {"scope", "rabbitmq.tag:administrator"}]),
            {ok, cowboy_req:reply(200, #{<<"content-type">> => <<"application/json">>}, 
                Body, Req), State};
        {bearer, <<"inactive">>} -> 
            Body = rabbit_json:encode([{"active", false}, {"scope", "rabbitmq.tag:administrator"}]),
            {ok, cowboy_req:reply(200, #{<<"content-type">> => <<"application/json">>}, 
                Body, Req), State};
        _ -> 
            {ok, cowboy_req:reply(401, #{}, [], Req), State}
    end.

terminate(_Reason, _Req, _State) ->
    ok.
