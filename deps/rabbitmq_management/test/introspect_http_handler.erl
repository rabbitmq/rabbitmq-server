-module(introspect_http_handler).
-behavior(cowboy_handler).

-export([init/2, terminate/3]).

init(Req, State) ->
    ct:log("introspect_http_handler init : ~p", [Req]),
    case cowboy_req:read_urlencoded_body(Req) of
        {ok, KeyValues, _Req} -> 
            ct:log("introspect_http_handler responding with active token: ~p", [KeyValues]),
            case proplists:get_value(<<"token">>, KeyValues) of 
                undefined -> 
                    {ok, cowboy_req:reply(401, #{}, [], Req), State};
                <<"active">> -> 
                    Body = rabbit_json:encode([{"active", true}, {"scope", "rabbitmq.tag:administrator"}]),
                    {ok, cowboy_req:reply(200, #{<<"content-type">> => <<"application/json">>}, 
                        Body, Req), State};
                <<"inactive">> -> Body = rabbit_json:encode([{"active", false}, {"scope", "rabbitmq.tag:administrator"}]),
                    {ok, cowboy_req:reply(200, #{<<"content-type">> => <<"application/json">>}, 
                        Body, Req), State}
            end;
        Other -> 
            ct:log("introspect_http_handler responding with 401 : ~p", [Other]),
            {ok, cowboy_req:reply(401, #{}, [], Req), State}
    end.

terminate(_Reason, _Req, _State) ->
    ok.
