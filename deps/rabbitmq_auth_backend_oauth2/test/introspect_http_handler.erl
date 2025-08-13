-module(introspect_http_handler).
-behavior(cowboy_handler).

-export([init/2, terminate/3]).

init(Req, State) ->
    ct:log("introspect_http_handler init : ~p", [Req]),
    case cowboy_req:read_urlencoded_body(Req) of
        {ok, KeyValues, _Req} -> 
            ct:log("introspect_http_handler responding with active token: ~p", [KeyValues]),
            case proplists:get_value(<<"token">>, KeyValues) of 
                <<"401">> -> 
                    {ok, cowboy_req:reply(401, #{}, [], Req), State};
                <<"active">> -> 
                    Body = rabbit_json:encode([
                        {"active", true}, 
                        {"sub", <<"test_case">>},
                        {"exp", os:system_time(seconds) + 30},
                        {"aud", <<"rabbitmq">>},
                        {"scope", <<"rabbitmq.configure:*/* rabbitmq.write:*/* rabbitmq.read:*/*">>}]),
                    {ok, cowboy_req:reply(200, #{<<"content-type">> => <<"application/json">>}, 
                        Body, Req), State};
                <<"active-2">> -> 
                    Body = rabbit_json:encode([
                        {"active", true}, 
                        {"sub", <<"test_case">>},
                        {"exp", os:system_time(seconds) + 30},
                        {"aud", <<"rabbitmq">>},
                        {"scope", <<"rabbitmq.write:*/* rabbitmq.read:*/*">>}]),
                    {ok, cowboy_req:reply(200, #{<<"content-type">> => <<"application/json">>}, 
                        Body, Req), State};
                <<"inactive">> -> 
                    Body = rabbit_json:encode([
                        {"active", false}, 
                        {"sub", <<"test_case">>},
                        {"exp", os:system_time(seconds) + 30},
                        {"scope", <<"rabbitmq.configure:*/* rabbitmq.write:*/* rabbitmq.read:*/*">>}]),
                    {ok, cowboy_req:reply(200, #{<<"content-type">> => <<"application/json">>}, 
                        Body, Req), State}
            end;
        Other -> 
            ct:log("introspect_http_handler responding with 401 : ~p", [Other]),
            {ok, cowboy_req:reply(401, #{}, [], Req), State}
    end.

terminate(_Reason, _Req, _State) ->
    ok.
