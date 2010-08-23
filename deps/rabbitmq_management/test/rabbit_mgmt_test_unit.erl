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
%%   The Original Code is RabbitMQ Management Console.
%%
%%   The Initial Developers of the Original Code are Rabbit Technologies Ltd.
%%
%%   Copyright (C) 2010 Rabbit Technologies Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%
-module(rabbit_mgmt_test_unit).

-include_lib("eunit/include/eunit.hrl").

-define(OK, 200).
-define(NO_CONTENT, 204).
-define(NOT_FOUND, 404).

rates_test() ->
    Previous = [{foo, 1}, {bar, 100}, {baz, 3}],
    PreviousTS = {0, 0, 0},
    New = [{foo, 2}, {bar, 200}, {bash, 100}, {baz, 3}],
    NewTS = {0, 10, 0},
    WithRates = rabbit_mgmt_db:rates(New, NewTS, Previous, PreviousTS,
                                     [foo, bar, bash]),
    equals(0.1, pget(foo_rate, WithRates)),
    equals(10, pget(bar_rate, WithRates)),
    undefined = pget(bash_rate, WithRates),
    undefined = pget(baz_rate, WithRates).

http_overview_test() ->
    %% Rather crude, but this req doesn't say much and at least this means it
    %% didn't blow up.
    [<<"0.0.0.0:5672">>] = pget(<<"bound_to">>, http_get("/overview", ?OK)).

%% This test is rather over-verbose as we're trying to test understanding of
%% Webmachine
http_vhosts_test() ->
    [<<"/">>] = rget("vhosts", http_get("/vhost", ?OK)),
    %% Create a new one
    http_put("/vhost/myvhost", "", ?NO_CONTENT),
    %% PUT should be idempotent
    http_put("/vhost/myvhost", "", ?NO_CONTENT),
    %% Check it's there
    [<<"/">>, <<"myvhost">>] = rget("vhosts", http_get("/vhost", ?OK)),
    %% Check individually
    <<"/">> = rget("vhost", http_get("/vhost/%2f", ?OK)),
    <<"myvhost">> = rget("vhost", http_get("/vhost/myvhost", ?OK)),
    %% Delete it
    http_delete("/vhost/myvhost", ?NO_CONTENT),
    %% It's not there
    http_get("/vhost/myvhost", ?NOT_FOUND),
    http_delete("/vhost/myvhost", ?NOT_FOUND).

%%---------------------------------------------------------------------------

http_get(Path, CodeExp) ->
    {ok, {{_HTTP, CodeExp, _}, _Headers, ResBody}} =
        req(get, Path, [auth_header()]),
    decode(CodeExp, ResBody).

http_put(Path, Body, CodeExp) ->
    {ok, {{_HTTP, CodeExp, _}, _Headers, ResBody}} =
        req(put, Path, [auth_header()], Body),
    decode(CodeExp, ResBody).

http_delete(Path, CodeExp) ->
    {ok, {{_HTTP, CodeExp, _}, _Headers, ResBody}} =
        req(delete, Path, [auth_header()]),
    decode(CodeExp, ResBody).

req(Type, Path, Headers) ->
    httpc:request(Type, {"http://localhost:55672/json" ++ Path, Headers},
                  [], []).

req(Type, Path, Headers, Body) ->
    httpc:request(Type, {"http://localhost:55672/json" ++ Path, Headers,
                         "application/json", Body},
                  [], []).

decode(Code, ResBody) ->
    case Code of
        ?OK -> {struct, Res} = mochijson2:decode(ResBody),
               Res;
        _   -> ok
    end.

auth_header() ->
    {"Authorization",
     "Basic " ++ binary_to_list(base64:encode("guest:guest"))}.

%%---------------------------------------------------------------------------

pget(K, L) ->
     proplists:get_value(K, L).

rget(K, L) ->
    pget(list_to_binary(K), L).

equals(F1, F2) ->
    true = (abs(F1 - F2) < 0.001).
