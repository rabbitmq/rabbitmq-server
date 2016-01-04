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
%% Copyright (c) 2007-2016 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_web_dispatch_test).

-include_lib("eunit/include/eunit.hrl").

%% Silence compiler warnings. These *are* used: callbacks for a Cowboy
%% test handler.
-export([init/3, handle/2, terminate/3]).

query_static_resource_test() ->
    application:start(inets),

    %% TODO this is a fairly rubbish test, but not as bad as it was
    rabbit_web_dispatch:register_static_context(test, [{port, 12345}],
                                                "rabbit_web_dispatch_test",
                                                ?MODULE, "test/priv/www", "Test"),
    {ok, {_Status, _Headers, Body}} =
        httpc:request("http://localhost:12345/rabbit_web_dispatch_test/index.html"),
    ?assert(string:str(Body, "RabbitMQ HTTP Server Test Page") /= 0).

add_idempotence_test() ->
    F = fun(_Req) -> ok end,
    L = {"/foo", "Foo"},
    rabbit_web_dispatch_registry:add(foo, [{port, 12345}], F, F, L),
    rabbit_web_dispatch_registry:add(foo, [{port, 12345}], F, F, L),
    ?assertEqual(
       1, length([ok || {"/foo", _, _} <-
                            rabbit_web_dispatch_registry:list_all()])),
    passed.

log_source_address_test() ->

    %% Given: everything in place to issue AND log an HTTP response.
    {ok, _} = rabbit_web_dispatch:register_context_handler(log_response_test,
        [{port, port()}], path(), table(), description()),
    ok = webmachine_log:add_handler(webmachine_log_handler, [log_directory()]),

    %% When: when a client makes a request.
    {ok, _Response} = httpc:request(get, {"http://127.0.0.1:" ++
        string(port()) ++ "/" ++ string(path()), []}, [],
        [{ip, string(source())}]),

    %% Then: log written WITH source IP address AND path.
    true = logged(source()),
    true = logged(path  ()).


%% Ancillary procedures for log test

%% Resource for testing with.
path() -> <<"/wonderland">>.

%% HTTP server port.
port() -> 4096.

%% Log files will be written here.
log_directory() ->
    "/tmp/rabbit/test/".

%% Source IP address of request.
source() -> <<"127.0.0.1">>.

description() -> "Test that source IP address is logged upon HTTP response.".

status() -> 500.

reason() -> <<"Testing, testing... 1, 2, 3.">>.

%% HTTP server forwarding table.
table() ->
    cowboy_router:compile([{source(), [{path(), ?MODULE, []}]}]).

%% Cowboy handler callbacks.
init(_, Request, _) ->
    {ok, Request, no_state}.

handle(Request, St) ->
    {ok, Response} = cowboy_req:reply(status(), [{<<"content-type">>,
        <<"text/plain">>}], reason(), Request),
    {ok, Response, St}.

terminate(_, _, _) ->
    ok.

%% Predicate: the given `Text` is read from file.
logged(Text) ->
    {ok, Handle} = file:open(log_directory() ++
        "access.log" ++ webmachine_log:suffix(webmachine_log:datehour()),
        [read, binary]),
    logged(Handle, Text).

logged(Handle, Text) ->
    case io:get_line(Handle, "") of
        eof ->
            file:close(Handle),
            false;
        Line when is_binary(Line) ->
            case binary:matches(Line, Text) of
                [] ->
                    logged(Handle, Text);
                [{N,_}] when is_integer(N), N >= 0 ->
                    true
            end
    end.

%% Convenience procedure.
string(N) when is_integer(N) ->
    erlang:integer_to_list(N);
string(B) when is_binary(B) ->
    erlang:binary_to_list(B).
