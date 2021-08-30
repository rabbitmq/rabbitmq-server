%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_web_dispatch_SUITE).


-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

all() ->
    [
      {group, non_parallel_tests}
    ].

groups() ->
    [
      {non_parallel_tests, [], [
                                query_static_resource_test,
                                add_idempotence_test,
                                log_source_address_test,
                                parse_ip_test
                               ]}
    ].

%% -------------------------------------------------------------------
%% Test suite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodename_suffix, ?MODULE},
        {rmq_extra_tcp_ports, [tcp_port_http_extra]}
      ]),
    rabbit_ct_helpers:run_setup_steps(Config1,
      rabbit_ct_broker_helpers:setup_steps()).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config,
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_group(_, Config) ->
    Config.

end_per_group(_, Config) ->
    Config.

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% -------------------------------------------------------------------
%% Test cases.
%% -------------------------------------------------------------------

query_static_resource_test(Config) ->
    Host = rabbit_ct_helpers:get_config(Config, rmq_hostname),
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_http_extra),
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, query_static_resource_test1, [Host, Port]).
query_static_resource_test1(Host, Port) ->
    inets:start(),
    %% TODO this is a fairly rubbish test, but not as bad as it was
    rabbit_web_dispatch:register_static_context(test, [{port, Port}],
                                                "rabbit_web_dispatch_test",
                                                ?MODULE, "test/priv/www", "Test"),
    inets:start(),
    {ok, {_Status, _Headers, Body}} =
        httpc:request(format("http://~s:~w/rabbit_web_dispatch_test/index.html", [Host, Port])),
    ?assert(string:str(Body, "RabbitMQ HTTP Server Test Page") /= 0),

    passed.

add_idempotence_test(Config) ->
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_http_extra),
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, add_idempotence_test1, [Port]).
add_idempotence_test1(Port) ->
    inets:start(),
    F = fun(_Req) -> ok end,
    L = {"/foo", "Foo"},
    rabbit_web_dispatch_registry:add(foo, [{port, Port}], F, F, L),
    rabbit_web_dispatch_registry:add(foo, [{port, Port}], F, F, L),
    ?assertEqual(
       1, length([ok || {"/foo", _, _} <-
                            rabbit_web_dispatch_registry:list_all()])),
    passed.

parse_ip_test(Config) ->
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_http_extra),
    %% I have to Port + 1 here to have a free port not used by a listener.
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, parse_ip_test1, [Port + 1]).
parse_ip_test1(Port) ->
    F = fun(_Req) -> ok end,
    L = {"/parse_ip", "ParseIP"},
    rabbit_web_dispatch_registry:add(parse_ip, [{port, Port}, {ip, "127.0.0.1"}], F, F, L),
    ?assertEqual(
       1, length([ok || {"/parse_ip", _, _} <-
                            rabbit_web_dispatch_registry:list_all()])),
    passed.


log_source_address_test(Config) ->
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_http_extra),
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, log_source_address_test1, [Port]).

log_source_address_test1(Port) ->
    inets:start(),
    %% Given: everything in place to issue AND log an HTTP response.
    {ok, _} = rabbit_web_dispatch:register_context_handler(log_response_test,
        [{port, Port}], path(), table(), description()),
    ok = webmachine_log:add_handler(webmachine_log_handler, [log_directory()]),

    %% When: when a client makes a request.
    {ok, _Response} = httpc:request(get, {"http://127.0.0.1:" ++
        string(Port) ++ "/" ++ string(path()), []}, [],
        [{ip, string(source())}]),

    %% Then: log written WITH source IP address AND path.
    true = logged(source()),
    true = logged(path  ()),
    true = logged(binary(status())),

    passed.

%% Ancillary procedures for log test

%% Resource for testing with.
path() -> <<"wonderland">>.

%% HTTP server port.
port() -> 4096.

%% Log files will be written here.
log_directory() ->
    os:getenv("RABBITMQ_LOG_BASE") ++ "/".

%% Source IP address of request.
source() -> <<"127.0.0.1">>.

description() -> "Test that source IP address is logged upon HTTP response.".

status() -> 500.

reason() -> <<"Testing, testing... 1, 2, 3.">>.

%% HTTP server forwarding table.
table() ->
    cowboy_router:compile([{source(), [{"/" ++ string(path()), ?MODULE, []}]}]).

%% Cowboy handler callbacks.
init(Req, State) ->
    cowboy_req:reply(
        status(), #{<<"content-type">> => <<"text/plain">>}, reason(), Req),
    {ok, Req, State}.

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

%% Convenience procedures.
string(N) when is_integer(N) ->
    erlang:integer_to_list(N);
string(B) when is_binary(B) ->
    erlang:binary_to_list(B).

binary(N) when is_integer(N) ->
    erlang:integer_to_binary(N).

format(Fmt, Val) ->
    lists:flatten(io_lib:format(Fmt, Val)).
