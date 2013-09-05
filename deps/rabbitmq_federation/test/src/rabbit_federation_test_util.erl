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
%% The Original Code is RabbitMQ Federation.
%%
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2007-2013 VMware, Inc.  All rights reserved.
%%

-module(rabbit_federation_test_util).

-include("rabbit_federation.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-compile(export_all).

-import(rabbit_misc, [pget/2]).

expect(Ch, Q, Fun) when is_function(Fun) ->
    amqp_channel:subscribe(Ch, #'basic.consume'{queue  = Q,
                                                no_ack = true}, self()),
    receive
        #'basic.consume_ok'{consumer_tag = CTag} -> ok
    end,
    Fun(),
    amqp_channel:call(Ch, #'basic.cancel'{consumer_tag = CTag});

expect(Ch, Q, Payloads) ->
    expect(Ch, Q, fun() -> expect(Payloads) end).

expect([]) ->
    ok;
expect(Payloads) ->
    receive
        {#'basic.deliver'{}, #amqp_msg{payload = Payload}} ->
            case lists:member(Payload, Payloads) of
                true  -> expect(Payloads -- [Payload]);
                false -> throw({expected, Payloads, actual, Payload})
            end
    end.

expect_empty(Ch, Q) ->
    ?assertMatch(#'basic.get_empty'{},
                 amqp_channel:call(Ch, #'basic.get'{ queue = Q })).

set_param(Component, Name, Value) ->
    rabbitmqctl(fmt("set_parameter ~s ~s '~s'", [Component, Name, Value])).

clear_param(Component, Name) ->
    rabbitmqctl(fmt("clear_parameter ~s ~s", [Component, Name])).

set_pol(Name, Pattern, Defn) ->
    rabbitmqctl(fmt("set_policy ~s \"~s\" '~s'", [Name, Pattern, Defn])).

clear_pol(Name) ->
    rabbitmqctl(fmt("clear_policy ~s ", [Name])).

fmt(Fmt, Args) ->
    string:join(string:tokens(rabbit_misc:format(Fmt, Args), [$\n]), " ").

start_other_node({Name, Port}) ->
    start_other_node({Name, Port}, Name).

start_other_node({Name, Port}, Config) ->
    start_other_node({Name, Port}, Config,
                     os:getenv("RABBITMQ_ENABLED_PLUGINS_FILE")).

start_other_node({Name, Port}, Config, PluginsFile) ->
    %% ?assertCmd seems to hang if you background anything. Bah!
    Res = os:cmd("make -C " ++ plugin_dir() ++ " OTHER_NODE=" ++ Name ++
                     " OTHER_PORT=" ++ integer_to_list(Port) ++
                     " OTHER_CONFIG=" ++ Config ++
                     " OTHER_PLUGINS=" ++ PluginsFile ++
                     " start-other-node ; echo $?"),
    case lists:reverse(string:tokens(Res, "\n")) of
        ["0" | _] -> ok;
        _         -> exit(broker_start_failed, Res)
    end,
    {ok, Conn} = amqp_connection:start(#amqp_params_network{port = Port}),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    Ch.

stop_other_node({Name, _Port}) ->
    ?assertCmd("make -C " ++ plugin_dir() ++ " OTHER_NODE=" ++ Name ++
                      " stop-other-node"),
    timer:sleep(1000).

rabbitmqctl(Args) ->
    ?assertCmd(
       plugin_dir() ++ "/../rabbitmq-server/scripts/rabbitmqctl " ++ Args),
    timer:sleep(100).

policy(UpstreamSet) ->
    rabbit_misc:format("{\"federation-upstream-set\": \"~s\"}", [UpstreamSet]).

plugin_dir() ->
    {ok, [[File]]} = init:get_argument(config),
    filename:dirname(filename:dirname(File)).

%%----------------------------------------------------------------------------

assert_status(XorQs) ->
    Links = lists:append([links(XorQ) || XorQ <- XorQs]),
    Remaining = lists:foldl(fun assert_link_status/2,
                            rabbit_federation_status:status(), Links),
    ?assertEqual([], Remaining),
    ok.

assert_link_status({DXNameBin, ConnectionName, UXNameBin}, Status) ->
    {This, Rest} = lists:partition(
                     fun(St) ->
                             pget(connection, St) =:= ConnectionName andalso
                                 pget(name, St) =:= DXNameBin andalso
                                 pget(upstream_name, St) =:= UXNameBin
                     end, Status),
    ?assertMatch([_], This),
    Rest.

links(#'exchange.declare'{exchange = Name}) ->
    case rabbit_policy:get(<<"federation-upstream-set">>, xr(Name)) of
        undefined -> [];
        Set       -> X = #exchange{name = xr(Name)},
                     [{Name, U#upstream.name, U#upstream.exchange_name} ||
                         U <- rabbit_federation_upstream:from_set(Set, X)]
    end;
links(#'queue.declare'{queue = Name}) ->
    case rabbit_policy:get(<<"federation-upstream-set">>, qr(Name)) of
        undefined -> [];
        Set       -> Q = #amqqueue{name = qr(Name)},
                     [{Name, U#upstream.name, U#upstream.queue_name} ||
                         U <- rabbit_federation_upstream:from_set(Set, Q)]
    end.

xr(Name) -> rabbit_misc:r(<<"/">>, exchange, Name).
qr(Name) -> rabbit_misc:r(<<"/">>, queue, Name).
