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

-module(rabbit_sharding_test_util).

-include("rabbit_sharding.hrl").
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
    execute("make -C " ++ plugin_dir() ++ " OTHER_NODE=" ++ Name ++
                " OTHER_PORT=" ++ integer_to_list(Port) ++
                " OTHER_CONFIG=" ++ Config ++
                " OTHER_PLUGINS=" ++ PluginsFile ++
                " start-other-node"),
    timer:sleep(1000).

stop_other_node({Name, _Port}) ->
    execute("make -C " ++ plugin_dir() ++ " OTHER_NODE=" ++ Name ++
                " stop-other-node"),
    timer:sleep(1000).

reset_other_node({Name, _Port}) ->
    execute("make -C " ++ plugin_dir() ++ " OTHER_NODE=" ++ Name ++
                " reset-other-node"),
    timer:sleep(1000).

cluster_other_node({Name, _Port}, {MainName, _Port2}) ->
    execute("make -C " ++ plugin_dir() ++ " OTHER_NODE=" ++ Name ++
                " MAIN_NODE=" ++ MainName ++
                " cluster-other-node"),
    timer:sleep(1000).

rabbitmqctl(Args) ->
    execute(plugin_dir() ++ "/../rabbitmq-server/scripts/rabbitmqctl " ++ Args),
    timer:sleep(100).

%% ?assertCmd seems to hang if you background anything. Bah!
execute(Cmd) ->
    Res = os:cmd(Cmd ++ " ; echo $?"),
    case lists:reverse(string:tokens(Res, "\n")) of
        ["0" | _] -> ok;
        _         -> exit({command_failed, Cmd, Res})
    end.

policy(ShardingDef) ->
    rabbit_misc:format("{\"sharding-definition\": \"~s\"}", [ShardingDef]).

plugin_dir() ->
    {ok, [[File]]} = init:get_argument(config),
    filename:dirname(filename:dirname(File)).

%%----------------------------------------------------------------------------

% assert_status(XorQs, Names) ->
%     Links = lists:append([links(XorQ) || XorQ <- XorQs]),
%     Remaining = lists:foldl(fun (Link, Status) ->
%                                     assert_link_status(Link, Status, Names)
%                             end, rabbit_federation_status:status(), Links),
%     ?assertEqual([], Remaining),
%     ok.

assert_link_status({DXorQNameBin, UpstreamName, UXorQNameBin}, Status,
                   {TypeName, UpstreamTypeName}) ->
    {This, Rest} = lists:partition(
                     fun(St) ->
                             pget(upstream, St) =:= UpstreamName andalso
                                 pget(TypeName, St) =:= DXorQNameBin andalso
                                 pget(UpstreamTypeName, St) =:= UXorQNameBin
                     end, Status),
    ?assertMatch([_], This),
    Rest.

xr(Name) -> rabbit_misc:r(<<"/">>, exchange, Name).
qr(Name) -> rabbit_misc:r(<<"/">>, queue, Name).
