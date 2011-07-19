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
-module(rabbit_mgmt_test_clustering).

-include("rabbit_mgmt_test.hrl").

-export([start_second_node/0, stop_second_node/0]).

-import(rabbit_mgmt_test_http, [http_get/1]).

%%----------------------------------------------------------------------------

start_second_node() ->
    ?assertCmd("make -C " ++ plugin_dir() ++ " start-second-node").

stop_second_node() ->
    ?assertCmd("make -C " ++ plugin_dir() ++ " stop-second-node").

plugin_dir() ->
    {ok, [[File]]} = init:get_argument(config),
    filename:dirname(filename:dirname(File)).

%%----------------------------------------------------------------------------

cluster_nodes_test() ->
    ?assertEqual(2, length(http_get("/nodes"))),
    ok.
