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

-module(rabbit_shovel_test_dyn).

-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

simple_test() ->
    with_ch(
      fun (Ch) ->
              set_param("shovel", "test",
                        [{"src-uri",    "amqp://"},
                         {"src-queue",  "src"},
                         {"dest-uri",   "amqp://"},
                         {"dest-queue", "dest"}]),
              publish_expect(Ch, <<>>, <<"src">>, <<"dest">>, <<"hello">>),
              clear_param("shovel", "test"),
              publish_expect(Ch, <<>>, <<"src">>, <<"src">>, <<"hello">>),
              expect_empty(Ch, <<"dest">>)
      end).

%%----------------------------------------------------------------------------

with_ch(Fun) ->
    {ok, Conn} = amqp_connection:start(#amqp_params_network{}),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    Fun(Ch),
    amqp_connection:close(Conn),
    ok.

publish(Ch, X, Key, Payload) when is_binary(Payload) ->
    publish(Ch, X, Key, #amqp_msg{payload = Payload});

publish(Ch, X, Key, Msg = #amqp_msg{}) ->
    amqp_channel:cast(Ch, #'basic.publish'{exchange    = X,
                                           routing_key = Key}, Msg).

publish_expect(Ch, X, Key, Q, Payload) ->
    publish(Ch, X, Key, Payload),
    expect(Ch, Q, Payload).

expect(Ch, Q, Payload) ->
    amqp_channel:subscribe(Ch, #'basic.consume'{queue  = Q,
                                                no_ack = true}, self()),
    receive
        #'basic.consume_ok'{consumer_tag = CTag} -> ok
    end,
    receive
        {#'basic.deliver'{}, #amqp_msg{payload = Payload}} ->
            ok
    after 1000 ->
            exit({not_received, Payload})
    end,
    amqp_channel:call(Ch, #'basic.cancel'{consumer_tag = CTag}).

expect_empty(Ch, Q) ->
    ?assertMatch(#'basic.get_empty'{},
                 amqp_channel:call(Ch, #'basic.get'{ queue = Q })).

set_param(Component, Name, Value) ->
    rabbitmqctl(
      fmt("set_parameter ~s ~s '~s'", [Component, Name, json(Value)])).

clear_param(Component, Name) ->
    rabbitmqctl(fmt("clear_parameter ~s ~s", [Component, Name])).

fmt(Fmt, Args) ->
    string:join(string:tokens(rabbit_misc:format(Fmt, Args), [$\n]), " ").

rabbitmqctl(Args) ->
    ?assertCmd(
       plugin_dir() ++ "/../rabbitmq-server/scripts/rabbitmqctl " ++ Args),
    timer:sleep(100).

plugin_dir() ->
    {ok, [[File]]} = init:get_argument(config),
    filename:dirname(filename:dirname(File)).

json(List) ->
    "{" ++ string:join([quote(K, V) || {K, V} <- List], ",") ++ "}".
quote(K, V) -> rabbit_misc:format("\"~s\":\"~s\"", [K, V]).
