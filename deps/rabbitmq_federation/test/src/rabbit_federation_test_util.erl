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

rabbitmqctl(Args) ->
    ?assertCmd(
       plugin_dir() ++ "/../rabbitmq-server/scripts/rabbitmqctl " ++ Args),
    timer:sleep(100).

policy(UpstreamSet) ->
    rabbit_misc:format("{\"federation-upstream-set\": \"~s\"}", [UpstreamSet]).

plugin_dir() ->
    {ok, [[File]]} = init:get_argument(config),
    filename:dirname(filename:dirname(File)).
