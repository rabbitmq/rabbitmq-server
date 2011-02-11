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
%% Copyright (c) 2007-2011 VMware, Inc.  All rights reserved.
%%

-module(rabbit_federation_test).

-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

simple_test() ->
    {ok, Conn} = amqp_connection:start(network),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    amqp_channel:call(Ch, #'exchange.declare'{ exchange = <<"upstream">>,
                                               type = <<"direct">> }),
    amqp_channel:call(
      Ch, #'exchange.declare'{
        exchange = <<"downstream">>,
        type = <<"x-federation">>,
        arguments = [{<<"upstream">>, longstr,
                      <<"amqp://localhost/%2f/upstream">>},
                     {<<"type">>, longstr, <<"direct">>}]
       }),
    amqp_channel:call(Ch, #'queue.declare'{ queue = <<"result">>,
                                            exclusive = true }),
    amqp_channel:call(Ch, #'queue.bind'{ queue = <<"result">>,
                                         exchange = <<"downstream">>,
                                         routing_key = <<"key">> }),
    amqp_channel:call(Ch, #'basic.publish'{ exchange = <<"upstream">>,
                                            routing_key = <<"key">> },
                      #amqp_msg { payload = <<"HELLO">> }),
    amqp_channel:subscribe(Ch, #'basic.consume'{ queue = <<"result">> },
                           self()),
    receive
        #'basic.consume_ok'{} -> ok
    end,
    receive
        {#'basic.deliver'{}, #amqp_msg { payload = <<"HELLO">> }} -> ok
    end,
    amqp_channel:call(Ch, #'exchange.delete'{ exchange = <<"downstream">> }),
    amqp_channel:call(Ch, #'exchange.delete'{ exchange = <<"upstream">> }),
    amqp_connection:close(Conn),
    ok.
