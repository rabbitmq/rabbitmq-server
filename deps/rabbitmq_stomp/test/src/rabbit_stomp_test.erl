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
%% Copyright (c) 2007-2013 GoPivotal, Inc.  All rights reserved.
%%

-module(rabbit_stomp_test).
-export([all_tests/0]).
-import(rabbit_misc, [pget/2]).

-include_lib("amqp_client/include/amqp_client.hrl").
-include("rabbit_stomp_frame.hrl").
-define(DESTINATION, "/queue/bulk-test").

all_tests() ->
    test_messages_not_dropped_on_disconnect(),
    ok.

test_messages_not_dropped_on_disconnect() ->
    {ok, Client} = rabbit_stomp_client:connect(),
    [rabbit_stomp_client:send(
       Client, "SEND", [{"destination", ?DESTINATION}],
       [integer_to_list(Count)]) || Count <- lists:seq(1, 1000)],
    rabbit_stomp_client:disconnect(Client),
    QName = rabbit_misc:r(<<"/">>, queue, <<"bulk-test">>),
    timer:sleep(3000),
    rabbit_amqqueue:with(
      QName, fun(Q) ->
                     1000 = pget(messages, rabbit_amqqueue:info(Q, [messages]))
             end),
    ok.
