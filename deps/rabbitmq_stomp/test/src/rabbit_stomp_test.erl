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
%% Copyright (c) 2007-2014 GoPivotal, Inc.  All rights reserved.
%%

-module(rabbit_stomp_test).
-export([all_tests/0]).
-import(rabbit_misc, [pget/2]).

-include_lib("amqp_client/include/amqp_client.hrl").
-include("rabbit_stomp_frame.hrl").
-define(DESTINATION, "/queue/bulk-test").

all_tests() ->
    test_messages_not_dropped_on_disconnect(),
    test_direct_client_connections_are_not_leaked(),
    ok.

-define(GARBAGE, <<"bdaf63dda9d78b075c748b740e7c3510ad203b07\nbdaf63dd">>).

count_connections() ->
    length(supervisor2:which_children(rabbit_stomp_client_sup_sup)).

test_direct_client_connections_are_not_leaked() ->
    N = count_connections(),
    lists:foreach(fun (_) ->
                          {ok, Client = {Socket, _}} = rabbit_stomp_client:connect(),
                          %% send garbage which trips up the parser
                          gen_tcp:send(Socket, ?GARBAGE),
                          rabbit_stomp_client:send(
                           Client, "LOL", [{"", ""}])
                  end,
                  lists:seq(1, 1000)),
    timer:sleep(5000),
    N = count_connections(),
    ok.

test_messages_not_dropped_on_disconnect() ->
    N = count_connections(),
    {ok, Client} = rabbit_stomp_client:connect(),
    N1 = N + 1,
    N1 = count_connections(),
    [rabbit_stomp_client:send(
       Client, "SEND", [{"destination", ?DESTINATION}],
       [integer_to_list(Count)]) || Count <- lists:seq(1, 1000)],
    rabbit_stomp_client:disconnect(Client),
    QName = rabbit_misc:r(<<"/">>, queue, <<"bulk-test">>),
    timer:sleep(3000),
    N = count_connections(),
    rabbit_amqqueue:with(
      QName, fun(Q) ->
                     1000 = pget(messages, rabbit_amqqueue:info(Q, [messages]))
             end),
    ok.
