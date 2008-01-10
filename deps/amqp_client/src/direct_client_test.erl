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
%%   The Original Code is the RabbitMQ Erlang Client.
%%
%%   The Initial Developers of the Original Code are LShift Ltd.,
%%   Cohesive Financial Technologies LLC., and Rabbit Technologies Ltd.
%%
%%   Portions created by LShift Ltd., Cohesive Financial
%%   Technologies LLC., and Rabbit Technologies Ltd. are Copyright (C)
%%   2007 LShift Ltd., Cohesive Financial Technologies LLC., and Rabbit
%%   Technologies Ltd.;
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): Ben Hood <0x6e6562@gmail.com>.
%%

-module(direct_client_test).

-define(RPC_TIMEOUT, 10000).
-define(RPC_SLEEP, 500).

-export([test_wrapper/1, test_coverage/1]).

-include_lib("eunit/include/eunit.hrl").

basic_get_test() -> test_util:basic_get_test(new_connection()).

basic_return_test() -> test_util:basic_return_test(new_connection()).

basic_qos_test() -> test_util:basic_qos_test(new_connection()).

basic_recover_test() -> test_util:basic_recover_test(new_connection()).

basic_consume_test() -> test_util:basic_consume_test(new_connection()).

lifecycle_test() -> test_util:lifecycle_test(new_connection()).

basic_ack_test() ->test_util:basic_ack_test(new_connection()).

new_connection() -> amqp_connection:start("guest", "guest").

test_wrapper(NodeName) ->
    Node = rabbit_misc:localnode(list_to_atom(NodeName)),
    rabbit_multi:get_pid(Node, 0),
    test().

test_coverage(NodeName) ->
    Node = rabbit_misc:localnode(list_to_atom(NodeName)),
    rabbit_multi:get_pid(Node, 0),
    rabbit_misc:enable_cover(),
    test(),
    rabbit_misc:report_cover().

