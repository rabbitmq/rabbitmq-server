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

-module(direct_client_SUITE).

-define(RPC_TIMEOUT, 10000).
-define(RPC_SLEEP, 500).

-export([test_coverage/0]).
-export([test_channel_flow/0]).

-include("amqp_client.hrl").
-include_lib("eunit/include/eunit.hrl").

basic_get_test() -> 
    test_util:basic_get_test(new_connection()).

basic_return_test() ->
    test_util:basic_return_test(new_connection()).

basic_qos_test() ->
    test_util:basic_qos_test(new_connection()).

basic_recover_test() ->
    test_util:basic_recover_test(new_connection()).

basic_consume_test() ->
    test_util:basic_consume_test(new_connection()).

large_content_test() ->
    test_util:large_content_test(new_connection()).

lifecycle_test() ->
    test_util:lifecycle_test(new_connection()).

nowait_exchange_declare_test() ->
    test_util:nowait_exchange_declare_test(new_connection()).

basic_ack_test() ->
    test_util:basic_ack_test(new_connection()).

basic_ack_call_test() ->
    test_util:basic_ack_call_test(new_connection()).

command_serialization_test() ->
    test_util:command_serialization_test(new_connection()).

queue_unbind_test() ->
    test_util:queue_unbind_test(new_connection()).

%%---------------------------------------------------------------------------
%% This must be kicked off manually because it can only be run after Rabbit
%% has been running for 1 minute
test_channel_flow() ->
    test_util:channel_flow_test(new_connection()).

%%---------------------------------------------------------------------------
%% Negative Tests
%%---------------------------------------------------------------------------

non_existent_exchange_test() -> 
    negative_test_util:non_existent_exchange_test(new_connection()).

hard_error_test() ->
    negative_test_util:hard_error_test(new_connection()).

%%---------------------------------------------------------------------------
%% Common Functions
%%---------------------------------------------------------------------------

new_connection() ->
    amqp_connection:start_direct().

test_coverage() ->
    rabbit_misc:enable_cover(),
    test(),
    rabbit_misc:report_cover().

