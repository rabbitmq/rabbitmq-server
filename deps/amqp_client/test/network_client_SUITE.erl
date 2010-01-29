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

-module(network_client_SUITE).

-export([test_coverage/0]).

-include("amqp_client.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(ITERATIONS, 100).

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

channel_lifecycle_test() ->
    test_util:channel_lifecycle_test(new_connection()).

queue_unbind_test() ->
    test_util:queue_unbind_test(new_connection()).

command_serialization_test() ->
    test_util:command_serialization_test(new_connection()).

teardown_test() ->
    repeat(fun test_util:teardown_test/1, ?ITERATIONS).

rpc_test() ->
    test_util:rpc_test(new_connection()).

pub_and_close_test_() ->
    {timeout, 50,
        fun() ->
            test_util:pub_and_close_test(new_connection(), new_connection())
        end}.

channel_tune_negotiation_test() ->
    amqp_connection:close(amqp_connection:start_network(
                            #amqp_params{ channel_max = 10 })).

%%---------------------------------------------------------------------------
%% Negative Tests

non_existent_exchange_test() -> 
    negative_test_util:non_existent_exchange_test(new_connection()).

bogus_rpc_test() ->
    repeat(fun negative_test_util:bogus_rpc_test/1, ?ITERATIONS).

hard_error_test() ->
    repeat(fun negative_test_util:hard_error_test/1, ?ITERATIONS).

non_existent_user_test() ->
    negative_test_util:non_existent_user_test().

invalid_password_test() ->
    negative_test_util:invalid_password_test().

non_existent_vhost_test() ->
    negative_test_util:non_existent_vhost_test().

no_permission_test() ->
    negative_test_util:no_permission_test().

channel_writer_death_test() ->
    negative_test_util:channel_writer_death_test(new_connection()).

channel_death_test() ->
    negative_test_util:channel_death_test(new_connection()).

shortstr_overflow_property_test() ->
    negative_test_util:shortstr_overflow_property_test(new_connection()).

shortstr_overflow_field_test() ->
    negative_test_util:shortstr_overflow_field_test(new_connection()).
    
%%---------------------------------------------------------------------------
%% Common Functions

repeat(Fun, Times) ->
    [ Fun(new_connection()) || _ <- lists:seq(1, Times)].


new_connection() ->
    amqp_connection:start_network().

test_coverage() ->
    rabbit_misc:enable_cover(),
    test(),
    rabbit_misc:report_cover().
