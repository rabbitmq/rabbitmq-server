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
-module(negative_test_util).

-include_lib("eunit/include/eunit.hrl").
-include_lib("rabbit_framing.hrl").

-compile(export_all).

non_existent_exchange_test(Connection) ->
    {A,B,C} = now(),
    X = <<A:32,B:32,C:32>>,
    RoutingKey = <<"a">>, 
    Payload = <<"foobar">>,
    Channel = lib_amqp:start_channel(Connection),
    lib_amqp:declare_exchange(Channel, X),
    %% Deliberately mix up the routingkey and exchange arguments
    lib_amqp:publish(Channel, RoutingKey, X, Payload),
    wait_for_death(Channel),
    ?assertMatch(true, is_process_alive(Connection)),
    lib_amqp:close_connection(Connection).

hard_error_test(Connection) ->
    Channel = lib_amqp:start_channel(Connection),
    try
        amqp_channel:call(Channel, #'basic.qos'{global = true})
    catch
        exit:_ -> ok;
        _:_    -> exit(did_not_throw_error)
    end,
    wait_for_death(Channel),
    wait_for_death(Connection).

wait_for_death(Pid) ->
    Ref = erlang:monitor(process, Pid),
    receive {'DOWN', Ref, process, Pid, _Reason} -> ok
    after 1000 -> exit({timed_out_waiting_for_process_death, Pid})
    end.
