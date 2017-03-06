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
%% The Original Code is RabbitMQ Consistent Hash Exchange.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2016 Pivotal Software, Inc.  All rights reserved.
%%

-module(system_SUITE).
-include_lib("common_test/include/ct.hrl").

-include_lib("amqp_client/include/amqp_client.hrl").
-compile(export_all).

all() ->
    [
     queue_created,
     authentication,
     resource_alarm
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodename_suffix, ?MODULE}
      ]),
    Config2 = rabbit_ct_helpers:run_setup_steps(Config1,
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps()),
    Config2.

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_group(_, Config) ->
    Config.

end_per_group(_, Config) ->
    Config.

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).   


%% -------------------------------------------------------------------
%% Testsuite cases 
%% -------------------------------------------------------------------

%% Only really tests that we're not completely broken.
queue_created(Config) ->
    Now = time_compat:os_system_time(seconds),

    Ch =  rabbit_ct_client_helpers:open_channel(Config, 0),
    #'queue.declare_ok'{queue = Q} =
        amqp_channel:call(Ch, #'queue.declare'{exclusive = true}),
    amqp_channel:call(Ch, #'queue.bind'{queue       = Q,
                                        exchange    = <<"amq.rabbitmq.event">>,
                                        routing_key = <<"queue.*">>}),
    amqp_channel:subscribe(Ch, #'basic.consume'{queue = Q, no_ack = true},
                           self()),
    receive
        #'basic.consume_ok'{} -> ok
    end,

    #'queue.declare_ok'{queue = Q2} =
        amqp_channel:call(Ch, #'queue.declare'{exclusive = true}),

    receive
        {#'basic.deliver'{routing_key = Key},
         #amqp_msg{props = #'P_basic'{headers = Headers, timestamp = TS}}} ->
            %% timestamp is within the last 5 seconds
            true = ((TS - Now) =< 5),
            <<"queue.created">> = Key,
            {longstr, Q2} = rabbit_misc:table_lookup(Headers, <<"name">>)
    end,

    rabbit_ct_client_helpers:close_channel(Ch),
    ok.


authentication(Config) ->
    Ch =  rabbit_ct_client_helpers:open_channel(Config, 0),

    #'queue.declare_ok'{queue = Q} =
        amqp_channel:call(Ch, #'queue.declare'{exclusive = true}),
    amqp_channel:call(Ch, #'queue.bind'{queue       = Q,
                                        exchange    = <<"amq.rabbitmq.event">>,
                                        routing_key = <<"user.#">>}),
    Conn2 = rabbit_ct_client_helpers:open_unmanaged_connection(Config, 0),

    amqp_channel:subscribe(Ch, #'basic.consume'{queue = Q, no_ack = true},
                           self()),
    receive
        #'basic.consume_ok'{} -> ok
    end,

    receive
        {#'basic.deliver'{routing_key = Key},
         #amqp_msg{props = #'P_basic'{headers = Headers}}} ->
            <<"user.authentication.success">> = Key,
            undefined = rabbit_misc:table_lookup(Headers, <<"vhost">>),
            {longstr, _PeerHost} = rabbit_misc:table_lookup(Headers, <<"peer_host">>),
            {bool, false} = rabbit_misc:table_lookup(Headers, <<"ssl">>)
    end,

    amqp_connection:close(Conn2),
    ok.

resource_alarm(Config) ->
    Ch = declare_event_queue(Config, <<"alarm.*">>),

    Source = disk,
    Node = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),

    rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_alarm, set_alarm,
                                 [{{resource_limit, Source, Node}, []}]),
    receive_event(<<"alarm.set">>),

    rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_alarm, clear_alarm,
                                 [{resource_limit, Source, Node}]),
    receive_event(<<"alarm.cleared">>),

    rabbit_ct_client_helpers:close_channel(Ch),
    ok.

%% -------------------------------------------------------------------
%% Helpers
%% -------------------------------------------------------------------

declare_event_queue(Config, RoutingKey) ->
    Ch = rabbit_ct_client_helpers:open_channel(Config, 0),
    #'queue.declare_ok'{queue = Q} =
        amqp_channel:call(Ch, #'queue.declare'{exclusive = true}),
    amqp_channel:call(Ch, #'queue.bind'{queue       = Q,
                                        exchange    = <<"amq.rabbitmq.event">>,
                                        routing_key = RoutingKey}),
    amqp_channel:subscribe(Ch, #'basic.consume'{queue = Q, no_ack = true},
                           self()),
    receive
        #'basic.consume_ok'{} ->
            ok
    end,
    Ch.

receive_event(Event) ->
    receive
        {#'basic.deliver'{routing_key = RoutingKey},
         #amqp_msg{props = #'P_basic'{}}} ->
            Event = RoutingKey
    after
        60000 ->
            throw({receive_event_timeout, Event})
    end.
