-module(rabbit_mqtt_reader_tests).

-include_lib("eunit/include/eunit.hrl").


block_test_() ->
    {foreach,
    fun() ->
        {ok, C} = emqttc:start_link([{host, "localhost"}, 
                                 {client_id, <<"simpleClient">>}, 
                                 {proto_ver, 3}, 
                                 {logger, info}, 
                                 {puback_timeout, 1}]),
        emqttc:subscribe(C, <<"TopicA">>, qos0),
        emqttc:publish(C, <<"TopicA">>, <<"Payload">>),
        
        %% Client is tricky. There is no way to tell if we are connected except
        %% publishing and receiving
        skip_publishes(<<"TopicA">>, [<<"Payload">>]),
        emqttc:unsubscribe(C, [<<"TopicA">>]),
        C
    end,
    fun(C) ->
        vm_memory_monitor:set_vm_memory_high_watermark(0.4),
        rabbit_alarm:clear_alarm({resource_limit, memory, node()}),
        emqttc:disconnect(C)
    end,
    [
    fun(C) ->
        fun() ->
        %% Not blocked
        {ok, _} = emqttc:sync_publish(C, <<"Topic1">>, <<"Payload1">>, 
                                      [{qos, 1}]),

        vm_memory_monitor:set_vm_memory_high_watermark(0.00000001),
        rabbit_alarm:set_alarm({{resource_limit, memory, node()}, []}),

        %% Let it block
        timer:sleep(100),
        %% Blocked, but still will publish
        {ok, _} = emqttc:sync_publish(C, <<"Topic1">>, <<"Still not blocked">>, 
                                      [{qos, 1}]),

        %% Blocked
        {error, ack_timeout} = emqttc:sync_publish(C, <<"Topic1">>, 
                                                   <<"Blocked">>, [{qos, 1}])
        end
    end
    ]}.

skip_publishes(Topic, []) -> ok;
skip_publishes(Topic, [Payload|Rest]) ->
    receive
        {publish, Topic, Payload} -> skip_publishes(Topic, Rest)
        after 100 -> 
            throw({publish_not_delivered, Payload})
    end.
