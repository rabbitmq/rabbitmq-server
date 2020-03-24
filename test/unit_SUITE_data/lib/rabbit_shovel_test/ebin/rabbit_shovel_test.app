{application, rabbit_shovel_test,
 [{description, "Test .app file for tests for encrypting configuration"},
  {vsn, ""},
  {modules, []},
  {env, [ {shovels, [ {my_first_shovel,
                    [ {sources,
                        [ {brokers, [ {encrypted, <<"CfJXuka/uJYsqAtiJnwKpSY4moMPcOBh4sO8XDcdmhXbVYGKCDLKEilWPMfvOAQ2lN1BQneGn6bvDZi2+gDu6iHVKfafQAZSv8zcsVB3uYdBXFzqTCWO8TAsgG6LUMPT">>}
                                    , {encrypted, <<"dBO6n+G1OiBwZeLXhvmNYeTE57nhBOmicUBF34zo4nQjerzQaNoEk8GA2Ts5PzMhYeO6U6Y9eEmheqIr9Gzh2duLZic65ZMQtIKNpWcZJllEhGpk7aV1COr23Yur9fWG">>}
                                    ]}
                        , {declarations, [ {'exchange.declare',
                                              [ {exchange, <<"my_fanout">>}
                                              , {type, <<"fanout">>}
                                              , durable
                                              ]}
                                         , {'queue.declare',
                                              [{arguments,
                                                 [{<<"x-message-ttl">>, long, 60000}]}]}
                                         , {'queue.bind',
                                              [ {exchange, <<"my_direct">>}
                                              , {queue,    <<>>}
                                              ]}
                                         ]}
                        ]}
                    , {destinations,
                        [ {broker, "amqp://"}
                        , {declarations, [ {'exchange.declare',
                                              [ {exchange, <<"my_direct">>}
                                              , {type, <<"direct">>}
                                              , durable
                                              ]}
                                         ]}
                        ]}
                    , {queue, <<>>}
                    , {prefetch_count, 10}
                    , {ack_mode, on_confirm}
                    , {publish_properties, [ {delivery_mode, 2} ]}
                    , {add_forward_headers, true}
                    , {publish_fields, [ {exchange, <<"my_direct">>}
                                       , {routing_key, <<"from_shovel">>}
                                       ]}
                    , {reconnect_delay, 5}
                    ]}
                ]}
    ]},

  {applications, [kernel, stdlib]}]}.
