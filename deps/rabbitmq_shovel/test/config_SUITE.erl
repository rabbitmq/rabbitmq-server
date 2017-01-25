-module(config_SUITE).

-compile(export_all).

-include_lib("eunit/include/eunit.hrl").

-define(EXCHANGE,    <<"test_exchange">>).
-define(TO_SHOVEL,   <<"to_the_shovel">>).
-define(FROM_SHOVEL, <<"from_the_shovel">>).
-define(UNSHOVELLED, <<"unshovelled">>).
-define(SHOVELLED,   <<"shovelled">>).
-define(TIMEOUT,     1000).

all() ->
    [
      {group, tests}
    ].

groups() ->
    [
      {tests, [parallel], [
          parse_amqp091,
          parse_amqp10_mixed
        ]}
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    Config.

end_per_suite(Config) ->
    Config.

init_per_group(_, Config) ->
    Config.

end_per_group(_, Config) ->
    Config.

init_per_testcase(_Testcase, Config) -> Config.

end_per_testcase(_Testcase, Config) -> Config.


%% -------------------------------------------------------------------
%% Testcases.
%% -------------------------------------------------------------------

parse_amqp091(_Config) ->
    Amqp091Src = {source, [{protocol, amqp091},
                           {uris, ["ampq://myhost:5672/vhost"]},
                           {declarations, []},
                           {queue, <<"the-queue">>},
                           {delete_after, never},
                           {prefetch_count, 10}]},
    Amqp091Dst = {destination, [{protocol, amqp091},
                                {uris, ["ampq://myhost:5672"]},
                                {declarations, []},
                                {publish_properties, [{delivery_mode, 1}]},
                                {publish_fields, []},
                                {add_forward_headers, true}]},
    % Amqp10Dst = {destination, [{protocol, amqp10},
    %                            {uris, ["ampq://myhost:5672"]},
    %                            {declarations, []},
    %                            {target_address, <<"targe-queue">>},
    %                            {delivery_annotations, [{soma_ann, <<"some-info">>}]},
    %                            {message_annotations, [{soma_ann, <<"some-info">>}]},
    %                            {message_properties, [{user_id, <<"some-user">>}]}
    %                            ]},
    In = [Amqp091Src,
          Amqp091Dst,
          {ack_mode, on_confirm},
          {reconnect_delay, 2}],

    ?assertMatch(
       {ok, #{name := my_shovel,
              ack_mode := on_confirm,
              source := #{module := rabbit_amqp0_9_1_node,
                          uris := ["ampq://myhost:5672/vhost"],
                          queue := <<"the-queue">>,
                          prefetch_count := 10,
                          delete_after := never,
                          resource_decl := _SDecl},
              dest := #{module := rabbit_amqp0_9_1_node,
                        uris := ["ampq://myhost:5672"],
                        fields_fun := _PubFields,
                        props_fun := _PubProps,
                        resource_decl := _DDecl,
                        add_forward_headers := true}}},
        rabbit_shovel_config:parse(my_shovel, In)),
    ok.

parse_amqp10_mixed(_Config) ->
    Amqp10Src = {source, [{protocol, amqp10},
                          {uris, ["ampq://myotherhost:5672"]},
                          {source_address, <<"the-queue">>}
                         ]},
    Amqp10Dst = {destination, [{protocol, amqp10},
                               {uris, ["ampq://myhost:5672"]},
                               {target_address, <<"targe-queue">>},
                               {delivery_annotations, [{soma_ann, <<"some-info">>}]},
                               {message_annotations, [{soma_ann, <<"some-info">>}]},
                               {properties, [{user_id, <<"some-user">>}]},
                               {add_forward_headers, true}
                              ]},
    In = [Amqp10Src,
          Amqp10Dst,
          {ack_mode, on_confirm},
          {reconnect_delay, 2}],

    ?assertMatch(
       {ok, #{name := my_shovel,
              ack_mode := on_confirm,
              source := #{module := rabbit_amqp10_shovel,
                          uris := ["ampq://myotherhost:5672"],
                          source_address := <<"the-queue">>
                          },
              dest := #{module := rabbit_amqp10_shovel,
                        uris := ["ampq://myhost:5672"],
                        target_address := <<"targe-queue">>,
                        delivery_annotations := #{soma_ann := <<"some-info">>},
                        message_annotations := #{soma_ann := <<"some-info">>},
                        properties := #{user_id := <<"some-user">>},
                        add_forward_headers := true}}},
        rabbit_shovel_config:parse(my_shovel, In)),
    ok.
