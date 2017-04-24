-module(parameters_SUITE).

-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("rabbit_common/include/rabbit_framing.hrl").

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
          parse_amqp091_legacy,
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

parse_amqp091_legacy(_Config) ->
    Params =
        [{<<"src-uri">>, <<"amqp://localhost:5672">>},
         {<<"src-protocol">>, <<"amqp091">>},
         {<<"dst-protocol">>, <<"amqp091">>},
         {<<"dest-uri">>, <<"amqp://remotehost:5672">>},
         {<<"add-forward-headers">>, true},
         {<<"add-timestamp-header">>, true},
         {<<"publish-properties">>, [{<<"cluster_id">>, <<"x">>},
                                     {<<"delivery_mode">>, 2}]},
         {<<"ack-mode">>, <<"on-publish">>},
         {<<"delete-after">>, <<"queue-length">>},
         {<<"prefetch-count">>, 30},
         {<<"reconnect-delay">>, 1001},
         {<<"src-queue">>, <<"a-src-queue">>},
         {<<"dest-queue">>, <<"a-dest-queue">>}
        ],

    test_parse_amqp091(Params).

parse_amqp091(_Config) ->
    Params =
        [{<<"src-uri">>, <<"amqp://localhost:5672">>},
         {<<"src-protocol">>, <<"amqp091">>},
         {<<"dst-protocol">>, <<"amqp091">>},
         {<<"dest-uri">>, <<"amqp://remotehost:5672">>},
         {<<"dest-add-forward-headers">>, true},
         {<<"dest-add-timestamp-header">>, true},
         {<<"dest-publish-properties">>, [{<<"cluster_id">>, <<"x">>},
                                          {<<"delivery_mode">>, 2}]},
         {<<"ack-mode">>, <<"on-publish">>},
         {<<"src-delete-after">>, <<"queue-length">>},
         {<<"src-prefetch-count">>, 30},
         {<<"reconnect-delay">>, 1001},
         {<<"src-queue">>, <<"a-src-queue">>},
         {<<"dest-queue">>, <<"a-dest-queue">>}
        ],
    test_parse_amqp091(Params).


test_parse_amqp091(Params) ->
    {ok, Result} = rabbit_shovel_parameters:parse({"vhost", "name"},
                                                  "my-cluster", Params),
    #{ack_mode := on_publish,
      name := "name",
      reconnect_delay := 1001,
      dest := #{module := rabbit_amqp091_shovel,
                uris := ["amqp://remotehost:5672"],
                props_fun := PropsFun
               },
      source := #{module := rabbit_amqp091_shovel,
                  uris := ["amqp://localhost:5672"],
                  prefetch_count := 30,
                  queue := <<"a-src-queue">>,
                  delete_after := 'queue-length'}
     } = Result,

    #'P_basic'{headers = HeadersResult,
               delivery_mode = 2,
               cluster_id = <<"x">>} = PBasic = PropsFun("amqp://localhost:5672",
                                                         "amqp://remotehost:5672",
                                                         #'P_basic'{headers = undefined}),
    ct:pal("PBasic ~p~n", [PBasic]),
    {_, array, [{table, Shovelled}]} = lists:keyfind(<<"x-shovelled">>, 1, HeadersResult),
    {_, long, _} = lists:keyfind(<<"x-shovelled-timestamp">>, 1, HeadersResult),

    ExpectedHeaders =
    [{<<"shovelled-by">>, "my-cluster"},
     {<<"shovel-type">>, <<"dynamic">>},
     {<<"shovel-name">>, "name"},
     {<<"shovel-vhost">>, "vhost"},
     {<<"src-uri">>,"amqp://localhost:5672"},
     {<<"dest-uri">>,"amqp://remotehost:5672"},
     {<<"src-queue">>,<<"a-src-queue">>},
     {<<"dest-queue">>,<<"a-dest-queue">>}],
    lists:foreach(fun({K, V}) ->
                          ?assertMatch({K, _, V},
                                       lists:keyfind(K, 1, Shovelled))
                  end, ExpectedHeaders),
    ok.

parse_amqp10_mixed(_Config) ->
    % Amqp10Src = {source, [{protocol, amqp10},
    %                       {uris, ["ampq://myotherhost:5672"]},
    %                       {source_address, <<"the-queue">>}
    %                      ]},
    % Amqp10Dst = {destination, [{protocol, amqp10},
    %                            {uris, ["ampq://myhost:5672"]},
    %                            {target_address, <<"targe-queue">>},
    %                            {delivery_annotations, [{soma_ann, <<"some-info">>}]},
    %                            {message_annotations, [{soma_ann, <<"some-info">>}]},
    %                            {properties, [{user_id, <<"some-user">>}]},
    %                            {add_forward_headers, true}
    %                           ]},
    % In = [Amqp10Src,
    %       Amqp10Dst,
    %       {ack_mode, on_confirm},
    %       {reconnect_delay, 2}],

    % ?assertMatch(
    %    {ok, #{name := my_shovel,
    %           ack_mode := on_confirm,
    %           source := #{module := rabbit_amqp10_shovel,
    %                       uris := ["ampq://myotherhost:5672"],
    %                       source_address := <<"the-queue">>
    %                       },
    %           dest := #{module := rabbit_amqp10_shovel,
    %                     uris := ["ampq://myhost:5672"],
    %                     target_address := <<"targe-queue">>,
    %                     delivery_annotations := #{soma_ann := <<"some-info">>},
    %                     message_annotations := #{soma_ann := <<"some-info">>},
    %                     properties := #{user_id := <<"some-user">>},
    %                     add_forward_headers := true}}},
    %     rabbit_shovel_config:parse(my_shovel, In)),
    ok.
