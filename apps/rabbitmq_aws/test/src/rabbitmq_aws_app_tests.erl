-module(rabbitmq_aws_app_tests).

-include_lib("eunit/include/eunit.hrl").

start_test_() ->
  {foreach,
    fun() ->
      meck:new(rabbitmq_aws_sup, [passthrough])
    end,
    fun(_) ->
      meck:unload(rabbitmq_aws_sup)
    end,
    [
      {"supervisor initialized", fun() ->
        meck:expect(rabbitmq_aws_sup, start_link, fun() -> {ok, test_result} end),
        ?assertEqual({ok, test_result},
                     rabbitmq_aws_app:start(temporary, [])),
        meck:validate(rabbitmq_aws_sup)
       end}
    ]
  }.

stop_test() ->
  ?assertEqual(ok, rabbitmq_aws_app:stop({})).
