-module(rabbitmq_aws_sup_tests).

-include_lib("eunit/include/eunit.hrl").

start_link_test_() ->
  {foreach,
    fun() ->
      meck:new(supervisor, [passthrough, unstick])
    end,
    fun(_) ->
      meck:unload(supervisor)
    end,
    [
      {"supervisor start_link", fun() ->
        meck:expect(supervisor, start_link, fun(_, _, _) -> {ok, test_result} end),
        ?assertEqual({ok, test_result},
                      rabbitmq_aws_sup:start_link()),
        meck:validate(supervisor)
       end}
    ]
  }.

init_test() ->
  ?assertEqual({ok, {{one_for_one, 5, 10},
                     [{rabbitmq_aws, {rabbitmq_aws, start_link, []},
                       permanent, 5, worker, [rabbitmq_aws]}]}},
               rabbitmq_aws_sup:init([])).
