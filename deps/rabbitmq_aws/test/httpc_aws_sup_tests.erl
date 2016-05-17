-module(httpc_aws_sup_tests).

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
                      httpc_aws_sup:start_link()),
        meck:validate(supervisor)
       end}
    ]
  }.

init_test() ->
  ?assertEqual({ok, {{one_for_one, 5, 10},
                     [{httpc_aws, {httpc_aws, start_link, []},
                       permanent, 5, worker, [httpc_aws]}]}},
               httpc_aws_sup:init([])).
