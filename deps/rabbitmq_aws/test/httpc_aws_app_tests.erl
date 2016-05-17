-module(httpc_aws_app_tests).

-include_lib("eunit/include/eunit.hrl").

start_test_() ->
  {foreach,
    fun() ->
      meck:new(httpc_aws_sup, [passthrough])
    end,
    fun(_) ->
      meck:unload(httpc_aws_sup)
    end,
    [
      {"supervisor initialized", fun() ->
        meck:expect(httpc_aws_sup, start_link, fun() -> {ok, test_result} end),
        ?assertEqual({ok, test_result},
                     httpc_aws_app:start(temporary, [])),
        meck:validate(httpc_aws_sup)
       end}
    ]
  }.

stop_test() ->
  ?assertEqual(ok, httpc_aws_app:stop({})).
