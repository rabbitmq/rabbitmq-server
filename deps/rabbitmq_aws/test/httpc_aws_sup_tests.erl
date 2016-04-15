-module(httpc_aws_sup_tests).

-include_lib("eunit/include/eunit.hrl").


init_test() ->
  ?assertEqual({ok, {{one_for_one, 5, 10},
                     [{httpc_aws, {httpc_aws, start_link, []},
                       permanent, 5, worker, [httpc_aws]}]}},
               httpc_aws_sup:init([])).
