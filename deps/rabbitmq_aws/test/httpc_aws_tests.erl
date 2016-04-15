-module(httpc_aws_tests).

-include_lib("eunit/include/eunit.hrl").

-include("httpc_aws.hrl").

api_test_() ->
  {
    foreach,
    fun setup/0,
    [
      {"set_credentials values assigned",
        fun() ->
          AccessKey = "foo",
          SecretKey = "bar",
          ok = httpc_aws:set_credentials(AccessKey, SecretKey),
          ?assertEqual({ok, AccessKey, SecretKey},
                        httpc_aws:get_credentials())
        end
      }
    ]
  }.

setup() ->
  {ok, Pid} = httpc_aws:start_link(),
  Pid.
