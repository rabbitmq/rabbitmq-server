-module(httpc_aws_tests).

-include_lib("eunit/include/eunit.hrl").

-include("httpc_aws.hrl").

api_test_() ->
  {
    foreach,
    fun setup/0,
    fun cleanup/1,
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
    {ok, _Pid} = httpc_aws:start_link(),
    httpc_aws.

cleanup(Name) ->
    gen_server:stop(Name).
