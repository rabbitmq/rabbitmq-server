-module(rabbit_mqtt_processor_tests).

-include_lib("eunit/include/eunit.hrl").

s(X) -> application:set_env(rabbitmq_mqtt, ignore_colons_in_username, X).

get_vhost_username_test_() ->
    {foreach,
     fun () -> application:load(rabbitmq_mqtt) end,
     fun (_) -> s(false) end,
     [{"ignores colons in username if option set",
       fun () ->
               s(true),
               ?assertEqual({rabbit_mqtt_util:env(vhost), <<"a:b:c">>},
                            rabbit_mqtt_processor:get_vhost_username(<<"a:b:c">>))
       end},
      {"interprets colons in username if option not set",
       fun() ->
               ?assertEqual({<<"a:b">>, <<"c">>},
                            rabbit_mqtt_processor:get_vhost_username(<<"a:b:c">>))
       end}]}.
