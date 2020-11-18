-module(rabbitmq_aws_all_tests).

-export([run/0]).

-include_lib("eunit/include/eunit.hrl").

run() ->
  Result = {
    eunit:test(rabbitmq_aws_app_tests, [verbose]),
    eunit:test(rabbitmq_aws_config_tests, [verbose]),
    eunit:test(rabbitmq_aws_json_tests, [verbose]),
    eunit:test(rabbitmq_aws_sign_tests, [verbose]),
    eunit:test(rabbitmq_aws_sup_tests, [verbose]),
    eunit:test(rabbitmq_aws_tests, [verbose]),
    eunit:test(rabbitmq_aws_urilib_tests, [verbose]),
    eunit:test(rabbitmq_aws_xml_tests, [verbose])
  },
  ?assertEqual({ok, ok, ok, ok, ok, ok, ok, ok}, Result).
