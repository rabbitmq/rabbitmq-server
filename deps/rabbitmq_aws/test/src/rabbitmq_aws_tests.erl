-module(rabbitmq_aws_tests).

-include_lib("eunit/include/eunit.hrl").

-include("include/rabbitmq_aws.hrl").

init_test_() ->
  {foreach,
    fun() ->
      os:putenv("AWS_DEFAULT_REGION", "us-west-3"),
      meck:new(rabbitmq_aws_config, [passthrough])
    end,
    fun(_) ->
      os:unsetenv("AWS_DEFAULT_REGION"),
      meck:unload(rabbitmq_aws_config)
    end,
    [
      {"ok", fun() ->
        os:putenv("AWS_ACCESS_KEY_ID", "Sésame"),
        os:putenv("AWS_SECRET_ACCESS_KEY", "ouvre-toi"),
        {ok, Pid} = rabbitmq_aws:start_link(),
        rabbitmq_aws:set_region("us-west-3"),
        rabbitmq_aws:refresh_credentials(),
        {ok, State} = gen_server:call(Pid, get_state),
        ok = gen_server:stop(Pid),
        os:unsetenv("AWS_ACCESS_KEY_ID"),
        os:unsetenv("AWS_SECRET_ACCESS_KEY"),
        Expectation = {state,"Sésame","ouvre-toi",undefined,undefined,"us-west-3", undefined,undefined},
        ?assertEqual(Expectation, State)
       end},
      {"error", fun() ->
        meck:expect(rabbitmq_aws_config, credentials, fun() -> {error, test_result} end),
        {ok, Pid} = rabbitmq_aws:start_link(),
        rabbitmq_aws:set_region("us-west-3"),
        rabbitmq_aws:refresh_credentials(),
        {ok, State} = gen_server:call(Pid, get_state),
        ok = gen_server:stop(Pid),
        Expectation = {state,undefined,undefined,undefined,undefined,"us-west-3",undefined,test_result},
        ?assertEqual(Expectation, State),
        meck:validate(rabbitmq_aws_config)
       end}
    ]
  }.

terminate_test() ->
  ?assertEqual(ok, rabbitmq_aws:terminate(foo, bar)).

code_change_test() ->
  ?assertEqual({ok, {state, denial}}, rabbitmq_aws:code_change(foo, bar, {state, denial})).

endpoint_test_() ->
  [
    {"specified", fun() ->
      Region = "us-east-3",
      Service = "dynamodb",
      Path = "/",
      Host = "localhost:32767",
      Expectation = "https://localhost:32767/",
      ?assertEqual(Expectation, rabbitmq_aws:endpoint(#state{region = Region}, Host, Service, Path))
     end},
    {"unspecified", fun() ->
      Region = "us-east-3",
      Service = "dynamodb",
      Path = "/",
      Host = undefined,
      Expectation = "https://dynamodb.us-east-3.amazonaws.com/",
      ?assertEqual(Expectation, rabbitmq_aws:endpoint(#state{region = Region}, Host, Service, Path))
     end}
  ].

endpoint_host_test_() ->
  [
    {"dynamodb service", fun() ->
      Expectation = "dynamodb.us-west-2.amazonaws.com",
      ?assertEqual(Expectation, rabbitmq_aws:endpoint_host("us-west-2", "dynamodb"))
     end}
  ].

cn_endpoint_host_test_() ->
  [
    {"s3", fun() ->
      Expectation = "s3.cn-north-1.amazonaws.com.cn",
      ?assertEqual(Expectation, rabbitmq_aws:endpoint_host("cn-north-1", "s3"))
     end},
   {"s3", fun() ->
      Expectation = "s3.cn-northwest-1.amazonaws.com.cn",
      ?assertEqual(Expectation, rabbitmq_aws:endpoint_host("cn-northwest-1", "s3"))
     end}
  ].

expired_credentials_test_() ->
  {
    foreach,
    fun () ->
      meck:new(calendar, [passthrough, unstick]),
      [calendar]
    end,
    fun meck:unload/1,
    [
      {"true", fun() ->
        Value = {{2016, 4, 1}, {12, 0, 0}},
        Expectation = true,
        meck:expect(calendar, local_time_to_universal_time_dst, fun(_) -> [{{2016, 4, 1}, {12, 0, 0}}] end),
        ?assertEqual(Expectation, rabbitmq_aws:expired_credentials(Value)),
        meck:validate(calendar)
       end},
      {"false", fun() ->
        Value = {{2016,5, 1}, {16, 30, 0}},
        Expectation = false,
        meck:expect(calendar, local_time_to_universal_time_dst, fun(_) -> [{{2016, 4, 1}, {12, 0, 0}}] end),
        ?assertEqual(Expectation, rabbitmq_aws:expired_credentials(Value)),
        meck:validate(calendar)
       end},
      {"undefined", fun() ->
        ?assertEqual(false, rabbitmq_aws:expired_credentials(undefined))
       end}
    ]
  }.

format_response_test_() ->
  [
    {"ok", fun() ->
      Response = {ok, {{"HTTP/1.1", 200, "Ok"}, [{"Content-Type", "text/xml"}], "<test>Value</test>"}},
      Expectation = {ok, {[{"Content-Type", "text/xml"}], [{"test", "Value"}]}},
      ?assertEqual(Expectation, rabbitmq_aws:format_response(Response))
     end},
    {"error", fun() ->
      Response = {ok, {{"HTTP/1.1", 500, "Internal Server Error"}, [{"Content-Type", "text/xml"}], "<error>Boom</error>"}},
      Expectation = {error, "Internal Server Error", {[{"Content-Type", "text/xml"}], [{"error", "Boom"}]}},
      ?assertEqual(Expectation, rabbitmq_aws:format_response(Response))
     end}
  ].


gen_server_call_test_() ->
  {
    foreach,
    fun () ->
      % We explicitely set a few defaults, in case the caller has
      % something in ~/.aws.
      os:putenv("AWS_DEFAULT_REGION", "us-west-3"),
      os:putenv("AWS_ACCESS_KEY_ID", "Sésame"),
      os:putenv("AWS_SECRET_ACCESS_KEY", "ouvre-toi"),
      meck:new(httpc, []),
      [httpc]
    end,
    fun (Mods) ->
      meck:unload(Mods),
      os:unsetenv("AWS_DEFAULT_REGION"),
      os:unsetenv("AWS_ACCESS_KEY_ID"),
      os:unsetenv("AWS_SECRET_ACCESS_KEY")
    end,
    [
      {
        "request",
        fun() ->
          State = #state{access_key = "AKIDEXAMPLE",
                         secret_access_key = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
                         region = "us-east-1"},
          Service = "ec2",
          Method = get,
          Headers = [],
          Path = "/?Action=DescribeTags&Version=2015-10-01",
          Body = "",
          Options = [],
          Host = undefined,
          meck:expect(httpc, request,
            fun(get, {"https://ec2.us-east-1.amazonaws.com/?Action=DescribeTags&Version=2015-10-01", _Headers}, _Options, []) ->
                {ok, {{"HTTP/1.0", 200, "OK"}, [{"content-type", "application/json"}],  "{\"pass\": true}"}}
            end),
          Expectation = {reply, {ok, {[{"content-type", "application/json"}], [{"pass", true}]}}, State},
          Result = rabbitmq_aws:handle_call({request, Service, Method, Headers, Path, Body, Options, Host}, eunit, State),
          ?assertEqual(Expectation, Result),
          meck:validate(httpc)
        end
      },
      {
        "get_state",
        fun() ->
          State = #state{access_key = "AKIDEXAMPLE",
                         secret_access_key = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
                         region = "us-east-1"},
          ?assertEqual({reply, {ok, State}, State},
                       rabbitmq_aws:handle_call(get_state, eunit, State))
        end
      },
      {
        "refresh_credentials",
        fun() ->
          State = #state{access_key = "AKIDEXAMPLE",
                         secret_access_key = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
                         region = "us-east-1"},
          State2 = #state{access_key = "AKIDEXAMPLE2",
                          secret_access_key = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY2",
                          region = "us-east-1",
                          security_token = "AQoEXAMPLEH4aoAH0gNCAPyJxz4BlCFFxWNE1OPTgk5TthT+FvwqnKwRcOIfrRh3c/L2",
                          expiration = calendar:local_time()},
          meck:new(rabbitmq_aws_config, [passthrough]),
          meck:expect(rabbitmq_aws_config, credentials,
            fun() ->
              {ok,
                State2#state.access_key,
                State2#state.secret_access_key,
                State2#state.expiration,
                State2#state.security_token}
            end),
          ?assertEqual({reply, ok, State2}, rabbitmq_aws:handle_call(refresh_credentials, eunit, State)),
          meck:validate(rabbitmq_aws_config),
          meck:unload(rabbitmq_aws_config)
        end
      },
      {
        "set_credentials",
        fun() ->
          State = #state{access_key = "AKIDEXAMPLE",
                         secret_access_key = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
                         region = "us-west-3"},
          ?assertEqual({reply, ok, State},
                       rabbitmq_aws:handle_call({set_credentials,
                                              State#state.access_key,
                                              State#state.secret_access_key}, eunit, #state{region = "us-west-3"}))
        end
      },
      {
        "set_region",
        fun() ->
          State = #state{access_key = "Sésame",
                         secret_access_key = "ouvre-toi",
                         region = "us-east-5"},
          ?assertEqual({reply, ok, State},
                       rabbitmq_aws:handle_call({set_region, "us-east-5"}, eunit, #state{access_key = "Sésame",
                                                                                         secret_access_key = "ouvre-toi"}))
        end
      }
    ]
  }.

get_content_type_test_() ->
  [
    {"from headers caps", fun() ->
      Headers = [{"Content-Type", "text/xml"}],
      Expectation = {"text", "xml"},
      ?assertEqual(Expectation, rabbitmq_aws:get_content_type(Headers))
     end},
    {"from headers lower", fun() ->
      Headers = [{"content-type", "text/xml"}],
      Expectation = {"text", "xml"},
      ?assertEqual(Expectation, rabbitmq_aws:get_content_type(Headers))
    end}
  ].

has_credentials_test_() ->
  [
    {"true", fun() ->
      ?assertEqual(true, rabbitmq_aws:has_credentials(#state{access_key = "TESTVALUE1"}))
     end},
    {"false", fun() ->
      ?assertEqual(false, rabbitmq_aws:has_credentials(#state{error = "ERROR"}))
     end}
  ].


local_time_test_() ->
  {
    foreach,
    fun () ->
      meck:new(calendar, [passthrough, unstick]),
      [calendar]
    end,
    fun meck:unload/1,
    [
      {"value", fun() ->
        Value = {{2016, 5, 1}, {12, 0, 0}},
        meck:expect(calendar, local_time_to_universal_time_dst, fun(_) -> [Value] end),
        ?assertEqual(Value, rabbitmq_aws:local_time()),
        meck:validate(calendar)
       end}
    ]
  }.


maybe_decode_body_test_() ->
  [
    {"application/x-amz-json-1.0", fun() ->
      ContentType = {"application", "x-amz-json-1.0"},
      Body = "{\"test\": true}",
      Expectation = [{"test", true}],
      ?assertEqual(Expectation, rabbitmq_aws:maybe_decode_body(ContentType, Body))
     end},
    {"application/json", fun() ->
      ContentType = {"application", "json"},
      Body = "{\"test\": true}",
      Expectation = [{"test", true}],
      ?assertEqual(Expectation, rabbitmq_aws:maybe_decode_body(ContentType, Body))
     end},
    {"text/xml", fun() ->
      ContentType = {"text", "xml"},
      Body = "<test><node>value</node></test>",
      Expectation = [{"test", [{"node", "value"}]}],
      ?assertEqual(Expectation, rabbitmq_aws:maybe_decode_body(ContentType, Body))
     end},
    {"text/html [unsupported]", fun() ->
      ContentType = {"text", "html"},
      Body = "<html><head></head><body></body></html>",
      ?assertEqual(Body, rabbitmq_aws:maybe_decode_body(ContentType, Body))
     end}
  ].

parse_content_type_test_() ->
  [
    {"application/x-amz-json-1.0", fun() ->
      Expectation = {"application", "x-amz-json-1.0"},
      ?assertEqual(Expectation, rabbitmq_aws:parse_content_type("application/x-amz-json-1.0"))
     end},
    {"application/xml", fun() ->
      Expectation = {"application", "xml"},
      ?assertEqual(Expectation, rabbitmq_aws:parse_content_type("application/xml"))
     end},
    {"text/xml;charset=UTF-8", fun() ->
      Expectation = {"text", "xml"},
      ?assertEqual(Expectation, rabbitmq_aws:parse_content_type("text/xml"))
     end}
  ].


perform_request_test_() ->
  {
    foreach,
    fun () ->
      meck:new(httpc, []),
      meck:new(rabbitmq_aws_config, []),
      [httpc, rabbitmq_aws_config]
    end,
    fun meck:unload/1,
    [
      {
        "has_credentials true",
        fun() ->
          State = #state{access_key = "AKIDEXAMPLE",
                         secret_access_key = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
                         region = "us-east-1"},
          Service = "ec2",
          Method = get,
          Headers = [],
          Path = "/?Action=DescribeTags&Version=2015-10-01",
          Body = "",
          Options = [],
          Host = undefined,
          ExpectURI = "https://ec2.us-east-1.amazonaws.com/?Action=DescribeTags&Version=2015-10-01",
          meck:expect(httpc, request,
                      fun(get, {URI, _Headers}, _Options, []) ->
                        case URI of
                          ExpectURI ->
                            {ok, {{"HTTP/1.0", 200, "OK"}, [{"content-type", "application/json"}],  "{\"pass\": true}"}};
                          _ ->
                            {ok, {{"HTTP/1.0", 400, "RequestFailure", [{"content-type", "application/json"}],  "{\"pass\": false}"}}}
                        end
                      end),
          Expectation = {{ok, {[{"content-type", "application/json"}], [{"pass", true}]}}, State},
          Result = rabbitmq_aws:perform_request(State, Service, Method, Headers, Path, Body, Options, Host),
          ?assertEqual(Expectation, Result),
          meck:validate(httpc)
        end},
      {
        "has_credentials false",
        fun() ->
          State = #state{region = "us-east-1"},
          Service = "ec2",
          Method = get,
          Headers = [],
          Path = "/?Action=DescribeTags&Version=2015-10-01",
          Body = "",
          Options = [],
          Host = undefined,
          meck:expect(httpc, request, fun(get, {_URI, _Headers}, _Options, []) -> {ok, {{"HTTP/1.0", 400, "RequestFailure"}, [{"content-type", "application/json"}],  "{\"pass\": false}"}} end),
          Expectation = {{error, {credentials, State#state.error}}, State},
          Result = rabbitmq_aws:perform_request(State, Service, Method, Headers, Path, Body, Options, Host),
          ?assertEqual(Expectation, Result),
          meck:validate(httpc)
        end
      },
      {
        "has expired credentials",
        fun() ->
          State = #state{access_key = "AKIDEXAMPLE",
                         secret_access_key = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
                         region = "us-east-1",
                         security_token = "AQoEXAMPLEH4aoAH0gNCAPyJxz4BlCFFxWNE1OPTgk5TthT+FvwqnKwRcOIfrRh3c/L",
                         expiration = {{1973, 1, 1}, {10, 20, 30}}},
          Service = "ec2",
          Method = get,
          Headers = [],
          Path = "/?Action=DescribeTags&Version=2015-10-01",
          Body = "",
          Options = [],
          Host = undefined,
          meck:expect(rabbitmq_aws_config, credentials, fun() -> {error, unit_test} end),
          Expectation = {{error, {credentials, "Credentials expired!"}}, State#state{error = "Credentials expired!"}},
          Result = rabbitmq_aws:perform_request(State, Service, Method, Headers, Path, Body, Options, Host),
          ?assertEqual(Expectation, Result),
          meck:validate(rabbitmq_aws_config)
        end
      },
      {
        "creds_error",
        fun() ->
          State = #state{error=unit_test},
          Expectation = {{error, {credentials, State#state.error}}, State},
          ?assertEqual(Expectation, rabbitmq_aws:perform_request_creds_error(State))
        end}
    ]
  }.

sign_headers_test_() ->
  {
    foreach,
    fun () ->
      meck:new(calendar, [passthrough, unstick]),
      [calendar]
    end,
    fun meck:unload/1,
    [
      {"with security token", fun() ->
        Value = {{2016, 5, 1}, {12, 0, 0}},
        meck:expect(calendar, local_time_to_universal_time_dst, fun(_) -> [Value] end),
        State = #state{access_key = "AKIDEXAMPLE",
                       secret_access_key = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
                       security_token = "AQoEXAMPLEH4aoAH0gNCAPyJxz4BlCFFxWNE1OPTgk5TthT+FvwqnKwRcOIfrRh3c/L",
                       region = "us-east-1"},
        Service = "ec2",
        Method = get,
        Headers = [],
        Body = "",
        URI = "http://ec2.us-east-1.amazonaws.com/?Action=DescribeTags&Version=2015-10-01",
        Expectation = [{"authorization", "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20160501/us-east-1/ec2/aws4_request, SignedHeaders=content-length;date;host;x-amz-content-sha256;x-amz-security-token, Signature=62d10b4897f7d05e4454b75895b5e372f6c2eb6997943cd913680822e94c6999"},
                       {"content-length","0"},
                       {"date","20160501T120000Z"}, {"host","ec2.us-east-1.amazonaws.com"},
                       {"x-amz-content-sha256", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"},
                       {"x-amz-security-token", "AQoEXAMPLEH4aoAH0gNCAPyJxz4BlCFFxWNE1OPTgk5TthT+FvwqnKwRcOIfrRh3c/L"}],
        ?assertEqual(Expectation, rabbitmq_aws:sign_headers(State, Service, Method, URI, Headers, Body)),
        meck:validate(calendar)
     end}
    ]
  }.

api_get_request_test_() ->
  {
    foreach,
    fun () ->
      meck:new(httpc, []),
      meck:new(rabbitmq_aws_config, []),
      [httpc, rabbitmq_aws_config]
    end,
    fun meck:unload/1,
    [
      {"AWS service API request succeeded",
        fun() ->
          State = #state{access_key = "ExpiredKey",
                         secret_access_key = "ExpiredAccessKey",
                         region = "us-east-1",
                         expiration = {{3016, 4, 1}, {12, 0, 0}}},
          meck:expect(httpc, request, 4, {ok, {{"HTTP/1.0", 200, "OK"}, [{"content-type", "application/json"}], "{\"data\": \"value\"}"}}),
          {ok, Pid} = rabbitmq_aws:start_link(),
          rabbitmq_aws:set_region("us-east-1"),
          rabbitmq_aws:set_credentials(State),
          Result = rabbitmq_aws:api_get_request("AWS", "API"),
          ok = gen_server:stop(Pid),
          ?assertEqual({ok, [{"data","value"}]}, Result),
          meck:validate(httpc)
        end
      },
      {"AWS service API request failed - credentials",
        fun() ->
          meck:expect(rabbitmq_aws_config, credentials, 0, {error, undefined}),
          {ok, Pid} = rabbitmq_aws:start_link(),
          rabbitmq_aws:set_region("us-east-1"),
          Result = rabbitmq_aws:api_get_request("AWS", "API"),
          ok = gen_server:stop(Pid),
          ?assertEqual({error, credentials}, Result)
        end
      },
      {"AWS service API request failed - API error",
        fun() ->
          State = #state{access_key = "ExpiredKey",
                         secret_access_key = "ExpiredAccessKey",
                         region = "us-east-1",
                         expiration = {{3016, 4, 1}, {12, 0, 0}}},
          meck:expect(httpc, request, 4, {error, "invalid input"}),
          {ok, Pid} = rabbitmq_aws:start_link(),
          rabbitmq_aws:set_region("us-east-1"),
          rabbitmq_aws:set_credentials(State),
          Result = rabbitmq_aws:api_get_request("AWS", "API"),
          ok = gen_server:stop(Pid),
          ?assertEqual({error, "invalid input"}, Result),
          meck:validate(httpc)
        end
      }
    ]
  }.

ensure_credentials_valid_test_() ->
  {
    foreach,
    fun () ->
      meck:new(rabbitmq_aws_config, []),
      [rabbitmq_aws_config]
    end,
    fun meck:unload/1,
    [
      {"expired credentials are refreshed",
        fun() ->
          State = #state{access_key = "ExpiredKey",
                        secret_access_key = "ExpiredAccessKey",
                        region = "us-east-1",
                        expiration = {{2016, 4, 1}, {12, 0, 0}}},
          State2 = #state{access_key = "NewKey",
                          secret_access_key = "NewAccessKey",
                          region = "us-east-1",
                          expiration = {{3016, 4, 1}, {12, 0, 0}}},

          meck:expect(rabbitmq_aws_config, credentials,
            fun() ->
              {ok,
                State2#state.access_key,
                State2#state.secret_access_key,
                State2#state.expiration,
                State2#state.security_token}
            end),
          {ok, Pid} = rabbitmq_aws:start_link(),
          rabbitmq_aws:set_region("us-east-1"),
          rabbitmq_aws:set_credentials(State),
          Result = rabbitmq_aws:ensure_credentials_valid(),
          Credentials = gen_server:call(Pid, get_state),
          ok = gen_server:stop(Pid),
          ?assertEqual(ok, Result),
          ?assertEqual(Credentials, {ok, State2}),
          meck:validate(rabbitmq_aws_config)
        end},
      {"valid credentials are returned",
        fun() ->
          State = #state{access_key = "GoodKey",
                         secret_access_key = "GoodAccessKey",
                         region = "us-east-1",
                         expiration = {{3016, 4, 1}, {12, 0, 0}}},
          {ok, Pid} = rabbitmq_aws:start_link(),
          rabbitmq_aws:set_region("us-east-1"),
          rabbitmq_aws:set_credentials(State),
          Result = rabbitmq_aws:ensure_credentials_valid(),
          Credentials = gen_server:call(Pid, get_state),
          ok = gen_server:stop(Pid),
          ?assertEqual(ok, Result),
          ?assertEqual(Credentials, {ok, State}),
          meck:validate(rabbitmq_aws_config)
        end},
      {"load credentials if missing",
        fun() ->
          State = #state{access_key = "GoodKey",
                         secret_access_key = "GoodAccessKey",
                         region = "us-east-1",
                         expiration = {{3016, 4, 1}, {12, 0, 0}}},
          meck:expect(rabbitmq_aws_config, credentials,
            fun() ->
              {ok,
                State#state.access_key,
                State#state.secret_access_key,
                State#state.expiration,
                State#state.security_token}
            end),
          {ok, Pid} = rabbitmq_aws:start_link(),
          rabbitmq_aws:set_region("us-east-1"),
          Result = rabbitmq_aws:ensure_credentials_valid(),
          Credentials = gen_server:call(Pid, get_state),
          ok = gen_server:stop(Pid),
          ?assertEqual(ok, Result),
          ?assertEqual(Credentials, {ok, State}),
          meck:validate(rabbitmq_aws_config)
        end}
    ]
  }.

expired_imdsv2_token_test_() ->
  [
    {"imdsv2 token is valid",
      fun() ->
        [Value] = calendar:local_time_to_universal_time_dst(calendar:local_time()),
        Now = calendar:datetime_to_gregorian_seconds(Value),
        Imdsv2Token = #imdsv2token{token = "value", expiration = Now + 100},
        ?assertEqual(false, rabbitmq_aws:expired_imdsv2_token(Imdsv2Token))
      end
    },
    {"imdsv2 token is expired",
      fun() ->
        [Value] = calendar:local_time_to_universal_time_dst(calendar:local_time()),
        Now = calendar:datetime_to_gregorian_seconds(Value),
        Imdsv2Token = #imdsv2token{token = "value", expiration = Now - 100},
        ?assertEqual(true, rabbitmq_aws:expired_imdsv2_token(Imdsv2Token))
      end
    },
    {"imdsv2 token is not yet initialized",
      fun() ->
        ?assertEqual(true, rabbitmq_aws:expired_imdsv2_token(undefined))
      end
    },
    {"imdsv2 token is undefined",
      fun() ->
        Imdsv2Token = #imdsv2token{token = undefined, expiration = undefined},
        ?assertEqual(true, rabbitmq_aws:expired_imdsv2_token(Imdsv2Token))
      end
    }
  ].
