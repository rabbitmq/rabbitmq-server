-module(rabbitmq_aws_tests).

-include_lib("eunit/include/eunit.hrl").

-include("rabbitmq_aws.hrl").

%% Test helper functions
setup() ->
    application:ensure_all_started(rabbitmq_aws),
    ok.

teardown(_) ->
    application:stop(rabbitmq_aws),
    ok.

% Helper to populate test credentials
set_test_credentials(AccessKey, SecretKey) ->
    set_test_credentials(AccessKey, SecretKey, undefined, undefined).

set_test_credentials(AccessKey, SecretKey, SecurityToken, Expiration) ->
    Creds = #aws_credentials{
        access_key = AccessKey,
        secret_key = SecretKey,
        security_token = SecurityToken,
        expiration = Expiration
    },
    ets:insert(?AWS_CREDENTIALS_TABLE, {current, Creds}).

set_test_region(Region) ->
    ets:insert(?AWS_CONFIG_TABLE, {region, Region}).

init_test_() ->
    {foreach,
        fun() ->
            os:putenv("AWS_DEFAULT_REGION", "us-west-3"),
            meck:new(rabbitmq_aws_config, [passthrough]),
            setup()
        end,
        fun(_) ->
            teardown(ok),
            os:unsetenv("AWS_DEFAULT_REGION"),
            meck:unload(rabbitmq_aws_config)
        end,
        [
            {"ok", fun() ->
                os:putenv("AWS_ACCESS_KEY_ID", "Sésame"),
                os:putenv("AWS_SECRET_ACCESS_KEY", "ouvre-toi"),
                ?assertEqual(ok, rabbitmq_aws:refresh_credentials()),
                % Verify credentials were actually stored
                ?assertEqual(true, rabbitmq_aws:has_credentials()),
                {ok, AccessKey, SecretKey, SecurityToken, Region} = rabbitmq_aws:get_credentials(),
                ?assertEqual("Sésame", AccessKey),
                ?assertEqual("ouvre-toi", SecretKey),
                ?assertEqual(undefined, SecurityToken),
                ?assertEqual("us-west-3", Region),
                os:unsetenv("AWS_ACCESS_KEY_ID"),
                os:unsetenv("AWS_SECRET_ACCESS_KEY")
            end},
            {"error", fun() ->
                meck:expect(rabbitmq_aws_config, credentials, fun() -> {error, test_result} end),
                ?assertEqual(error, rabbitmq_aws:refresh_credentials()),
                % Verify no credentials were stored
                ?assertEqual(false, rabbitmq_aws:has_credentials()),
                meck:validate(rabbitmq_aws_config)
            end}
        ]}.

endpoint_test_() ->
    [
        {"specified", fun() ->
            Region = "us-east-3",
            Service = "dynamodb",
            Path = "/",
            Host = "localhost:32767",
            Expectation = "https://localhost:32767/",
            ?assertEqual(
                Expectation, rabbitmq_aws:endpoint(Region, Host, Service, Path)
            )
        end},
        {"unspecified", fun() ->
            Region = "us-east-3",
            Service = "dynamodb",
            Path = "/",
            Host = undefined,
            Expectation = "https://dynamodb.us-east-3.amazonaws.com/",
            ?assertEqual(
                Expectation, rabbitmq_aws:endpoint(Region, Host, Service, Path)
            )
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
        fun() ->
            meck:new(calendar, [passthrough, unstick]),
            [calendar]
        end,
        fun meck:unload/1,
        [
            {"true", fun() ->
                Value = {{2016, 4, 1}, {12, 0, 0}},
                Expectation = true,
                meck:expect(calendar, local_time_to_universal_time_dst, fun(_) ->
                    [{{2016, 4, 1}, {12, 0, 0}}]
                end),
                ?assertEqual(Expectation, rabbitmq_aws:expired_credentials(Value)),
                meck:validate(calendar)
            end},
            {"false", fun() ->
                Value = {{2016, 5, 1}, {16, 30, 0}},
                Expectation = false,
                meck:expect(calendar, local_time_to_universal_time_dst, fun(_) ->
                    [{{2016, 4, 1}, {12, 0, 0}}]
                end),
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
            Response =
                {ok, {
                    {"HTTP/1.1", 200, "Ok"},
                    [{<<"Content-Type">>, <<"text/xml">>}],
                    "<test>Value</test>"
                }},
            Expectation = {ok, {[{<<"Content-Type">>, <<"text/xml">>}], [{"test", "Value"}]}},
            ?assertEqual(Expectation, rabbitmq_aws:format_response(Response))
        end},
        {"error", fun() ->
            Response =
                {ok, {
                    {"HTTP/1.1", 500, "Internal Server Error"},
                    [{"Content-Type", "text/xml"}],
                    "<error>Boom</error>"
                }},
            Expectation =
                {error, "Internal Server Error",
                    {[{"Content-Type", "text/xml"}], [{"error", "Boom"}]}},
            ?assertEqual(Expectation, rabbitmq_aws:format_response(Response))
        end}
    ].

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
    {
        foreach,
        fun setup/0,
        fun teardown/1,
        [
            {"true", fun() ->
                set_test_credentials("TESTVALUE1", "SECRET"),
                ?assertEqual(true, rabbitmq_aws:has_credentials())
            end},
            {"false", fun() ->
                % No credentials set
                ?assertEqual(false, rabbitmq_aws:has_credentials())
            end}
        ]
    }.

local_time_test_() ->
    {
        foreach,
        fun() ->
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
        fun() ->
            setup(),
            meck:new(gun, []),
            [gun]
        end,
        fun(Mods) ->
            teardown(ok),
            meck:unload(Mods)
        end,
        [
            {
                "Successfull run",
                fun() ->
                    set_test_credentials("AKIDEXAMPLE", "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY"),
                    set_test_region("us-east-1"),
                    Service = "ec2",
                    Method = get,
                    Headers = [],
                    Path = "/?Action=DescribeTags&Version=2015-10-01",
                    Body = "",
                    Options = [],

                    meck:expect(gun, open, fun(_, _, _) -> {ok, pid} end),
                    meck:expect(gun, close, fun(_) -> ok end),
                    meck:expect(gun, await_up, fun(_, _) -> {ok, protocol} end),
                    meck:expect(
                        gun,
                        get,
                        fun(_Pid, "/?Action=DescribeTags&Version=2015-10-01", _Headers) -> nofin end
                    ),
                    meck:expect(
                        gun,
                        await,
                        fun(_Pid, _, _) ->
                            {response, nofin, 200, [{<<"content-type">>, <<"application/json">>}]}
                        end
                    ),
                    meck:expect(
                        gun,
                        await_body,
                        fun(_Pid, _, _) -> {ok, <<"{\"pass\": true}">>} end
                    ),

                    Expectation =
                        {ok, {[{<<"content-type">>, <<"application/json">>}], [{"pass", true}]}},
                    Result = rabbitmq_aws:request(Service, Method, Path, Body, Headers, Options),
                    ?assertEqual(Expectation, Result),
                    meck:validate(gun)
                end
            }
        ]
    }.

sign_headers_test_() ->
    {
        foreach,
        fun() ->
            meck:new(calendar, [passthrough, unstick]),
            [calendar]
        end,
        fun meck:unload/1,
        [
            {"with security token", fun() ->
                Value = {{2016, 5, 1}, {12, 0, 0}},
                meck:expect(calendar, local_time_to_universal_time_dst, fun(_) -> [Value] end),
                AccessKey = "AKIDEXAMPLE",
                SecretKey = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
                SecurityToken =
                    "AQoEXAMPLEH4aoAH0gNCAPyJxz4BlCFFxWNE1OPTgk5TthT+FvwqnKwRcOIfrRh3c/L",
                Region = "us-east-1",
                Service = "ec2",
                Method = get,
                Headers = [],
                Body = "",
                URI = "http://ec2.us-east-1.amazonaws.com/?Action=DescribeTags&Version=2015-10-01",
                Expectation = [
                    {"authorization",
                        "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20160501/us-east-1/ec2/aws4_request, SignedHeaders=content-length;date;host;x-amz-content-sha256;x-amz-security-token, Signature=62d10b4897f7d05e4454b75895b5e372f6c2eb6997943cd913680822e94c6999"},
                    {"content-length", "0"},
                    {"date", "20160501T120000Z"},
                    {"host", "ec2.us-east-1.amazonaws.com"},
                    {"x-amz-content-sha256",
                        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"},
                    {"x-amz-security-token",
                        "AQoEXAMPLEH4aoAH0gNCAPyJxz4BlCFFxWNE1OPTgk5TthT+FvwqnKwRcOIfrRh3c/L"}
                ],
                ?assertEqual(
                    Expectation,
                    rabbitmq_aws:sign_headers(
                        AccessKey,
                        SecretKey,
                        SecurityToken,
                        Region,
                        Service,
                        Method,
                        URI,
                        Headers,
                        Body,
                        undefined
                    )
                ),
                meck:validate(calendar)
            end}
        ]
    }.

api_get_request_test_() ->
    {
        foreach,
        fun() ->
            setup(),
            meck:new(gun, []),
            meck:new(rabbitmq_aws_config, []),
            [gun, rabbitmq_aws_config]
        end,
        fun(Mods) ->
            teardown(ok),
            meck:unload(Mods)
        end,
        [
            {"AWS service API request succeeded", fun() ->
                set_test_credentials("ExpiredKey", "ExpiredAccessKey", undefined, {
                    {3016, 4, 1}, {12, 0, 0}
                }),
                set_test_region("us-east-1"),

                meck:expect(gun, open, fun(_, _, _) -> {ok, pid} end),
                meck:expect(gun, close, fun(_) -> ok end),
                meck:expect(gun, await_up, fun(_, _) -> {ok, protocol} end),
                meck:expect(
                    gun,
                    get,
                    fun(_Pid, _Path, _Headers) -> nofin end
                ),
                meck:expect(
                    gun,
                    await,
                    fun(_Pid, _, _) ->
                        {response, nofin, 200, [{<<"content-type">>, <<"application/json">>}]}
                    end
                ),
                meck:expect(
                    gun,
                    await_body,
                    fun(_Pid, _, _) -> {ok, <<"{\"data\": \"value\"}">>} end
                ),

                Result = rabbitmq_aws:api_get_request("AWS", "API"),
                ?assertEqual({ok, [{"data", "value"}]}, Result),
                meck:validate(gun)
            end},
            {"AWS service API request failed - credentials", fun() ->
                set_test_region("us-east-1"),
                % No credentials set - should fail
                meck:expect(rabbitmq_aws_config, credentials, 0, {error, undefined}),

                Result = rabbitmq_aws:api_get_request("AWS", "API"),
                ?assertEqual({error, credentials}, Result)
            end},
            {"AWS service API request failed - API error with persistent failure", fun() ->
                set_test_credentials("ExpiredKey", "ExpiredAccessKey", undefined, {
                    {3016, 4, 1}, {12, 0, 0}
                }),
                set_test_region("us-east-1"),

                meck:expect(gun, open, fun(_, _, _) -> {ok, spawn(fun() -> ok end)} end),
                meck:expect(gun, close, fun(_) -> ok end),
                meck:expect(gun, await_up, fun(_, _) -> {ok, protocol} end),
                meck:expect(
                    gun,
                    get,
                    fun(_Pid, _Path, _Headers) -> nofin end
                ),
                meck:expect(
                    gun,
                    await,
                    fun(_Pid, _, _) -> {error, "network error"} end
                ),

                Result = rabbitmq_aws:api_get_request_with_retries("AWS", "API", 3, 1),
                ?assertEqual({error, "AWS service is unavailable"}, Result),
                meck:validate(gun)
            end},
            {"AWS service API request succeeded after a transient error", fun() ->
                set_test_credentials("ExpiredKey", "ExpiredAccessKey", undefined, {
                    {3016, 4, 1}, {12, 0, 0}
                }),
                set_test_region("us-east-1"),

                meck:expect(gun, open, fun(_, _, _) -> {ok, spawn(fun() -> ok end)} end),
                meck:expect(gun, close, fun(_) -> ok end),
                meck:expect(gun, await_up, fun(_, _) -> {ok, protocol} end),
                meck:expect(
                    gun,
                    get,
                    fun(_Pid, _Path, _Headers) -> nofin end
                ),

                %% meck:expect(gun, get, 3, meck:seq(
                %%             fun(_Pid, _Path, _Headers) -> {error, "network errors"} end),
                meck:expect(
                    gun,
                    await,
                    3,
                    meck:seq([
                        {error, "network error"},
                        {response, nofin, 500, [{<<"content-type">>, <<"application/json">>}]},
                        {response, nofin, 200, [{<<"content-type">>, <<"application/json">>}]}
                    ])
                ),

                meck:expect(
                    gun,
                    await_body,
                    3,
                    meck:seq([
                        {ok, <<"{\"error\": \"server error\"}">>},
                        {ok, <<"{\"data\": \"value\"}">>}
                    ])
                ),
                Result = rabbitmq_aws:api_get_request_with_retries("AWS", "API", 3, 1),
                ?assertEqual({ok, [{"data", "value"}]}, Result),
                meck:validate(gun)
            end}
        ]
    }.

ensure_credentials_valid_test_() ->
    {
        foreach,
        fun() ->
            setup(),
            meck:new(rabbitmq_aws_config, []),
            [rabbitmq_aws_config]
        end,
        fun(Mods) ->
            teardown(ok),
            meck:unload(Mods)
        end,
        [
            {"expired credentials are refreshed", fun() ->
                % Set expired credentials in ETS
                set_test_credentials("ExpiredKey", "ExpiredAccessKey", undefined, {
                    {2016, 4, 1}, {12, 0, 0}
                }),
                set_test_region("us-east-1"),

                % Mock config to return new credentials when refresh is called
                meck:expect(
                    rabbitmq_aws_config,
                    credentials,
                    fun() ->
                        {ok, "NewKey", "NewAccessKey", {{3016, 4, 1}, {12, 0, 0}}, undefined}
                    end
                ),

                Result = rabbitmq_aws:ensure_credentials_valid(),

                % Check that credentials were refreshed in ETS
                {ok, AccessKey, SecretKey, SecurityToken, Region} = rabbitmq_aws:get_credentials(),

                ?assertEqual(ok, Result),
                ?assertEqual("NewKey", AccessKey),
                ?assertEqual("NewAccessKey", SecretKey),
                ?assertEqual(undefined, SecurityToken),
                ?assertEqual("us-east-1", Region),
                meck:validate(rabbitmq_aws_config)
            end},
            {"valid credentials are returned", fun() ->
                % Set valid (non-expired) credentials in ETS
                set_test_credentials("GoodKey", "GoodAccessKey", undefined, {
                    {3016, 4, 1}, {12, 0, 0}
                }),
                set_test_region("us-east-1"),

                Result = rabbitmq_aws:ensure_credentials_valid(),

                % Check that credentials remain unchanged in ETS
                {ok, AccessKey, SecretKey, SecurityToken, Region} = rabbitmq_aws:get_credentials(),

                ?assertEqual(ok, Result),
                ?assertEqual("GoodKey", AccessKey),
                ?assertEqual("GoodAccessKey", SecretKey),
                ?assertEqual(undefined, SecurityToken),
                ?assertEqual("us-east-1", Region),
                meck:validate(rabbitmq_aws_config)
            end},
            {"load credentials if missing", fun() ->
                % Don't set any credentials in ETS - should trigger refresh
                set_test_region("us-east-1"),

                % Mock config to return credentials when refresh is called
                meck:expect(
                    rabbitmq_aws_config,
                    credentials,
                    fun() ->
                        {ok, "GoodKey", "GoodAccessKey", {{3016, 4, 1}, {12, 0, 0}}, undefined}
                    end
                ),

                Result = rabbitmq_aws:ensure_credentials_valid(),

                % Check that credentials were loaded into ETS
                {ok, AccessKey, SecretKey, SecurityToken, Region} = rabbitmq_aws:get_credentials(),

                ?assertEqual(ok, Result),
                ?assertEqual("GoodKey", AccessKey),
                ?assertEqual("GoodAccessKey", SecretKey),
                ?assertEqual(undefined, SecurityToken),
                ?assertEqual("us-east-1", Region),
                meck:validate(rabbitmq_aws_config)
            end}
        ]
    }.

expired_imdsv2_token_test_() ->
    [
        {"imdsv2 token is valid", fun() ->
            [Value] = calendar:local_time_to_universal_time_dst(calendar:local_time()),
            Now = calendar:datetime_to_gregorian_seconds(Value),
            Imdsv2Token = #imdsv2token{token = "value", expiration = Now + 100},
            ?assertEqual(false, rabbitmq_aws:expired_imdsv2_token(Imdsv2Token))
        end},
        {"imdsv2 token is expired", fun() ->
            [Value] = calendar:local_time_to_universal_time_dst(calendar:local_time()),
            Now = calendar:datetime_to_gregorian_seconds(Value),
            Imdsv2Token = #imdsv2token{token = "value", expiration = Now - 100},
            ?assertEqual(true, rabbitmq_aws:expired_imdsv2_token(Imdsv2Token))
        end},
        {"imdsv2 token is not yet initialized", fun() ->
            ?assertEqual(true, rabbitmq_aws:expired_imdsv2_token(undefined))
        end},
        {"imdsv2 token is undefined", fun() ->
            Imdsv2Token = #imdsv2token{token = undefined, expiration = undefined},
            ?assertEqual(true, rabbitmq_aws:expired_imdsv2_token(Imdsv2Token))
        end}
    ].
