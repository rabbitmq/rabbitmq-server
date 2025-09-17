-module(rabbitmq_aws_config_tests).

-include_lib("eunit/include/eunit.hrl").

%% Helper function to mock gun for IMDSv2 failure scenarios
mock_gun_imdsv2_failure() ->
    meck:expect(gun, open, fun(_, _, _) -> {ok, fake_conn} end),
    meck:expect(gun, await_up, fun(_, _) -> {ok, http} end),
    meck:expect(gun, get, fun(_, _, _) -> fake_stream end),
    meck:expect(gun, await, fun(_, _, _) -> {response, fin, 404, []} end),
    meck:expect(gun, close, fun(_) -> ok end).

-include("rabbitmq_aws.hrl").

config_file_test_() ->
    [
        {"from environment variable", fun() ->
            os:putenv("AWS_CONFIG_FILE", "/etc/aws/config"),
            ?assertEqual("/etc/aws/config", rabbitmq_aws_config:config_file())
        end},
        {"default without environment variable", fun() ->
            os:unsetenv("AWS_CONFIG_FILE"),
            os:putenv("HOME", "/home/rrabbit"),
            ?assertEqual(
                "/home/rrabbit/.aws/config",
                rabbitmq_aws_config:config_file()
            )
        end}
    ].

config_file_data_test_() ->
    [
        {"successfully parses ini", fun() ->
            setup_test_config_env_var(),
            Expectation = [
                {"default", [
                    {aws_access_key_id, "default-key"},
                    {aws_secret_access_key, "default-access-key"},
                    {region, "us-east-4"}
                ]},
                {"profile testing", [
                    {aws_access_key_id, "foo1"},
                    {aws_secret_access_key, "bar2"},
                    {s3, [
                        {max_concurrent_requests, 10},
                        {max_queue_size, 1000}
                    ]},
                    {region, "us-west-5"}
                ]},
                {"profile no-region", [
                    {aws_access_key_id, "foo2"},
                    {aws_secret_access_key, "bar3"}
                ]},
                {"profile only-key", [{aws_access_key_id, "foo3"}]},
                {"profile only-secret", [{aws_secret_access_key, "foo4"}]},
                {"profile bad-entry", [{aws_secret_access, "foo5"}]}
            ],
            ?assertEqual(
                Expectation,
                rabbitmq_aws_config:config_file_data()
            )
        end},
        {"file does not exist", fun() ->
            ?assertEqual(
                {error, enoent},
                rabbitmq_aws_config:ini_file_data(
                    filename:join([filename:absname("."), "bad_path"]), false
                )
            )
        end},
        {"file exists but path is invalid", fun() ->
            ?assertEqual(
                {error, enoent},
                rabbitmq_aws_config:ini_file_data(
                    filename:join([filename:absname("."), "bad_path"]), true
                )
            )
        end}
    ].

instance_metadata_test_() ->
    [
        {"instance role URL", fun() ->
            ?assertEqual(
                "http://169.254.169.254/latest/meta-data/iam/security-credentials",
                rabbitmq_aws_config:instance_role_url()
            )
        end},
        {"availability zone URL", fun() ->
            ?assertEqual(
                "http://169.254.169.254/latest/meta-data/placement/availability-zone",
                rabbitmq_aws_config:instance_availability_zone_url()
            )
        end},
        {"instance id URL", fun() ->
            ?assertEqual(
                "http://169.254.169.254/latest/meta-data/instance-id",
                rabbitmq_aws_config:instance_id_url()
            )
        end},
        {"arbitrary paths", fun() ->
            ?assertEqual(
                "http://169.254.169.254/a/b/c", rabbitmq_aws_config:instance_metadata_url("a/b/c")
            ),
            ?assertEqual(
                "http://169.254.169.254/a/b/c", rabbitmq_aws_config:instance_metadata_url("/a/b/c")
            )
        end}
    ].

credentials_file_test_() ->
    [
        {"from environment variable", fun() ->
            os:putenv("AWS_SHARED_CREDENTIALS_FILE", "/etc/aws/credentials"),
            ?assertEqual("/etc/aws/credentials", rabbitmq_aws_config:credentials_file())
        end},
        {"default without environment variable", fun() ->
            os:unsetenv("AWS_SHARED_CREDENTIALS_FILE"),
            os:putenv("HOME", "/home/rrabbit"),
            ?assertEqual(
                "/home/rrabbit/.aws/credentials",
                rabbitmq_aws_config:credentials_file()
            )
        end}
    ].

credentials_test_() ->
    {
        foreach,
        fun() ->
            meck:new(gun, []),
            meck:new(rabbitmq_aws, [passthrough]),
            reset_environment(),
            [gun, rabbitmq_aws]
        end,
        fun meck:unload/1,
        [
            {"from environment variables", fun() ->
                os:putenv("AWS_ACCESS_KEY_ID", "Sésame"),
                os:putenv("AWS_SECRET_ACCESS_KEY", "ouvre-toi"),
                ?assertEqual(
                    {ok, "Sésame", "ouvre-toi", undefined, undefined},
                    rabbitmq_aws_config:credentials()
                )
            end},
            {"from config file with default profile", fun() ->
                setup_test_config_env_var(),
                ?assertEqual(
                    {ok, "default-key", "default-access-key", undefined, undefined},
                    rabbitmq_aws_config:credentials()
                )
            end},
            {"with missing environment variable", fun() ->
                os:putenv("AWS_ACCESS_KEY_ID", "Sésame"),
                meck:sequence(rabbitmq_aws, ensure_imdsv2_token_valid, 0, "secret_imdsv2_token"),
                mock_gun_imdsv2_failure(),

                ?assertEqual(
                    {error, undefined},
                    rabbitmq_aws_config:credentials()
                )
            end},
            {"from config file with default profile", fun() ->
                setup_test_config_env_var(),
                ?assertEqual(
                    {ok, "default-key", "default-access-key", undefined, undefined},
                    rabbitmq_aws_config:credentials()
                )
            end},
            {"from config file with profile", fun() ->
                setup_test_config_env_var(),
                ?assertEqual(
                    {ok, "foo1", "bar2", undefined, undefined},
                    rabbitmq_aws_config:credentials("testing")
                )
            end},
            {"from config file with bad profile", fun() ->
                setup_test_config_env_var(),
                meck:expect(rabbitmq_aws, ensure_imdsv2_token_valid, 0, undefined),
                mock_gun_imdsv2_failure(),

                ?assertEqual(
                    {error, undefined},
                    rabbitmq_aws_config:credentials("bad-profile-name")
                )
            end},
            {"from credentials file with default profile", fun() ->
                setup_test_credentials_env_var(),

                ?assertEqual(
                    {ok, "foo1", "bar1", undefined, undefined},
                    rabbitmq_aws_config:credentials()
                )
            end},
            {"from credentials file with profile", fun() ->
                setup_test_credentials_env_var(),
                ?assertEqual(
                    {ok, "foo2", "bar2", undefined, undefined},
                    rabbitmq_aws_config:credentials("development")
                )
            end},
            {"from credentials file with bad profile", fun() ->
                setup_test_credentials_env_var(),
                meck:expect(rabbitmq_aws, ensure_imdsv2_token_valid, 0, undefined),
                mock_gun_imdsv2_failure(),

                ?assertEqual(
                    {error, undefined},
                    rabbitmq_aws_config:credentials("bad-profile-name")
                )
            end},
            {"from credentials file with only the key in profile", fun() ->
                setup_test_credentials_env_var(),
                meck:expect(rabbitmq_aws, ensure_imdsv2_token_valid, 0, undefined),
                mock_gun_imdsv2_failure(),

                ?assertEqual(
                    {error, undefined},
                    rabbitmq_aws_config:credentials("only-key")
                )
            end},
            {"from credentials file with only the value in profile", fun() ->
                setup_test_credentials_env_var(),
                meck:expect(rabbitmq_aws, ensure_imdsv2_token_valid, 0, undefined),
                mock_gun_imdsv2_failure(),

                ?assertEqual(
                    {error, undefined},
                    rabbitmq_aws_config:credentials("only-value")
                )
            end},
            {"from credentials file with missing keys in profile", fun() ->
                setup_test_credentials_env_var(),
                meck:expect(rabbitmq_aws, ensure_imdsv2_token_valid, 0, undefined),
                mock_gun_imdsv2_failure(),

                ?assertEqual(
                    {error, undefined},
                    rabbitmq_aws_config:credentials("bad-entry")
                )
            end},
            {"from instance metadata service", fun() ->
                CredsBody =
                    "{\n  \"Code\" : \"Success\",\n  \"LastUpdated\" : \"2016-03-31T21:51:49Z\",\n  \"Type\" : \"AWS-HMAC\",\n  \"AccessKeyId\" : \"ASIAIMAFAKEACCESSKEY\",\n  \"SecretAccessKey\" : \"2+t64tZZVaz0yp0x1G23ZRYn+FAKEyVALUEs/4qh\",\n  \"Token\" : \"FAKE//////////wEAK/TOKEN/VALUE=\",\n  \"Expiration\" : \"2016-04-01T04:13:28Z\"\n}",
                meck:expect(gun, open, fun(_, _, _) -> {ok, pid} end),
                meck:expect(gun, close, fun(_) -> ok end),
                meck:expect(gun, await_up, fun(_, _) -> {ok, protocol} end),
                meck:sequence(gun, get, 3, [stream_ref1, stream_ref2]),
                meck:sequence(
                    gun,
                    await,
                    3,
                    [
                        {response, nofin, 200, headers},
                        {response, nofin, 200, headers}
                    ]
                ),
                meck:sequence(
                    gun,
                    await_body,
                    3,
                    [
                        {ok, <<"Bob">>},
                        {ok, list_to_binary(CredsBody)}
                    ]
                ),
                meck:expect(rabbitmq_aws, ensure_imdsv2_token_valid, 0, undefined),
                Expectation =
                    {ok, "ASIAIMAFAKEACCESSKEY", "2+t64tZZVaz0yp0x1G23ZRYn+FAKEyVALUEs/4qh",
                        {{2016, 4, 1}, {4, 13, 28}}, "FAKE//////////wEAK/TOKEN/VALUE="},
                ?assertEqual(Expectation, rabbitmq_aws_config:credentials())
            end},
            {"with instance metadata service role error", fun() ->
                meck:expect(rabbitmq_aws, ensure_imdsv2_token_valid, 0, undefined),
                mock_gun_imdsv2_failure(),
                ?assertEqual({error, undefined}, rabbitmq_aws_config:credentials())
            end},
            {"with instance metadata service role http error", fun() ->
                meck:expect(rabbitmq_aws, ensure_imdsv2_token_valid, 0, undefined),
                meck:expect(gun, open, fun(_, _, _) -> {ok, pid} end),
                meck:expect(gun, close, fun(_) -> ok end),
                meck:expect(gun, await_up, fun(_, _) -> {ok, protocol} end),
                meck:expect(gun, get, fun(_, _, _) -> stream_ref end),
                meck:expect(gun, await, fun(_, _, _) -> {response, nofin, 500, headers} end),
                meck:expect(gun, await_body, fun(_, _, _) -> {ok, <<"Internal Server Error">>} end),
                ?assertEqual({error, undefined}, rabbitmq_aws_config:credentials())
            end},
            {"with instance metadata service credentials error", fun() ->
                meck:expect(rabbitmq_aws, ensure_imdsv2_token_valid, 0, undefined),
                meck:expect(gun, open, fun(_, _, _) -> {ok, pid} end),
                meck:expect(gun, close, fun(_) -> ok end),
                meck:expect(gun, await_up, fun(_, _) -> {ok, protocol} end),
                meck:sequence(gun, get, 3, [stream_ref1, stream_ref2]),
                meck:sequence(
                    gun,
                    await,
                    3,
                    [
                        {response, nofin, 200, headers},
                        {error, timeout}
                    ]
                ),
                meck:expect(gun, await_body, fun(_, _, _) -> {ok, <<"Bob">>} end),
                ?assertEqual({error, undefined}, rabbitmq_aws_config:credentials())
            end},
            {"with instance metadata service credentials not found", fun() ->
                meck:expect(rabbitmq_aws, ensure_imdsv2_token_valid, 0, undefined),
                meck:expect(gun, open, fun(_, _, _) -> {ok, pid} end),
                meck:expect(gun, close, fun(_) -> ok end),
                meck:expect(gun, await_up, fun(_, _) -> {ok, protocol} end),
                meck:sequence(gun, get, 3, [stream_ref1, stream_ref2]),
                meck:sequence(
                    gun,
                    await,
                    3,
                    [
                        {response, nofin, 200, headers},
                        {response, nofin, 404, headers}
                    ]
                ),
                meck:sequence(
                    gun,
                    await_body,
                    3,
                    [
                        {ok, <<"Bob">>},
                        {ok, <<"File Not Found">>}
                    ]
                ),
                ?assertEqual({error, undefined}, rabbitmq_aws_config:credentials())
            end}
        ]
    }.

home_path_test_() ->
    [
        {"with HOME", fun() ->
            os:putenv("HOME", "/home/rrabbit"),
            ?assertEqual(
                "/home/rrabbit",
                rabbitmq_aws_config:home_path()
            )
        end},
        {"without HOME", fun() ->
            os:unsetenv("HOME"),
            ?assertEqual(
                filename:absname("."),
                rabbitmq_aws_config:home_path()
            )
        end}
    ].

ini_format_key_test_() ->
    [
        {"when value is list", fun() ->
            ?assertEqual(test_key, rabbitmq_aws_config:ini_format_key("test_key"))
        end},
        {"when value is binary", fun() ->
            ?assertEqual({error, type}, rabbitmq_aws_config:ini_format_key(<<"test_key">>))
        end}
    ].

maybe_convert_number_test_() ->
    [
        {"when string contains an integer", fun() ->
            ?assertEqual(123, rabbitmq_aws_config:maybe_convert_number("123"))
        end},
        {"when string contains a float", fun() ->
            ?assertEqual(123.456, rabbitmq_aws_config:maybe_convert_number("123.456"))
        end},
        {"when string does not contain a number", fun() ->
            ?assertEqual("hello, world", rabbitmq_aws_config:maybe_convert_number("hello, world"))
        end}
    ].

parse_iso8601_test_() ->
    [
        {"parse test", fun() ->
            Value = "2016-05-19T18:25:23Z",
            Expectation = {{2016, 5, 19}, {18, 25, 23}},
            ?assertEqual(Expectation, rabbitmq_aws_config:parse_iso8601_timestamp(Value))
        end}
    ].

profile_test_() ->
    [
        {"from environment variable", fun() ->
            os:putenv("AWS_DEFAULT_PROFILE", "httpc-aws test"),
            ?assertEqual("httpc-aws test", rabbitmq_aws_config:profile())
        end},
        {"default without environment variable", fun() ->
            os:unsetenv("AWS_DEFAULT_PROFILE"),
            ?assertEqual("default", rabbitmq_aws_config:profile())
        end}
    ].

read_file_test_() ->
    [
        {"file does not exist", fun() ->
            ?assertEqual(
                {error, enoent},
                rabbitmq_aws_config:read_file(filename:join([filename:absname("."), "bad_path"]))
            )
        end}
    ].

region_test_() ->
    {
        foreach,
        fun() ->
            meck:new(gun, []),
            meck:new(rabbitmq_aws, [passthrough]),
            reset_environment(),
            [gun, rabbitmq_aws]
        end,
        fun meck:unload/1,
        [
            {"with environment variable", fun() ->
                os:putenv("AWS_DEFAULT_REGION", "us-west-1"),
                ?assertEqual({ok, "us-west-1"}, rabbitmq_aws_config:region())
            end},
            {"with config file and specified profile", fun() ->
                setup_test_config_env_var(),
                ?assertEqual({ok, "us-west-5"}, rabbitmq_aws_config:region("testing"))
            end},
            {"with config file using default profile", fun() ->
                setup_test_config_env_var(),
                ?assertEqual({ok, "us-east-4"}, rabbitmq_aws_config:region())
            end},
            {"missing profile in config", fun() ->
                setup_test_config_env_var(),
                meck:expect(rabbitmq_aws, ensure_imdsv2_token_valid, 0, undefined),
                ?assertEqual({ok, ?DEFAULT_REGION}, rabbitmq_aws_config:region("no-region"))
            end},
            {"from instance metadata service", fun() ->
                meck:expect(rabbitmq_aws, ensure_imdsv2_token_valid, 0, undefined),
                meck:expect(gun, open, fun(_, _, _) -> {ok, pid} end),
                meck:expect(gun, close, fun(_) -> ok end),
                meck:expect(gun, await_up, fun(_, _) -> {ok, protocol} end),
                meck:expect(gun, get, fun(_, _, _) -> stream_ref end),
                meck:expect(gun, await, fun(_, _, _) -> {response, nofin, 200, headers} end),
                meck:expect(gun, await_body, fun(_, _, _) -> {ok, <<"us-west-1a">>} end),
                ?assertEqual({ok, "us-west-1"}, rabbitmq_aws_config:region())
            end},
            {"full lookup failure", fun() ->
                meck:expect(rabbitmq_aws, ensure_imdsv2_token_valid, 0, undefined),
                ?assertEqual({ok, ?DEFAULT_REGION}, rabbitmq_aws_config:region())
            end},
            {"http error failure", fun() ->
                meck:expect(rabbitmq_aws, ensure_imdsv2_token_valid, 0, undefined),
                meck:expect(gun, open, fun(_, _, _) -> {ok, pid} end),
                meck:expect(gun, close, fun(_) -> ok end),
                meck:expect(gun, await_up, fun(_, _) -> {ok, protocol} end),
                meck:expect(gun, get, fun(_, _, _) -> stream_ref end),
                meck:expect(gun, await, fun(_, _, _) -> {response, nofin, 500, headers} end),
                meck:expect(gun, await_body, fun(_, _, _) -> {ok, <<"Internal Server Error">>} end),
                ?assertEqual({ok, ?DEFAULT_REGION}, rabbitmq_aws_config:region())
            end}
        ]
    }.

instance_id_test_() ->
    {
        foreach,
        fun() ->
            meck:new(gun, []),
            meck:new(rabbitmq_aws, [passthrough]),
            reset_environment(),
            [gun, rabbitmq_aws]
        end,
        fun meck:unload/1,
        [
            {"get instance id successfully", fun() ->
                meck:expect(rabbitmq_aws, ensure_imdsv2_token_valid, 0, undefined),
                meck:expect(gun, open, fun(_, _, _) -> {ok, pid} end),
                meck:expect(gun, close, fun(_) -> ok end),
                meck:expect(gun, await_up, fun(_, _) -> {ok, protocol} end),
                meck:expect(gun, get, fun(_, _, _) -> stream_ref end),
                meck:expect(gun, await, fun(_, _, _) -> {response, nofin, 200, headers} end),
                meck:expect(gun, await_body, fun(_, _, _) -> {ok, <<"instance-id">>} end),
                ?assertEqual({ok, "instance-id"}, rabbitmq_aws_config:instance_id())
            end},
            {"getting instance id is rejected with invalid token error", fun() ->
                meck:expect(rabbitmq_aws, ensure_imdsv2_token_valid, 0, "invalid"),
                meck:expect(gun, open, fun(_, _, _) -> {ok, pid} end),
                meck:expect(gun, close, fun(_) -> ok end),
                meck:expect(gun, await_up, fun(_, _) -> {ok, protocol} end),
                meck:expect(gun, get, fun(_, _, _) -> stream_ref end),
                meck:expect(gun, await, fun(_, _, _) -> {response, nofin, 401, headers} end),
                meck:expect(gun, await_body, fun(_, _, _) -> {ok, <<"Invalid token">>} end),
                ?assertEqual({error, undefined}, rabbitmq_aws_config:instance_id())
            end},
            {"getting instance id is rejected with access denied error", fun() ->
                meck:expect(rabbitmq_aws, ensure_imdsv2_token_valid, 0, "expired token"),
                meck:expect(gun, open, fun(_, _, _) -> {ok, pid} end),
                meck:expect(gun, close, fun(_) -> ok end),
                meck:expect(gun, await_up, fun(_, _) -> {ok, protocol} end),
                meck:expect(gun, get, fun(_, _, _) -> stream_ref end),
                meck:expect(gun, await, fun(_, _, _) -> {response, nofin, 403, headers} end),
                meck:expect(gun, await_body, fun(_, _, _) -> {ok, <<"access denied">>} end),
                ?assertEqual({error, undefined}, rabbitmq_aws_config:instance_id())
            end}
        ]
    }.

load_imdsv2_token_test_() ->
    {
        foreach,
        fun() ->
            meck:new(gun, []),
            [gun]
        end,
        fun meck:unload/1,
        [
            {"fail to get imdsv2 token - timeout", fun() ->
                meck:expect(gun, open, fun(_, _, _) -> {error, timeout} end),
                ?assertEqual(undefined, rabbitmq_aws_config:load_imdsv2_token())
            end},
            {"fail to get imdsv2 token - PUT request is not valid", fun() ->
                meck:expect(gun, open, fun(_, _, _) -> {ok, pid} end),
                meck:expect(gun, close, fun(_) -> ok end),
                meck:expect(gun, await_up, fun(_, _) -> {ok, protocol} end),
                meck:expect(gun, put, fun(_, _, _, _) -> stream_ref end),
                meck:expect(gun, await, fun(_, _, _) -> {response, nofin, 400, headers} end),
                meck:expect(gun, await_body, fun(_, _, _) ->
                    {ok, <<"Missing or Invalid Parameters – The PUT request is not valid.">>}
                end),
                ?assertEqual(undefined, rabbitmq_aws_config:load_imdsv2_token())
            end},
            {"successfully get imdsv2 token from instance metadata service", fun() ->
                IMDSv2Token = "super_secret_token_value",
                meck:expect(gun, open, fun(_, _, _) -> {ok, pid} end),
                meck:expect(gun, close, fun(_) -> ok end),
                meck:expect(gun, await_up, fun(_, _) -> {ok, protocol} end),
                meck:expect(gun, put, fun(_, _, _, _) -> stream_ref end),
                meck:expect(gun, await, fun(_, _, _) -> {response, nofin, 200, headers} end),
                meck:expect(gun, await_body, fun(_, _, _) -> {ok, list_to_binary(IMDSv2Token)} end),
                ?assertEqual(IMDSv2Token, rabbitmq_aws_config:load_imdsv2_token())
            end}
        ]
    }.

maybe_imdsv2_token_headers_test_() ->
    {
        foreach,
        fun() ->
            meck:new(rabbitmq_aws, [passthrough]),
            [rabbitmq_aws]
        end,
        fun meck:unload/1,
        [
            {"imdsv2 token is not available", fun() ->
                meck:expect(rabbitmq_aws, ensure_imdsv2_token_valid, 0, undefined),
                ?assertEqual([], rabbitmq_aws_config:maybe_imdsv2_token_headers())
            end},

            {"imdsv2 is available", fun() ->
                IMDSv2Token = "super_secret_token_value ;)",
                meck:expect(rabbitmq_aws, ensure_imdsv2_token_valid, 0, IMDSv2Token),
                ?assertEqual(
                    [{"X-aws-ec2-metadata-token", IMDSv2Token}],
                    rabbitmq_aws_config:maybe_imdsv2_token_headers()
                )
            end}
        ]
    }.

reset_environment() ->
    os:unsetenv("AWS_ACCESS_KEY_ID"),
    os:unsetenv("AWS_DEFAULT_REGION"),
    os:unsetenv("AWS_SECRET_ACCESS_KEY"),
    setup_test_file_with_env_var("AWS_CONFIG_FILE", "bad_config.ini"),
    setup_test_file_with_env_var(
        "AWS_SHARED_CREDENTIALS_FILE",
        "bad_credentials.ini"
    ),
    meck:expect(gun, open, fun(_, _, _) -> {error, timeout} end).

setup_test_config_env_var() ->
    setup_test_file_with_env_var("AWS_CONFIG_FILE", "test_aws_config.ini").

setup_test_file_with_env_var(EnvVar, Filename) ->
    os:putenv(
        EnvVar,
        filename:join([
            filename:absname("."),
            "test",
            Filename
        ])
    ).

setup_test_credentials_env_var() ->
    setup_test_file_with_env_var(
        "AWS_SHARED_CREDENTIALS_FILE",
        "test_aws_credentials.ini"
    ).
