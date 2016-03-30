-module(httpc_aws_config_tests).

-include_lib("eunit/include/eunit.hrl").

%% Return the parsed configuration file data
config_file_data_test() ->
  os:putenv("AWS_CONFIG_FILE",
            filename:join([filename:absname("."), "test",
                           "test_aws_config.ini"])),
  io:format("AWS_CONFIG_FILE: ~p~n", [os:getenv("AWS_CONFIG_FILE")]),
  Expectation = [
    {"default",
      [{aws_access_key_id, "default-key"},
       {aws_secret_access_key, "default-access-key"},
       {region, "us-east1"}]},
    {"profile testing",
      [{aws_access_key_id, "foo1"},
       {aws_secret_access_key, "bar2"},
       {region, "us-west-2"},
       {s3, [{max_concurrent_requests, 10},
             {max_queue_size, 1000}]}]}
  ],
  ?assertEqual(Expectation,
               httpc_aws_config:config_file_data()).

%% Return the default configuration when the environment variable is set
config_file_test() ->
  os:putenv("AWS_CONFIG_FILE", "/etc/aws/config"),
  ?assertEqual("/etc/aws/config",
               httpc_aws_config:config_file()).

%% Return the default configuration path based upon the user's home directory
config_file_no_env_var_test() ->
  os:unsetenv("AWS_CONFIG_FILE"),
  os:putenv("HOME", "/home/gavinr"),
  ?assertEqual("/home/gavinr/.aws/config",
               httpc_aws_config:config_file()).

%% Return the parsed configuration file data
credentials_file_data_test() ->
  os:putenv("AWS_SHARED_CREDENTIALS_FILE",
            filename:join([filename:absname("."), "test",
                           "test_aws_credentials.ini"])),
  io:format("AWS_SHARED_CREDENTIALS_FILE: ~p~n", [os:getenv("AWS_SHARED_CREDENTIALS_FILE")]),
  Expectation = [
    {"default",
      [{aws_access_key_id, "foo1"},
       {aws_secret_access_key, "bar1"}]},
    {"development",
      [{aws_access_key_id, "foo2"},
       {aws_secret_access_key, "bar2"}]}
  ],
  ?assertEqual(Expectation,
               httpc_aws_config:credentials_file_data()).

%% Return the default configuration when the environment variable is set
credentials_file_test() ->
  os:putenv("AWS_SHARED_CREDENTIALS_FILE", "/etc/aws/credentials"),
  ?assertEqual("/etc/aws/credentials",
               httpc_aws_config:credentials_file()).

%% Return the default configuration path based upon the user's home directory
credentials_no_env_var_test() ->
  os:unsetenv("AWS_SHARED_CREDENTIALS_FILE"),
  os:putenv("HOME", "/home/gavinr"),
  ?assertEqual("/home/gavinr/.aws/credentials",
               httpc_aws_config:credentials_file()).


%% Return the home directory path based upon the HOME shell environment variable
home_path_test() ->
  os:putenv("HOME", "/home/gavinr"),
  ?assertEqual("/home/gavinr",
               httpc_aws_config:home_path()).

%% Test that the cwd is returned if HOME is not set in the shell environment
home_path_no_env_var_test() ->
  os:unsetenv("HOME"),
  ?assertEqual(filename:absname("."),
               httpc_aws_config:home_path()).

maybe_convert_number_null_is_0_test() ->
  ?assertEqual(0, httpc_aws_config:maybe_convert_number(null)).

maybe_convert_number_empty_list_is_0_test() ->
  ?assertEqual(0, httpc_aws_config:maybe_convert_number([])).

maybe_convert_number_empty_binary_test() ->
  ?assertEqual(123, httpc_aws_config:maybe_convert_number(<<"123">>)).

maybe_convert_number_float_test() ->
  ?assertEqual(123.456, httpc_aws_config:maybe_convert_number(<<"123.456">>)).

ini_file_data_file_doesnt_exist_test() ->
  ?assertEqual({error, enoent}, httpc_aws_config:ini_file_data(filename:join([filename:absname("."), "bad_path"]), false)).

ini_file_data_bad_path_test() ->
  ?assertEqual({error, enoent}, httpc_aws_config:ini_file_data(filename:join([filename:absname("."), "bad_path"]), true)).

read_file_bad_path_test() ->
  ?assertEqual({error, enoent}, httpc_aws_config:read_file(filename:join([filename:absname("."), "bad_path"]))).

read_file_bad_handle_test() ->
  {MegaSecs, Secs, MicroSecs} = now(),
  Name = lists:flatten(io_lib:format("~p-~p-~p.tmp", [MegaSecs, Secs, MicroSecs])),
  {ok, Handle} = file:open(Name, [write]),
  file:close(Handle),
  ?assertEqual({error,terminated}, httpc_aws_config:read_file(Handle, [])),
  file:delete(Name).
