%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbitmq_aws_resource_fetcher_tests).

-include_lib("eunit/include/eunit.hrl").

parse_arn_s3_test() ->
    Arn = "arn:aws:s3:::private-ca-42/cacertfile.pem",
    {ok, Parsed} = rabbitmq_aws_resource_fetcher:parse_arn(Arn),
    ?assertEqual("aws", maps:get(partition, Parsed)),
    ?assertEqual("s3", maps:get(service, Parsed)),
    ?assertEqual("", maps:get(region, Parsed)),
    ?assertEqual("", maps:get(account, Parsed)),
    ?assertEqual("private-ca-42/cacertfile.pem", maps:get(resource, Parsed)).

parse_arn_s3_nested_path_test() ->
    Arn = "arn:aws:s3:::my-bucket/path/to/cert.pem",
    {ok, Parsed} = rabbitmq_aws_resource_fetcher:parse_arn(Arn),
    ?assertEqual("my-bucket/path/to/cert.pem", maps:get(resource, Parsed)).

parse_arn_invalid_test() ->
    ?assertEqual({error, invalid_arn_format}, rabbitmq_aws_resource_fetcher:parse_arn("invalid")).

parse_arn_empty_test() ->
    ?assertEqual({error, invalid_arn_format}, rabbitmq_aws_resource_fetcher:parse_arn("")).

parse_arn_incomplete_test() ->
    ?assertEqual({error, invalid_arn_format}, rabbitmq_aws_resource_fetcher:parse_arn("arn:aws:s3")).

update_env_test() ->
    application:set_env(rabbitmq_auth_backend_oauth2, key_config, [{cacertfile, "old_cert"}]),

    rabbitmq_aws_resource_fetcher:update_env(rabbitmq_auth_backend_oauth2, key_config, cacertfile, "/tmp/new_cert"),

    {ok, KeyConfig} = application:get_env(rabbitmq_auth_backend_oauth2, key_config),
    ?assertEqual("/tmp/new_cert", proplists:get_value(cacertfile, KeyConfig)).

update_env_new_key_test() ->
    application:set_env(rabbitmq_auth_backend_oauth2, key_config, [{other_key, "value"}]),

    rabbitmq_aws_resource_fetcher:update_env(rabbitmq_auth_backend_oauth2, key_config, cacertfile, "/tmp/cert"),

    {ok, KeyConfig} = application:get_env(rabbitmq_auth_backend_oauth2, key_config),
    ?assertEqual("/tmp/cert", proplists:get_value(cacertfile, KeyConfig)),
    ?assertEqual("value", proplists:get_value(other_key, KeyConfig)).

write_to_file_test() ->
    application:set_env(rabbitmq_aws, cacertfiles_download_path, "/tmp/test-certs"),

    Content = "test certificate content",
    Arn = "arn:aws:s3:::bucket/cert.pem",

    FilePath = rabbitmq_aws_resource_fetcher:write_to_file(Arn, Content),

    ?assert(filelib:is_file(FilePath)),

    {ok, ReadContent} = file:read_file(FilePath),
    ?assertEqual(Content, binary_to_list(ReadContent)),

    file:delete(FilePath).
