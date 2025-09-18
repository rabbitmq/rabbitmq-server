%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbitmq_aws_resource_fetcher).

%% API
-export([
    process_arns/0
]).

%% Export all for unit tests
-ifdef(TEST).
-compile(export_all).
-endif.

-include("rabbitmq_aws.hrl").
-include_lib("kernel/include/logger.hrl").

-spec process_arns() -> ok.
%% @doc Fetch certificate files from Amazon S3 and update application
%%      configuration when users don't have access to the local file system
%% @end
process_arns() ->
    case application:get_env(rabbit, aws_arns) of
        {ok, ArnList} ->
            lists:foreach(fun({_Key, Arn, Handler}) ->
                case resolve_arn(Arn) of
                    {ok, Content} ->
                        FilePath = write_to_file(Arn, Content),
                        case Handler of
                            oauth2_https_cacertfile ->
                                update_env(rabbitmq_auth_backend_oauth2, key_config, cacertfile, FilePath);
                            ssl_options_cacertfile ->
                                update_env(rabbit, ssl_options, cacertfile, FilePath);
                            ldap_ssl_options_cacertfile ->
                                update_env(rabbitmq_auth_backend_ldap, ssl_options, cacertfile, FilePath)
                        end,
                        ?LOG_INFO("Fetched ARN ~p and stored to ~p", [Arn, FilePath]);
                    {error, Reason} ->
                        ?LOG_ERROR("Failed to resolve ARN ~p: ~p", [Arn, Reason])
                end
            end, ArnList);
        _ ->
            ok
    end.

-spec update_env(atom(), atom(), atom(), string()) -> ok.
update_env(App, ConfigKey, Key, Value) ->
    Config = case application:get_env(App, ConfigKey) of
        {ok, ExistingConfig} -> ExistingConfig;
        undefined -> []
    end,
    NewConfig = lists:keystore(Key, 1, Config, {Key, Value}),
    application:set_env(App, ConfigKey, NewConfig).

-spec resolve_arn(string()) -> {ok, binary()} | {error, term()}.
resolve_arn(Arn) ->
    ?LOG_INFO("Attempting to resolve ARN: ~p", [Arn]),
    case parse_arn(Arn) of
        {ok, #{service := "s3"}} ->
            fetch_s3_object(Arn);
        {ok, #{service := Service}} ->
            ?LOG_ERROR("Unsupported service: ~p", [Service]),
            {error, {unsupported_service, Service}};
        {error, _} = Error ->
            ?LOG_ERROR("Failed to parse ARN ~p: ~p", [Arn, Error]),
            Error
    end.

-spec parse_arn(string()) -> {ok, map()} | {error, term()}.
parse_arn(Arn) ->
    ArnBin = list_to_binary(Arn),
    case binary:split(ArnBin, <<":">>, [global]) of
        [<<"arn">>, Partition, Service, Region, Account, Resource] ->
            {ok, #{
                partition => binary_to_list(Partition),
                service => binary_to_list(Service),
                region => binary_to_list(Region),
                account => binary_to_list(Account),
                resource => binary_to_list(Resource)
            }};
        _ ->
            {error, invalid_arn_format}
    end.

-spec fetch_s3_object(string()) -> {ok, binary()} | {error, term()}.
fetch_s3_object(Arn) ->
    {ok, #{resource := Resource}} = parse_arn(Arn),
    [Bucket | KeyParts] = string:tokens(Resource, "/"),
    Key = string:join(KeyParts, "/"),
    % Set region from environment variable
    {ok, Region} = rabbitmq_aws_config:region(),
    rabbitmq_aws:set_region(Region),
    % Refresh credentials to pick up environment variables
    rabbitmq_aws:refresh_credentials(),
    Path = "/" ++ Bucket ++ "/" ++ Key,
    case rabbitmq_aws:get("s3", Path) of
        {ok, {_Headers, Body}} ->
            {ok, Body};
        {error, Reason} ->
            {error, Reason};
        Other ->
            {error, {unexpected_response, Other}}
    end.

-spec write_to_file(string(), binary()) -> string().
write_to_file(Arn, Content) ->
    {ok, TempDir} = application:get_env(rabbitmq_aws, cacertfiles_download_path),
    Filename = re:replace(Arn, "[^a-zA-Z0-9]", "_", [global, {return, list}]),
    ok = filelib:ensure_dir(TempDir ++ "/"),
    FilePath = TempDir ++ "/" ++ Filename,
    ok = rabbit_file:write_file(FilePath, Content),
    FilePath.
