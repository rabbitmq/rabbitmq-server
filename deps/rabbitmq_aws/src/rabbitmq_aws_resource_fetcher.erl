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

-include_lib("kernel/include/logger.hrl").

-spec process_arns() -> ok.
%% @doc Fetch certificate files from Amazon S3 and update application configuration to use them
%% @end
process_arns() ->
    process_arns(application:get_env(rabbitmq_aws, aws_arns)).

process_arns([]) ->
    ok;
process_arns([{_Key, Arn, Handler} | Rest]) ->
    case resolve_arn(Arn) of
        {ok, Content} ->
            ok = handle_content(Handler, Content);
        {error, Reason} ->
            ?LOG_ERROR("aws arn: failed to resolve ~tp: ~tp", [Arn, Reason])
    end,
    process_arns(Rest);
process_arns({ok, ArnList}) ->
    process_arns(ArnList);
process_arns(undefined) ->
    ok.

handle_content(oauth2_https_cacertfile, PemData) ->
    {ok, CaCertsDerEncoded} = decode_pem_data(PemData),
    ok = replace_in_env(rabbitmq_auth_backend_oauth2, key_config, cacertfile,
                        cacerts, CaCertsDerEncoded);
handle_content(ssl_options_cacertfile, PemData) ->
    {ok, CaCertsDerEncoded} = decode_pem_data(PemData),
    ok = replace_in_env(rabbit, ssl_options, cacertfile,
                        cacerts, CaCertsDerEncoded);
handle_content(ldap_ssl_options_cacertfile, PemData) ->
    {ok, CaCertsDerEncoded} = decode_pem_data(PemData),
    ok = replace_in_env(rabbitmq_auth_backend_ldap, ssl_options, cacertfile,
                        cacerts, CaCertsDerEncoded).

-spec resolve_arn(string()) -> {ok, binary()} | {error, term()}.
resolve_arn(Arn) ->
    ?LOG_INFO("aws arn: attempting to resolve: ~tp", [Arn]),
    case parse_arn(Arn) of
        {ok, #{service := "s3"}} ->
            fetch_s3_object(Arn);
        {ok, #{service := Service}} ->
            ?LOG_ERROR("aws arn: unsupported service: ~tp", [Service]),
            {error, {unsupported_service, Service}};
        {error, _} = Error ->
            ?LOG_ERROR("aws arn: failed to parse ~tp: ~tp", [Arn, Error]),
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
    case rabbitmq_aws_config:region() of
        {ok, Region} ->
            rabbitmq_aws:set_region(Region),
            fetch_s3_object_with_region(Bucket, Key);
        {error, RegionError} ->
            ?LOG_ERROR("aws arn: failed to get AWS region: ~tp", [RegionError]),
            {error, {region_lookup_failed, RegionError}}
    end.

-spec fetch_s3_object_with_region(string(), string()) -> {ok, binary()} | {error, term()}.
fetch_s3_object_with_region(Bucket, Key) ->
    rabbitmq_aws:refresh_credentials(),
    case rabbitmq_aws:has_credentials() of
        false ->
            ?LOG_ERROR("aws arn: no AWS credentials available for assume role operation"),
            {error, no_base_credentials};
        true ->
            case application:get_env(rabbitmq_aws, assume_role_arn) of
                {ok, RoleArn} ->
                    case assume_role(RoleArn) of
                        ok ->
                            fetch_s3_object_final(Bucket, Key);
                        {error, AssumeRoleReason} ->
                            ?LOG_ERROR("aws arn: failed to assume role ~tp: ~tp", [RoleArn, AssumeRoleReason]),
                            {error, {assume_role_failed, AssumeRoleReason}}
                    end;
                _ ->
                    % No assume role configured, use existing credentials
                    fetch_s3_object_final(Bucket, Key)
            end
    end.

-spec fetch_s3_object_final(string(), string()) -> {ok, binary()} | {error, term()}.
fetch_s3_object_final(Bucket, Key) ->
    Path = "/" ++ Bucket ++ "/" ++ Key,
    case rabbitmq_aws:get("s3", Path) of
        {ok, {_Headers, Body}} ->
            {ok, Body};
        {error, Reason} ->
            {error, Reason};
        Other ->
            {error, {unexpected_response, Other}}
    end.

-spec assume_role(string()) -> ok | {error, term()}.
assume_role(RoleArn) ->
    SessionName = "rabbitmq-resource-fetcher-" ++ integer_to_list(erlang:system_time(second)),
    Body = "Action=AssumeRole&RoleArn=" ++ uri_string:quote(RoleArn) ++
           "&RoleSessionName=" ++ uri_string:quote(SessionName) ++
           "&Version=2011-06-15",
    Headers = [
        {"content-type", "application/x-www-form-urlencoded"},
        {"accept", "application/json"}
    ],
    case rabbitmq_aws:post("sts", "/", Body, Headers) of
        {ok, {_Headers, ResponseBody}} ->
            parse_assume_role_response(ResponseBody);
        {error, Reason} ->
            {error, Reason}
    end.

-spec parse_assume_role_response(binary()) -> ok | {error, term()}.
parse_assume_role_response(Body) ->
    try
        [{"AssumeRoleResponse", ResponseData}] = Body,
        {"AssumeRoleResult", ResultData} = lists:keyfind("AssumeRoleResult", 1, ResponseData),
        {"Credentials", CredentialsData} = lists:keyfind("Credentials", 1, ResultData),
        {"AccessKeyId", AccessKey} = lists:keyfind("AccessKeyId", 1, CredentialsData),
        {"SecretAccessKey", SecretKey} = lists:keyfind("SecretAccessKey", 1, CredentialsData),
        {"SessionToken", SessionToken} = lists:keyfind("SessionToken", 1, CredentialsData),

        rabbitmq_aws:set_credentials(AccessKey, SecretKey, SessionToken),
        ok
    catch
        Class:Error:Stacktrace ->
            ?LOG_ERROR("aws arn: parse error: ~tp:~tp", [Class, Error]),
            ?LOG_ERROR("~tp", [Stacktrace]),
            {error, {parse_error, Error}}
    end.

-spec replace_in_env(atom(), atom(), atom(), atom(), any()) -> ok.
replace_in_env(App, ConfigKey, KeyToDelete, Key, Value) ->
    ok = delete_from_env(App, ConfigKey, KeyToDelete),
    ok = update_env(App, ConfigKey, Key, Value).

-spec delete_from_env(atom(), atom(), atom()) -> ok.
delete_from_env(App, ConfigKey, KeyToDelete) ->
    Config = case application:get_env(App, ConfigKey) of
        {ok, ExistingConfig} -> ExistingConfig;
        undefined -> []
    end,
    NewConfig = lists:keydelete(KeyToDelete, 1, Config),
    ok = application:set_env(App, ConfigKey, NewConfig).

-spec update_env(atom(), atom(), atom(), any()) -> ok.
update_env(App, ConfigKey, Key, Value) ->
    Config = case application:get_env(App, ConfigKey) of
        {ok, ExistingConfig} -> ExistingConfig;
        undefined -> []
    end,
    NewConfig = lists:keystore(Key, 1, Config, {Key, Value}),
    ok = application:set_env(App, ConfigKey, NewConfig).

decode_pem_data(PemList) when is_list(PemList) ->
    decode_pem_data(list_to_binary(PemList));
decode_pem_data(PemBin) when is_binary(PemBin) ->
    try
        CaCertsDerEncoded = [Der || {'Certificate', Der, not_encrypted} <- public_key:pem_decode(PemBin)],
        case CaCertsDerEncoded of
            Certs when is_list(Certs) andalso length(Certs) > 0 ->
                {ok, CaCertsDerEncoded};
            _ ->
                ?LOG_ERROR("aws arn: invalid PEM data: no valid certificates found"),
                {ok, []}
        end
    catch Class:Error:Stacktrace ->
        ?LOG_ERROR("aws arn: error decoding certs ~tp:~tp", [Class, Error]),
        ?LOG_ERROR("~tp", [Stacktrace])
    end.
