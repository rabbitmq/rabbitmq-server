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
                        cacerts, CaCertsDerEncoded);
handle_content(ldap_dn_lookup_bind_password, SecretJsonBin) when is_binary(SecretJsonBin) ->
    ok = update_ldap_bind_password_env(ldap_dn_lookup_bind_password, SecretJsonBin);
handle_content(ldap_other_bind_password, SecretJsonBin) when is_binary(SecretJsonBin) ->
    ok = update_ldap_bind_password_env(ldap_other_bind_password, SecretJsonBin).
                

-spec resolve_arn(string()) -> {ok, binary()} | {error, term()}.
resolve_arn(Arn) ->
    ?LOG_INFO("aws arn: attempting to resolve: ~tp", [Arn]),
    case parse_arn(Arn) of
        {ok, #{service := "s3", resource := Resource}} ->
            fetch_s3_object(Resource);
        {ok, #{service := "secretsmanager", region := Region}} ->
            fetch_secretsmanager_secret(Arn, Region);
        {ok, #{service := Service}} ->
            ?LOG_ERROR("[AWS_ARNS_FETCH_ERROR]Unsupported service: ~p", [Service]),
            {error, {unsupported_service, Service}};
        {error, _} = Error ->
            ?LOG_ERROR("[AWS_ARNS_FETCH_ERROR]Failed to parse ARN ~p: ~p", [Arn, Error]),
            Error
    end.

-spec parse_arn(string()) -> {ok, map()} | {error, term()}.
parse_arn(Arn) ->
    ArnBin = list_to_binary(Arn),
    % resource name in arn could contain ":" itself, therefore using parts. eg: arn:aws:secretsmanager:us-east-1:12345678910:secret:mysecret
    case re:split(ArnBin, <<":">>, [{parts,6}, {return, binary}]) of
        [<<"arn">>, Partition, Service, Region, Account, Resource] ->
            ?LOG_INFO("rabbitmq_aws_resource_fetcher parsed ARN: arn:~tp:~tp:~tp:~tp:~tp", [Partition, Service, Region, Account, Resource]),
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
    [Bucket | KeyParts] = string:split(Resource, "/"),
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
            case application:get_env(rabbit_aws, assume_role_arn) of
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

-spec fetch_secretsmanager_secret(string(), string()) -> {ok, binary()} | {error, term()}.
fetch_secretsmanager_secret(Arn, Region) ->
    rabbitmq_aws:set_region(Region),
    rabbitmq_aws:refresh_credentials(),
    RequestBody = binary_to_list(rabbit_json:encode(#{
        <<"SecretId">> => list_to_binary(Arn),
        <<"VersionStage">> => <<"AWSCURRENT">>
    })),
    Headers = [
        {"X-Amz-Target", "secretsmanager.GetSecretValue"},
        {"Content-Type", "application/x-amz-json-1.1"}
    ],
    case rabbitmq_aws:post("secretsmanager", "/", RequestBody, Headers) of
        {ok, {_ResponseHeaders, ResponseBody}} ->
            case rabbit_json:decode(ResponseBody) of
                #{<<"SecretString">> := SecretValue} ->
                    {ok, SecretValue};
                #{<<"SecretBinary">> := SecretBinary} ->
                    {ok, base64:decode(SecretBinary)};
                _ ->
                    {error, no_secret_value}
            end;
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

-spec update_ldap_bind_password_env(string(), atom()) -> ok.
update_ldap_bind_password_env(PasswordType, SecretContent) ->
    % parse Json content to be a map.
    case rabbit_json:decode(SecretContent) of
        SecretMap when is_map(SecretMap) ->
            % Determine which password key to look for in secret manager. keep it the same with external configuration name.
            JsonKey = case PasswordType of
                ldap_dn_lookup_bind_password -> <<"auth_ldap.dn_lookup_bind.password">>;
                ldap_other_bind_password -> <<"auth_ldap.other_bind.password">>
            end,
            
            % Get password from json object
            case maps:get(JsonKey, SecretMap, undefined) of
                undefined ->
                    ?LOG_ERROR("[AWS_ARNS_FETCH_ERROR_LDAP_CONFIG]Key ~p not found in fetched customer secret content", [JsonKey]);
                Password ->
                    PasswordStr = binary_to_list(Password),
                    
                    case PasswordType of
                        % set password in application env based on password type
                        ldap_dn_lookup_bind_password ->
                            case application:get_env(rabbitmq_auth_backend_ldap, dn_lookup_bind) of
                                {ok, ExistingConfig} ->
                                NewConfig = get_updated_dn_lookup_bind_password(ExistingConfig, PasswordStr),
                                application:set_env(rabbitmq_auth_backend_ldap, dn_lookup_bind, NewConfig),
                                ?LOG_INFO("Updated LDAP ~p with password from fetched secrets", [PasswordType]);
                                _ ->
                                ?LOG_ERROR("[AWS_ARNS_FETCH_ERROR_LDAP_CONFIG]auth_ldap.dn_lookup_bind doesn't exist before password fetch.")
                            end;
                        ldap_other_bind_password ->
                            case application:get_env(rabbitmq_auth_backend_ldap, other_bind) of
                                {ok, ExistingConfig} ->
                                NewConfig = get_updated_other_bind_password(ExistingConfig, PasswordStr),
                                application:set_env(rabbitmq_auth_backend_ldap, other_bind, NewConfig),
                                ?LOG_INFO("Updated LDAP ~p with password from fetched secrets", [PasswordType]);
                                _ ->
                                ?LOG_ERROR("[AWS_ARNS_FETCH_ERROR_LDAP_CONFIG]auth_ldap.other_bind doesn't exist before password is fetched.")
                            end
                    end
            end; 
    _ ->
            ?LOG_ERROR("[AWS_ARNS_FETCH_ERROR_LDAP_CONFIG]Invalid JSON format from fetched secret content")
    end.


-spec get_updated_dn_lookup_bind_password(tuple() | atom(), string()) -> tuple().
get_updated_dn_lookup_bind_password(ExistingConfig, Password) ->
    case ExistingConfig of
        {Username, _OldPassword} ->
            % Replace with new password
            {Username, list_to_binary(Password)};
        _ ->
            % Case 2: Any other value (as_user, anon, etc.) - this should not happen
            ?LOG_ERROR("[AWS_ARNS_FETCH_ERROR_LDAP_CONFIG]dn_lookup_bind in env is configured either as scalar or not present, but password config is present. tuple of {user,password} is expected and therefore cannot overwrite password.", [ExistingConfig]),
            ExistingConfig
    end.

-spec get_updated_other_bind_password(list(), string()) -> list().
get_updated_other_bind_password(ExistingConfig, Password) ->
    case lists:keyfind(other_bind, 1, ExistingConfig) of
        {other_bind, {Username, _OldPassword}} ->
            % Case 1: Proper tuple - replace with new password
            lists:keyreplace(other_bind, 1, ExistingConfig, {other_bind, {Username, list_to_binary(Password)}});
        _ ->
            % Case 2: Any other value (as_user, anon, etc.) - this should not happen
            ?LOG_ERROR("[AWS_ARNS_FETCH_ERROR_LDAP_CONFIG]dn_lookup_bind in env is configured either as scalar or not present, but password config is present. tuple of {user,password} is expected and therefore cannot overwrite password.", [ExistingConfig]),
            ExistingConfig
    end.
