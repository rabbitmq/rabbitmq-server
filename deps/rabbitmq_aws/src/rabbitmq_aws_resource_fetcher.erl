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

-define(AWS_LOG_DEBUG(Fmt, Args),
    ?LOG_DEBUG("~tp: " ++ Fmt, [?MODULE | Args])).

-define(AWS_LOG_ERROR(Arg),
    ?LOG_ERROR("~tp: ~ts", [?MODULE, Arg])).

-define(AWS_LOG_ERROR(Fmt, Args),
    ?LOG_ERROR("~tp: " ++ Fmt, [?MODULE | Args])).

-define(AWS_LOG_INFO(Arg),
    ?LOG_INFO("~tp: ~ts", [?MODULE, Arg])).

-spec process_arns() -> ok.
%% @doc Fetch certificate files, secrets from Amazon S3 and Secret Manager and update application configuration to use them
%% @end
process_arns() ->
    try
        ok = process_arns(application:get_env(rabbitmq_aws, aws_arns)),
        ?AWS_LOG_INFO("success")
    catch Class:Reason:Stacktrace ->
        ?AWS_LOG_ERROR("~tp:~tp", [Class, Reason]),
        ?AWS_LOG_ERROR("~tp", [Stacktrace])
    end.

process_arns([]) ->
    ok;
process_arns([{_Key, Arn, Handler} | Rest]) ->
    case resolve_arn(Arn) of
        {ok, Content} ->
            ok = handle_content(Handler, Content);
        {error, Reason} ->
            ?AWS_LOG_ERROR("~tp", [Reason])
    end,
    process_arns(Rest);
process_arns({ok, ArnList}) ->
    process_arns(ArnList);
process_arns(undefined) ->
    ok.

-spec resolve_arn(string()) -> {ok, binary()} | {error, term()}.
resolve_arn(Arn) ->
    ?AWS_LOG_DEBUG("attempting to resolve: ~tp", [Arn]),
    case parse_arn(Arn) of
        {ok, #{service := "s3", resource := Resource}} ->
            fetch_s3_object(Resource);
        {ok, #{service := "secretsmanager", region := Region}} ->
            fetch_secretsmanager_secret(Arn, Region);
        {ok, #{service := Service}} ->
            Reason = unsupported_service,
            ?AWS_LOG_ERROR("~tp ~tp ~tp", [Arn, Reason, Service]),
            {error, {Reason, Service}};
        {error, Reason} = Error ->
            ?AWS_LOG_ERROR("~tp ~tp", [Arn, Reason]),
            Error
    end.

-spec parse_arn(string()) -> {ok, map()} | {error, term()}.
parse_arn(Arn) ->
    try
        % resource name in arn could contain ":" itself, therefore using parts.
        % eg: arn:aws:secretsmanager:us-east-1:12345678910:secret:mysecret
        case re:split(Arn, ":", [{parts,6}, {return, list}]) of
            ["arn", Partition, Service, Region, Account, Resource] ->
                ?AWS_LOG_DEBUG("parsed arn: ~tp into ~tp:~tp:~tp:~tp:~tp",
                          [Arn, Partition, Service, Region, Account, Resource]),
                {ok, #{
                    partition => Partition,
                    service => Service,
                    region => Region,
                    account => Account,
                    resource => Resource
                }};
            UnexpectedMatch ->
                {error, {invalid_arn_format, UnexpectedMatch}}
        end
    catch Class:Reason ->
        {error, {Class, Reason}}
    end.

-spec fetch_s3_object(string()) -> {ok, binary()} | {error, term()}.
fetch_s3_object(Resource) ->
    %% Note: splits on the first / only
    %% https://www.erlang.org/doc/apps/stdlib/string.html#split/2
    [Bucket | Key] = string:split(Resource, "/"),
    F = fun() ->
            fetch_s3_object_final(Bucket, Key)
        end,
    with_credentials_and_role(F).

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
    F = fun() ->
            fetch_secretsmanager_secret_after_env_credential_set(Arn, Region)
        end,
    with_credentials_and_role(Region, F).

fetch_secretsmanager_secret_after_env_credential_set(Arn, _Region) ->
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

%% TODO spec
with_credentials_and_role(F) when is_function(F) ->
    {ok, Region} = rabbitmq_aws_config:region(),
    with_credentials_and_role(Region, F).

with_credentials_and_role(Region, F) when is_function(F) ->
    ok = rabbitmq_aws:set_region(Region),
    ok = rabbitmq_aws:refresh_credentials(),
    case rabbitmq_aws:has_credentials() of
        false ->
            {error, no_base_credentials};
        true ->
            case application:get_env(rabbitmq_aws, assume_role_arn) of
                {ok, RoleArn} ->
                    case assume_role(RoleArn) of
                        ok ->
                            F();
                        Error ->
                            {error, {assume_role_failed, Error}}
                    end;
                _ ->
                    % No assume role configured, use existing credentials
                    F()
            end
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
        Error ->
            Error
    end.

-spec parse_assume_role_response(binary()) -> ok | {error, term()}.
parse_assume_role_response(Body) ->
    [{"AssumeRoleResponse", ResponseData}] = Body,
    {"AssumeRoleResult", ResultData} = lists:keyfind("AssumeRoleResult", 1, ResponseData),
    {"Credentials", CredentialsData} = lists:keyfind("Credentials", 1, ResultData),
    {"AccessKeyId", AccessKey} = lists:keyfind("AccessKeyId", 1, CredentialsData),
    {"SecretAccessKey", SecretKey} = lists:keyfind("SecretAccessKey", 1, CredentialsData),
    {"SessionToken", SessionToken} = lists:keyfind("SessionToken", 1, CredentialsData),
    ok = rabbitmq_aws:set_credentials(AccessKey, SecretKey, SessionToken).

handle_content(oauth2_https_cacertfile, PemData) ->
    {ok, CaCertsDerEncoded} = decode_pem_data(PemData),
    %% Note: yes it's really key_config and not https like in the
    %% cuttlefish schema
    replace_in_env(rabbitmq_auth_backend_oauth2, key_config, cacertfile,
                   cacerts, CaCertsDerEncoded);
handle_content(ssl_options_cacertfile, PemData) ->
    {ok, CaCertsDerEncoded} = decode_pem_data(PemData),
    replace_in_env(rabbit, ssl_options, cacertfile,
                   cacerts, CaCertsDerEncoded);
handle_content(ldap_ssl_options_cacertfile, PemData) ->
    {ok, CaCertsDerEncoded} = decode_pem_data(PemData),
    replace_in_env(rabbitmq_auth_backend_ldap, ssl_options, cacertfile,
                   cacerts, CaCertsDerEncoded);
handle_content(ldap_dn_lookup_bind_password, SecretJsonBin) when is_binary(SecretJsonBin) ->
    update_ldap_env(ldap_dn_lookup_bind_password, SecretJsonBin);
handle_content(ldap_other_bind_password, SecretJsonBin) when is_binary(SecretJsonBin) ->
    update_ldap_env(ldap_other_bind_password, SecretJsonBin).

-spec replace_in_env(atom(), atom(), atom(), atom(), any()) -> ok.
replace_in_env(App, ConfigKey, cacertfile, cacerts, CaCertsDerEncoded0) ->
    {ok, OrigCacertFile} =  delete_from_env(App, ConfigKey, cacertfile),
    {ok, CaCertsDerEncoded1} = maybe_add_cacertfile_to_cacerts(OrigCacertFile, CaCertsDerEncoded0),
    ok = update_env(App, ConfigKey, cacerts, CaCertsDerEncoded1);
replace_in_env(App, ConfigKey, KeyToDelete, Key, Value) ->
    {ok, _} = delete_from_env(App, ConfigKey, KeyToDelete),
    ok = update_env(App, ConfigKey, Key, Value).

-spec delete_from_env(atom(), atom(), atom()) -> ok.
delete_from_env(App, ConfigKey, KeyToDelete) ->
    ConfigValue = case application:get_env(App, ConfigKey) of
        {ok, Val} -> Val;
        undefined -> []
    end,
    OrigConfigValue = lists:keyfind(KeyToDelete, 1, ConfigValue),
    NewConfig = lists:keydelete(KeyToDelete, 1, ConfigValue),
    ok = application:set_env(App, ConfigKey, NewConfig),
    {ok, OrigConfigValue}.

-spec update_env(atom(), atom(), atom(), any()) -> ok.
update_env(App, ConfigKey, Key, Value) ->
    Config = case application:get_env(App, ConfigKey) of
        {ok, ExistingConfig} -> ExistingConfig;
        undefined -> []
    end,
    NewConfig = lists:keystore(Key, 1, Config, {Key, Value}),
    ok = application:set_env(App, ConfigKey, NewConfig).

maybe_add_cacertfile_to_cacerts({cacertfile, CacertFilePath}, CaCertsDerEncoded) ->
    maybe_add_cacertfile_to_cacerts_from_file(file:read_file(CacertFilePath), CaCertsDerEncoded).

maybe_add_cacertfile_to_cacerts_from_file({ok, CacertBin}, CaCertsDerEncoded) ->
    maybe_add_decoded_pem_to_cacerts(decode_pem_data(CacertBin), CaCertsDerEncoded);
maybe_add_cacertfile_to_cacerts_from_file(_, CaCertsDerEncoded) ->
    {ok, CaCertsDerEncoded}.

maybe_add_decoded_pem_to_cacerts({ok, CaCertsDerEncoded0}, CaCertsDerEncoded1) ->
    {ok, CaCertsDerEncoded0 ++ CaCertsDerEncoded1};
maybe_add_decoded_pem_to_cacerts(_, CaCertsDerEncoded1) ->
    {ok, CaCertsDerEncoded1}.

decode_pem_data(PemList) when is_list(PemList) ->
    decode_pem_data(list_to_binary(PemList));
decode_pem_data(PemBin) when is_binary(PemBin) ->
    try
        CaCertsDerEncoded = [Der || {'Certificate', Der, not_encrypted} <- public_key:pem_decode(PemBin)],
        case CaCertsDerEncoded of
            Certs when is_list(Certs) andalso length(Certs) > 0 ->
                {ok, CaCertsDerEncoded};
            _ ->
                ?AWS_LOG_ERROR("invalid PEM data: no valid certificates found"),
                {error, invalid_pem_data}
        end
    catch Class:Error ->
        ?AWS_LOG_ERROR("error decoding certs ~tp:~tp", [Class, Error]),
        {error, error_decoding_certs}
    end.

-spec update_ldap_env(string(), atom()) -> ok.
update_ldap_env(ldap_dn_lookup_bind_password, SecretContent) ->
    do_update_ldap_env(dn_lookup_bind, SecretContent, <<"auth_ldap.dn_lookup_bind.password">>);
update_ldap_env(ldap_other_bind_password, SecretContent) ->
    do_update_ldap_env(other_bind, SecretContent, <<"auth_ldap.other_bind.password">>).

do_update_ldap_env(LdapAppConfigKey, SecretContent, SecretMapKey) ->
    case application:get_env(rabbitmq_auth_backend_ldap, LdapAppConfigKey) of
        {ok, {DN, _OldPassword}} ->
            SecretMap = rabbit_json:decode(SecretContent),
            NewPassword = maps:get(SecretMapKey, SecretMap),
            NewConfig = {DN, NewPassword},
            ok = application:set_env(rabbitmq_auth_backend_ldap, LdapAppConfigKey, NewConfig),
            ?AWS_LOG_DEBUG("updated LDAP ~p with password from fetched secrets", [LdapAppConfigKey]),
            ok;
        _ ->
            {error, {ldap_config_key_missing, LdapAppConfigKey}}
    end.
