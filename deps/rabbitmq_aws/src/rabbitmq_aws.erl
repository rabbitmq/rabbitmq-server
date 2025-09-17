%% ====================================================================
%% @author Gavin M. Roy <gavinmroy@gmail.com>
%% @copyright 2016, Gavin M. Roy
%% @doc rabbitmq_aws client library
%% @end
%% ====================================================================
-module(rabbitmq_aws).

%% API exports
-export([
    get/2, get/3, get/4,
    put/4, put/5,
    post/4,
    refresh_credentials/0,
    request/5, request/6, request/7,
    set_credentials/2,
    has_credentials/0,
    parse_uri/1,
    set_region/1,
    get_region/0,
    ensure_credentials_valid/0,
    ensure_imdsv2_token_valid/0,
    api_get_request/2,
    status_text/1,
    open_connection/1, open_connection/2,
    close_connection/1,
    direct_request/6,
    endpoint/4,
    sign_headers/9
]).

%% Export all for unit tests
-ifdef(TEST).
-compile(export_all).
-endif.

-include("rabbitmq_aws.hrl").
-include_lib("kernel/include/logger.hrl").

-type connection_handle() :: {gun:conn_ref(), string()}.
%%====================================================================
%% ETS-based state management
%%====================================================================

-spec get_credentials() ->
    {ok, access_key(), secret_access_key(), security_token(), region()} | {error, term()}.
get_credentials() ->
    get_credentials(10).

-spec get_credentials(Retries :: non_neg_integer()) ->
    {ok, access_key(), secret_access_key(), security_token(), region()} | {error, term()}.
get_credentials(Retries) ->
    case ets:lookup(?AWS_CREDENTIALS_TABLE, current) of
        [{current, Creds}] ->
            case expired_credentials(Creds#aws_credentials.expiration) of
                false ->
                    Region = get_region(),
                    {ok, Creds#aws_credentials.access_key, Creds#aws_credentials.secret_key,
                        Creds#aws_credentials.security_token, Region};
                true ->
                    refresh_credentials_with_lock(Retries)
            end;
        [] ->
            refresh_credentials_with_lock(Retries)
    end.

-spec refresh_credentials_with_lock(Retries :: non_neg_integer()) ->
    {ok, access_key(), secret_access_key(), security_token(), region()} | {error, term()}.
refresh_credentials_with_lock(0) ->
    {error, lock_timeout};
refresh_credentials_with_lock(Retries) ->
    LockId = {aws_credentials_refresh, node()},
    case global:set_lock(LockId, [node()], 0) of
        true ->
            try
                % Double-check if someone else already refreshed
                case ets:lookup(?AWS_CREDENTIALS_TABLE, current) of
                    [{current, Creds}] ->
                        case expired_credentials(Creds#aws_credentials.expiration) of
                            false ->
                                Region = get_region(),
                                {ok, Creds#aws_credentials.access_key,
                                    Creds#aws_credentials.secret_key,
                                    Creds#aws_credentials.security_token, Region};
                            true ->
                                do_refresh_credentials()
                        end;
                    [] ->
                        do_refresh_credentials()
                end
            after
                global:del_lock(LockId, [node()])
            end;
        false ->
            % Someone else is refreshing, wait and retry
            timer:sleep(100),
            get_credentials(Retries - 1)
    end.

-spec do_refresh_credentials() ->
    {ok, access_key(), secret_access_key(), security_token(), region()} | {error, term()}.
do_refresh_credentials() ->
    Region = get_region(),
    case rabbitmq_aws_config:credentials() of
        {ok, AccessKey, SecretAccessKey, Expiration, SecurityToken} ->
            Creds = #aws_credentials{
                access_key = AccessKey,
                secret_key = SecretAccessKey,
                security_token = SecurityToken,
                expiration = Expiration
            },
            ets:insert(?AWS_CREDENTIALS_TABLE, {current, Creds}),
            {ok, AccessKey, SecretAccessKey, SecurityToken, Region};
        {error, Reason} ->
            {error, Reason}
    end.

-spec get_region() -> region().
get_region() ->
    case ets:lookup(?AWS_CONFIG_TABLE, region) of
        [{region, Region}] ->
            Region;
        [] ->
            % Use proper region detection
            case rabbitmq_aws_config:region() of
                {ok, DetectedRegion} ->
                    % Cache the detected region
                    ets:insert(?AWS_CONFIG_TABLE, {region, DetectedRegion}),
                    DetectedRegion;
                _ ->
                    % Final fallback
                    ets:insert(?AWS_CONFIG_TABLE, {region, "us-east-1"}),
                    "us-east-1"
            end
    end.

-spec set_region(Region :: region()) -> ok.
set_region(Region) ->
    ets:insert(?AWS_CONFIG_TABLE, {region, Region}),
    ok.

-spec has_credentials() -> boolean().
has_credentials() ->
    case ets:lookup(?AWS_CREDENTIALS_TABLE, current) of
        [{current, Creds}] when Creds#aws_credentials.access_key =/= undefined ->
            not expired_credentials(Creds#aws_credentials.expiration);
        _ ->
            false
    end.

%%====================================================================
%% exported wrapper functions
%%====================================================================

-spec get(
    ServiceOrHandle :: string() | connection_handle(),
    Path :: path()
) -> result().
%% @doc Perform a HTTP GET request to the AWS API for the specified service. The
%%      response will automatically be decoded if it is either in JSON, or XML
%%      format.
%% @end
get(ServiceOrHandle, Path) ->
    get(ServiceOrHandle, Path, []).

-spec get(
    ServiceOrHandle :: string() | connection_handle(),
    Path :: path(),
    Headers :: headers()
) -> result().
%% @doc Perform a HTTP GET request to the AWS API for the specified service. The
%%      response will automatically be decoded if it is either in JSON or XML
%%      format.
%% @end
get(ServiceOrHandle, Path, Headers) ->
    get(ServiceOrHandle, Path, Headers, []).

get(Service, Path, Headers, Options) ->
    request(Service, get, Path, <<>>, Headers, Options).

-spec post(
    ServiceOrHandle :: string() | connection_handle(),
    Path :: path(),
    Body :: body(),
    Headers :: headers()
) -> result().
%% @doc Perform a HTTP Post request to the AWS API for the specified service. The
%%      response will automatically be decoded if it is either in JSON or XML
%%      format.
%% @end
post(ServiceOrHandle, Path, Body, Headers) ->
    post(ServiceOrHandle, Path, Body, Headers, []).

post(Service, Path, Body, Headers, Options) ->
    request(Service, post, Path, Body, Headers, Options).

-spec put(
    ServiceOrHandle :: string() | connection_handle(),
    Path :: path(),
    Body :: body(),
    Headers :: headers()
) -> result().
%% @doc Perform a HTTP Post request to the AWS API for the specified service. The
%%      response will automatically be decoded if it is either in JSON or XML
%%      format.
%% @end
put(ServiceOrHandle, Path, Body, Headers) ->
    put(ServiceOrHandle, Path, Body, Headers, []).

put(Service, Path, Body, Headers, Options) ->
    request(Service, put, Path, Body, Headers, Options).

-spec refresh_credentials() -> ok | error.
%% @doc Manually refresh the credentials from the environment, filesystem or EC2 Instance Metadata Service.
%% @end
refresh_credentials() ->
    case refresh_credentials_with_lock(10) of
        {ok, _, _, _, _} -> ok;
        {error, _} -> error
    end.

%%====================================================================
%% New Concurrent API Functions
%%====================================================================

%% Open a connection and return handle for direct use
-spec open_connection(Service :: string()) -> {ok, connection_handle()} | {error, term()}.
open_connection(Service) ->
    open_connection(Service, []).

-spec open_connection(Service :: string(), Options :: list()) ->
    {ok, connection_handle()} | {error, term()}.
open_connection(Service, Options) ->
    % Just get region and open connection - no credential validation needed
    Region = get_region(),
    Host = endpoint_host(Region, Service),
    Port = 443,
    GunPid = create_gun_connection(Host, Port, Options),
    {ok, {GunPid, Service}}.

%% Close a direct connection
-spec close_connection(Handle :: connection_handle()) -> ok.
close_connection({GunPid, _Service}) ->
    gun:close(GunPid).

-spec direct_request(
    Handle :: connection_handle(),
    Method :: method(),
    Path :: path(),
    Body :: body(),
    Headers :: headers(),
    Options :: list()
) -> result().
direct_request({GunPid, Service}, Method, Path, Body, Headers, Options) ->
    case get_credentials() of
        {ok, AccessKey, SecretKey, SecurityToken, Region} ->
            Host = endpoint_host(Region, Service),
            URI = create_uri(Host, Path),
            BodyHash = proplists:get_value(payload_hash, Options),
            SignedHeaders = sign_headers(
                AccessKey, SecretKey, SecurityToken, Region, Service, Method, URI, Headers, Body, BodyHash
            ),
            direct_gun_request(GunPid, Method, Path, SignedHeaders, Body, Options);
        {error, Reason} ->
            {error, Reason}
    end.

-spec sign_headers(
    AccessKey :: access_key(),
    SecretKey :: secret_access_key(),
    SecurityToken :: security_token(),
    Region :: region(),
    Service :: string(),
    Method :: method(),
    URI :: string(),
    Headers :: headers(),
    Body :: body(),
    BodyHash :: iodata()
) -> headers().
sign_headers(AccessKey, SecretKey, SecurityToken, Region, Service, Method, URI, Headers, Body, BodyHash) ->
    rabbitmq_aws_sign:headers(
        #request{
            access_key = AccessKey,
            secret_access_key = SecretKey,
            security_token = SecurityToken,
            region = Region,
            service = Service,
            method = Method,
            uri = URI,
            headers = Headers,
            body = Body
        },
        BodyHash
    ).

-spec request(
    Service :: string(),
    Method :: method(),
    Path :: path(),
    Body :: body(),
    Headers :: headers()
) -> result().
%% @doc Perform a HTTP request to the AWS API for the specified service. The
%%      response will automatically be decoded if it is either in JSON or XML
%%      format.
%% @end
request(Service, Method, Path, Body, Headers) ->
    request(Service, Method, Path, Body, Headers, []).

-spec request(
    Service :: string(),
    Method :: method(),
    Path :: path(),
    Body :: body(),
    Headers :: headers(),
    HTTPOptions :: http_options()
) -> result().
%% @doc Perform a HTTP request to the AWS API for the specified service. The
%%      response will automatically be decoded if it is either in JSON or XML
%%      format.
%% @end
request(Service, Method, Path, Body, Headers, HTTPOptions) ->
    request(Service, Method, Path, Body, Headers, HTTPOptions, undefined).

-spec request(
    ServiceOrHandle :: string() | connection_handle(),
    Method :: method(),
    Path :: path(),
    Body :: body(),
    Headers :: headers(),
    HTTPOptions :: http_options(),
    Endpoint :: host()
) -> result().
%% @doc Perform a HTTP request to the AWS API for the specified service, overriding
%%      the endpoint URL to use when invoking the API. This is useful for local testing
%%      of services such as DynamoDB. The response will automatically be decoded
%%      if it is either in JSON or XML format.
%% @end
request({GunPid, Service}, Method, Path, Body, Headers, HTTPOptions, _) when
    is_pid(GunPid)
->
    direct_request({GunPid, Service}, Method, Path, Body, Headers, HTTPOptions);
request(Service, Method, Path, Body, Headers, HTTPOptions, Endpoint) ->
    perform_request_direct(Service, Method, Headers, Path, Body, HTTPOptions, Endpoint).

-spec set_credentials(access_key(), secret_access_key()) -> ok.
%% @doc Manually set the access credentials for requests. This should
%%      be used in cases where the client application wants to control
%%      the credentials instead of automatically discovering them from
%%      configuration or the AWS Instance Metadata service.
%% @end
set_credentials(AccessKey, SecretAccessKey) ->
    Creds = #aws_credentials{
        access_key = AccessKey,
        secret_key = SecretAccessKey,
        security_token = undefined,
        expiration = undefined
    },
    ets:insert(?AWS_CREDENTIALS_TABLE, {current, Creds}),
    ok.

-spec ensure_credentials_valid() -> ok.
%% @doc Invoked before each AWS service API request to check if the current credentials are available and that they have not expired.
%%      If the credentials are available and are still current, then move on and perform the request.
%%      If the credentials are not available or have expired, then refresh them before performing the request.
%% @end
ensure_credentials_valid() ->
    ?LOG_DEBUG("Making sure AWS credentials are available and still valid"),
    case has_credentials() of
        true ->
            ok;
        false ->
            refresh_credentials(),
            ok
    end.

-spec perform_request_direct(
    Service :: string(),
    Method :: method(),
    Headers :: headers(),
    Path :: path(),
    Body :: body(),
    Options :: http_options(),
    Host :: string() | undefined
) -> result().
perform_request_direct(Service, Method, Headers, Path, Body, Options, Host) ->
    case get_credentials() of
        {ok, AccessKey, SecretKey, SecurityToken, Region} ->
            URI = endpoint(Region, Host, Service, Path),
            SignedHeaders = sign_headers(
                AccessKey, SecretKey, SecurityToken, Region, Service, Method, URI, Headers, Body
            ),
            gun_request(Method, URI, SignedHeaders, Body, Options);
        {error, Reason} ->
            {error, {credentials, Reason}}
    end.

-spec endpoint(
    Region :: region(),
    Host :: string() | undefined,
    Service :: string(),
    Path :: string()
) -> string().
endpoint(Region, undefined, Service, Path) ->
    lists:flatten(["https://", endpoint_host(Region, Service), Path]);
endpoint(_, Host, _, Path) ->
    lists:flatten(["https://", Host, Path]).
%% @doc Construct the endpoint hostname for the request based upon the service
%%      and region.
%% @end
endpoint_host(Region, Service) ->
    lists:flatten(string:join([Service, Region, endpoint_tld(Region)], ".")).

-spec endpoint_tld(Region :: region()) -> host().
%% @doc Construct the endpoint hostname TLD for the request based upon the region.
%%      See https://docs.aws.amazon.com/general/latest/gr/rande.html#ec2_region for details.
%% @end
endpoint_tld("cn-north-1") ->
    "amazonaws.com.cn";
endpoint_tld("cn-northwest-1") ->
    "amazonaws.com.cn";
endpoint_tld(_Other) ->
    "amazonaws.com".

-spec format_response(Response :: httpc_result()) -> result().
%% @doc Format the httpc response result, returning the request result data
%% structure. The response body will attempt to be decoded by invoking the
%% maybe_decode_body/2 method.
%% @end
format_response({ok, {{_Version, 200, _Message}, Headers, Body}}) ->
    {ok, {Headers, maybe_decode_body(get_content_type(Headers), Body)}};
format_response({ok, {{_Version, 206, _Message}, Headers, Body}}) ->
    {ok, {Headers, maybe_decode_body(get_content_type(Headers), Body)}};
format_response({ok, {{_Version, StatusCode, Message}, Headers, Body}}) when StatusCode >= 400 ->
    {error, Message, {Headers, maybe_decode_body(get_content_type(Headers), Body)}};
format_response({error, Reason}) ->
    {error, Reason, undefined}.

-spec get_content_type(Headers :: headers()) -> {Type :: string(), Subtype :: string()}.
%% @doc Fetch the content type from the headers and return it as a tuple of
%%      {Type, Subtype}.
%% @end
get_content_type(Headers) ->
    Value =
        case proplists:get_value(<<"content-type">>, Headers, undefined) of
            undefined ->
                proplists:get_value(<<"Content-Type">>, Headers, "text/xml");
            Other ->
                Other
        end,
    parse_content_type(Value).

-spec expired_credentials(Expiration :: calendar:datetime()) -> boolean().
%% @doc Indicates if the date that is passed in has expired.
%% end
expired_credentials(undefined) ->
    false;
expired_credentials(Expiration) ->
    Now = calendar:datetime_to_gregorian_seconds(local_time()),
    Expires = calendar:datetime_to_gregorian_seconds(Expiration),
    Now >= Expires.

-spec local_time() -> calendar:datetime().
%% @doc Return the current local time.
%% @end
local_time() ->
    [Value] = calendar:local_time_to_universal_time_dst(calendar:local_time()),
    Value.

-spec maybe_decode_body(ContentType :: {nonempty_string(), nonempty_string()}, Body :: body()) ->
    list() | body().
%% @doc Attempt to decode the response body by its MIME
%% @end
maybe_decode_body(_, <<>>) ->
    <<>>;
maybe_decode_body({"application", "x-amz-json-1.0"}, Body) ->
    rabbitmq_aws_json:decode(Body);
maybe_decode_body({"application", "json"}, Body) ->
    rabbitmq_aws_json:decode(Body);
maybe_decode_body({_, "xml"}, Body) ->
    rabbitmq_aws_xml:parse(Body);
maybe_decode_body(_ContentType, Body) ->
    Body.

-spec parse_content_type(ContentType :: string()) -> {Type :: string(), Subtype :: string()}.
%% @doc parse a content type string returning a tuple of type/subtype
%% @end
parse_content_type(ContentType) when is_binary(ContentType) ->
    parse_content_type(binary_to_list(ContentType));
parse_content_type(ContentType) ->
    Parts = string:tokens(ContentType, ";"),
    [Type, Subtype] = string:tokens(lists:nth(1, Parts), "/"),
    {Type, Subtype}.

-spec expired_imdsv2_token('undefined' | imdsv2token()) -> boolean().
%% @doc Determine whether or not an Imdsv2Token has expired.
%% @end
expired_imdsv2_token(undefined) ->
    ?LOG_DEBUG("EC2 IMDSv2 token has not yet been obtained"),
    true;
expired_imdsv2_token(#imdsv2token{expiration = undefined}) ->
    ?LOG_DEBUG("EC2 IMDSv2 token is not available"),
    true;
expired_imdsv2_token(#imdsv2token{expiration = Expiration}) ->
    Now = calendar:datetime_to_gregorian_seconds(local_time()),
    HasExpired = Now >= Expiration,
    ?LOG_DEBUG("EC2 IMDSv2 token has expired: ~tp", [HasExpired]),
    HasExpired.

-spec get_imdsv2_token() -> imdsv2token() | 'undefined'.
%% @doc return the current Imdsv2Token used to perform instance metadata service requests.
%% @end
get_imdsv2_token() ->
    case ets:lookup(?AWS_CONFIG_TABLE, imdsv2_token) of
        [{imdsv2_token, Token}] -> Token;
        [] -> undefined
    end.

-spec set_imdsv2_token(imdsv2token()) -> ok.
%% @doc Manually set the Imdsv2Token used to perform instance metadata service requests.
%% @end
set_imdsv2_token(Imdsv2Token) ->
    ets:insert(?AWS_CONFIG_TABLE, {imdsv2_token, Imdsv2Token}),
    ok.

-spec ensure_imdsv2_token_valid() -> security_token().
ensure_imdsv2_token_valid() ->
    Imdsv2Token = get_imdsv2_token(),
    case expired_imdsv2_token(Imdsv2Token) of
        true ->
            Value = rabbitmq_aws_config:load_imdsv2_token(),
            Expiration =
                calendar:datetime_to_gregorian_seconds(local_time()) + ?METADATA_TOKEN_TTL_SECONDS,
            set_imdsv2_token(#imdsv2token{
                token = Value,
                expiration = Expiration
            }),
            Value;
        _ ->
            Imdsv2Token#imdsv2token.token
    end.

-spec api_get_request(string(), path()) -> {'ok', list()} | {'error', term()}.
%% @doc Invoke an API call to an AWS service.
%% @end
api_get_request(Service, Path) ->
    ?LOG_DEBUG("Invoking AWS request {Service: ~tp; Path: ~tp}...", [Service, Path]),
    api_get_request_with_retries(Service, Path, ?MAX_RETRIES, ?LINEAR_BACK_OFF_MILLIS).

-spec api_get_request_with_retries(string(), path(), integer(), integer()) ->
    {'ok', list()} | {'error', term()}.
%% @doc Invoke an API call to an AWS service with retries.
%% @end
api_get_request_with_retries(_, _, 0, _) ->
    ?LOG_WARNING("Request to AWS service has failed after ~b retries", [?MAX_RETRIES]),
    {error, "AWS service is unavailable"};
api_get_request_with_retries(Service, Path, Retries, WaitTimeBetweenRetries) ->
    ensure_credentials_valid(),
    case get(Service, Path) of
        {ok, {_Headers, Payload}} ->
            ?LOG_DEBUG("AWS request: ~ts~nResponse: ~tp", [Path, Payload]),
            {ok, Payload};
        {error, {credentials, _}} ->
            {error, credentials};
        {error, Message, Response} ->
            ?LOG_WARNING("Error occurred: ~ts", [Message]),
            case Response of
                {_, Payload} ->
                    ?LOG_WARNING("Failed AWS request: ~ts~nResponse: ~tp", [Path, Payload]);
                _ ->
                    ok
            end,
            ?LOG_WARNING("Will retry AWS request, remaining retries: ~b", [Retries]),
            timer:sleep(WaitTimeBetweenRetries),
            api_get_request_with_retries(Service, Path, Retries - 1, WaitTimeBetweenRetries)
    end.

%% Gun HTTP client functions
gun_request(Method, URI, Headers, Body, Options) ->
    {Host, Port, Path} = parse_uri(URI),
    GunPid = create_gun_connection(Host, Port, Options),
    Reply = direct_gun_request(GunPid, Method, Path, Headers, Body, Options),
    gun:close(GunPid),
    Reply.

do_gun_request(ConnPid, get, Path, Headers, _Body) ->
    gun:get(ConnPid, Path, Headers);
do_gun_request(ConnPid, post, Path, Headers, Body) ->
    gun:post(ConnPid, Path, Headers, Body, #{});
do_gun_request(ConnPid, put, Path, Headers, Body) ->
    gun:put(ConnPid, Path, Headers, Body, #{});
do_gun_request(ConnPid, head, Path, Headers, _Body) ->
    gun:head(ConnPid, Path, Headers, #{});
do_gun_request(ConnPid, delete, Path, Headers, _Body) ->
    gun:delete(ConnPid, Path, Headers, #{});
do_gun_request(ConnPid, patch, Path, Headers, Body) ->
    gun:patch(ConnPid, Path, Headers, Body, #{});
do_gun_request(ConnPid, options, Path, Headers, _Body) ->
    gun:options(ConnPid, Path, Headers, #{}).

create_gun_connection(Host, Port, Options) ->
    % Map HTTP version to Gun protocols, always include http as fallback
    HttpVersion = proplists:get_value(version, Options, "HTTP/1.1"),
    Protocols =
        case HttpVersion of
            "HTTP/2" -> [http2, http];
            "HTTP/2.0" -> [http2, http];
            "HTTP/1.1" -> [http];
            "HTTP/1.0" -> [http];
            % Default: try HTTP/2, fallback to HTTP/1.1
            _ -> [http2, http]
        end,
    ConnectTimeout = proplists:get_value(connect_timeout, Options, infinity),
    Opts = #{
        transport =>
            if
                Port == 443 -> tls;
                true -> tcp
            end,
        protocols => Protocols,
        connect_timeout => ConnectTimeout
    },
    case gun:open(Host, Port, Opts) of
        {ok, ConnPid} ->
            case gun:await_up(ConnPid, ConnectTimeout) of
                {ok, _Protocol} ->
                    ConnPid;
                {error, Reason} ->
                    gun:close(ConnPid),
                    error({gun_connection_failed, Reason})
            end;
        {error, Reason} ->
            error({gun_open_failed, Reason})
    end.

create_uri(Host, Path) when is_list(Path) ->
    "https://" ++ Host ++ Path;
create_uri(Host, {Bucket, Key}) ->
    "https://" ++ Bucket ++ "." ++ Host ++ "/" ++ Key.

parse_uri(URI) ->
    case string:split(URI, "://", leading) of
        [Scheme, Rest] ->
            case string:split(Rest, "/", leading) of
                [HostPort] ->
                    {Host, Port} = parse_host_port(HostPort, Scheme),
                    {Host, Port, "/"};
                [HostPort, Path] ->
                    {Host, Port} = parse_host_port(HostPort, Scheme),
                    {Host, Port, "/" ++ Path}
            end
    end.

parse_host_port(HostPort, Scheme) ->
    DefaultPort =
        case Scheme of
            "https" -> 443;
            "http" -> 80;
            % Fallback to HTTPS
            _ -> 443
        end,
    case string:split(HostPort, ":", trailing) of
        [Host] ->
            {Host, DefaultPort};
        [Host, PortStr] ->
            {Host, list_to_integer(PortStr)}
    end.

status_text(200) -> "OK";
status_text(206) -> "Partial Content";
status_text(400) -> "Bad Request";
status_text(401) -> "Unauthorized";
status_text(403) -> "Forbidden";
status_text(404) -> "Not Found";
status_text(416) -> "Range Not Satisfiable";
status_text(500) -> "Internal Server Error";
status_text(Code) -> integer_to_list(Code).


-spec direct_gun_request(
    GunPid :: pid(),
    Method :: method(),
    Path :: path(),
    Headers :: headers(),
    Body :: body(),
    Options :: list()
) -> result().
direct_gun_request(GunPid, Method, {_, Path}, Headers, Body, Options) ->
    direct_gun_request(GunPid, Method, [$/ | Path], Headers, Body, Options);
direct_gun_request(GunPid, Method, Path, Headers, Body, Options) ->
    HeadersBin = lists:map(
        fun({Key, Value}) ->
            {list_to_binary(Key), list_to_binary(Value)}
        end,
        Headers
    ),
    Timeout = proplists:get_value(timeout, Options, ?DEFAULT_HTTP_TIMEOUT),
    Response =
        try
            StreamRef = do_gun_request(GunPid, Method, Path, HeadersBin, Body),
            case gun:await(GunPid, StreamRef, Timeout) of
                {response, fin, Status, RespHeaders} ->
                    {ok, {{http_version, Status, status_text(Status)}, RespHeaders, <<>>}};
                {response, nofin, Status, RespHeaders} ->
                    {ok, RespBody} = gun:await_body(GunPid, StreamRef, Timeout),
                    {ok, {{http_version, Status, status_text(Status)}, RespHeaders, RespBody}};
                {error, Reason} ->
                    {error, Reason}
            end
        catch
            _:Error ->
                {error, Error}
        end,
    format_response(Response).
