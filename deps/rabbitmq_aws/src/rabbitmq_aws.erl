%% ====================================================================
%% @author Gavin M. Roy <gavinmroy@gmail.com>
%% @copyright 2016, Gavin M. Roy
%% @doc rabbitmq_aws client library
%% @end
%% ====================================================================
-module(rabbitmq_aws).

-behavior(gen_server).

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
    ensure_imdsv2_token_valid/0,
    api_get_request/2,
    status_text/1,
    open_connection/1, open_connection/2,
    close_connection/1
]).

%% gen-server exports
-export([
    start_link/0,
    init/1,
    terminate/2,
    code_change/3,
    handle_call/3,
    handle_cast/2,
    handle_info/2
]).

%% Export all for unit tests
-ifdef(TEST).
-compile(export_all).
-endif.

-include("rabbitmq_aws.hrl").
-include_lib("kernel/include/logger.hrl").

%% Types for new concurrent API
-type connection_handle() :: {gun:conn_ref(), credential_context()}.
-type credential_context() :: #{
    access_key => access_key(),
    secret_access_key => secret_access_key(),
    security_token => security_token(),
    region => region(),
    service => string()
}.

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
    gen_server:call(rabbitmq_aws, refresh_credentials).

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
    gen_server:call(?MODULE, {open_direct_connection, Service, Options}).

%% Close a direct connection
-spec close_connection(Handle :: connection_handle()) -> ok.
close_connection({GunPid, _CredContext}) ->
    gun:close(GunPid).

-spec direct_request(
    Handle :: connection_handle(),
    Method :: method(),
    Path :: path(),
    Body :: body(),
    Headers :: headers(),
    Options :: list()
) -> result().
direct_request({GunPid, CredContext}, Method, Path, Body, Headers, Options) ->
    #{service := Service, region := Region} = CredContext,
    % Build URI for signing
    Host = endpoint_host(Region, Service),
    URI = "https://" ++ Host ++ Path,
    % Sign headers directly (no gen_server call)
    SignedHeaders = sign_headers_with_context(CredContext, Method, URI, Headers, Body),
    % Make Gun request directly
    direct_gun_request(GunPid, Method, Path, SignedHeaders, Body, Options).

-spec refresh_credentials(state()) -> ok | error.
%% @doc Manually refresh the credentials from the environment, filesystem or EC2 Instance Metadata Service.
%% @end
refresh_credentials(State) ->
    ?LOG_DEBUG("Refreshing AWS credentials..."),
    {_, NewState} = load_credentials(State),
    ?LOG_DEBUG("AWS credentials have been refreshed"),
    set_credentials(NewState).

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
request({GunPid, _CredContext} = Handle, Method, Path, Body, Headers, HTTPOptions, _) when
    is_pid(GunPid)
->
    direct_request(Handle, Method, Path, Body, Headers, HTTPOptions);
request(Service, Method, Path, Body, Headers, HTTPOptions, Endpoint) ->
    gen_server:call(
        rabbitmq_aws, {request, Service, Method, Headers, Path, Body, HTTPOptions, Endpoint}
    ).

-spec set_credentials(state()) -> ok.
set_credentials(NewState) ->
    gen_server:call(rabbitmq_aws, {set_credentials, NewState}).

-spec set_credentials(access_key(), secret_access_key()) -> ok.
%% @doc Manually set the access credentials for requests. This should
%%      be used in cases where the client application wants to control
%%      the credentials instead of automatically discovering them from
%%      configuration or the AWS Instance Metadata service.
%% @end
set_credentials(AccessKey, SecretAccessKey) ->
    gen_server:call(rabbitmq_aws, {set_credentials, AccessKey, SecretAccessKey}).

-spec set_region(Region :: string()) -> ok.
%% @doc Manually set the AWS region to perform API requests to.
%% @end
set_region(Region) ->
    gen_server:call(rabbitmq_aws, {set_region, Region}).

-spec set_imdsv2_token(imdsv2token()) -> ok.
%% @doc Manually set the Imdsv2Token used to perform instance metadata service requests.
%% @end
set_imdsv2_token(Imdsv2Token) ->
    gen_server:call(rabbitmq_aws, {set_imdsv2_token, Imdsv2Token}).

-spec get_imdsv2_token() -> imdsv2token() | 'undefined'.
%% @doc return the current Imdsv2Token used to perform instance metadata service requests.
%% @end
get_imdsv2_token() ->
    {ok, Imdsv2Token} = gen_server:call(rabbitmq_aws, get_imdsv2_token),
    Imdsv2Token.

%%====================================================================
%% gen_server functions
%%====================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec init(list()) -> {ok, state()}.
init([]) ->
    {ok, _} = application:ensure_all_started(gun),
    {ok, #state{}}.

terminate(_, _State) ->
    ok.

code_change(_, _, State) ->
    {ok, State}.

handle_call(Msg, _From, State) ->
    handle_msg(Msg, State).

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

%%====================================================================
%% Internal functions
%%====================================================================
handle_msg({request, Service, Method, Headers, Path, Body, Options, Host}, State) ->
    {Response, NewState} = perform_request(
        State, Service, Method, Headers, Path, Body, Options, Host
    ),
    {reply, Response, NewState};
handle_msg({open_direct_connection, Service, Options}, State) ->
    case ensure_credentials_valid_internal(State) of
        {ok, ValidState} ->
            case create_direct_connection(ValidState, Service, Options) of
                {ok, Handle} ->
                    {reply, {ok, Handle}, ValidState};
                {error, Reason} ->
                    {reply, {error, Reason}, ValidState}
            end;
        {error, Reason} ->
            {reply, {error, Reason}, State}
    end;
handle_msg(get_state, State) ->
    {reply, {ok, State}, State};
handle_msg(refresh_credentials, State) ->
    {Reply, NewState} = load_credentials(State),
    {reply, Reply, NewState};
handle_msg({set_credentials, AccessKey, SecretAccessKey}, State) ->
    {reply, ok, State#state{
        access_key = AccessKey,
        secret_access_key = SecretAccessKey,
        security_token = undefined,
        expiration = undefined,
        error = undefined
    }};
handle_msg({set_credentials, NewState}, State) ->
    {reply, ok, State#state{
        access_key = NewState#state.access_key,
        secret_access_key = NewState#state.secret_access_key,
        security_token = NewState#state.security_token,
        expiration = NewState#state.expiration,
        error = NewState#state.error
    }};
handle_msg({set_region, Region}, State) ->
    {reply, ok, State#state{region = Region}};
handle_msg({set_imdsv2_token, Imdsv2Token}, State) ->
    {reply, ok, State#state{imdsv2_token = Imdsv2Token}};
handle_msg(has_credentials, State) ->
    {reply, has_credentials(State), State};
handle_msg(get_imdsv2_token, State) ->
    {reply, {ok, State#state.imdsv2_token}, State};
handle_msg(_Request, State) ->
    {noreply, State}.

-spec endpoint(
    State :: state(),
    Host :: string(),
    Service :: string(),
    Path :: string()
) -> string().
%% @doc Return the endpoint URL, either by constructing it with the service
%%      information passed in, or by using the passed in Host value.
%% @ednd
endpoint(#state{region = Region}, undefined, Service, Path) ->
    lists:flatten(["https://", endpoint_host(Region, Service), Path]);
endpoint(_, Host, _, Path) ->
    lists:flatten(["https://", Host, Path]).

-spec endpoint_host(Region :: region(), Service :: string()) -> host().
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

-spec has_credentials() -> boolean().
has_credentials() ->
    gen_server:call(rabbitmq_aws, has_credentials).

-spec has_credentials(state()) -> boolean().
%% @doc check to see if there are credentials made available in the current state
%%      returning false if not or if they have expired.
%% @end
has_credentials(#state{error = Error}) when Error /= undefined -> false;
has_credentials(#state{access_key = Key}) when Key /= undefined -> true;
has_credentials(_) -> false.

-spec expired_credentials(Expiration :: calendar:datetime()) -> boolean().
%% @doc Indicates if the date that is passed in has expired.
%% end
expired_credentials(undefined) ->
    false;
expired_credentials(Expiration) ->
    Now = calendar:datetime_to_gregorian_seconds(local_time()),
    Expires = calendar:datetime_to_gregorian_seconds(Expiration),
    Now >= Expires.

-spec load_credentials(State :: state()) -> {ok, state()} | {error, state()}.
%% @doc Load the credentials using the following order of configuration precedence:
%%        - Environment variables
%%        - Credentials file
%%        - EC2 Instance Metadata Service
%% @end
load_credentials(#state{region = Region}) ->
    case rabbitmq_aws_config:credentials() of
        {ok, AccessKey, SecretAccessKey, Expiration, SecurityToken} ->
            {ok, #state{
                region = Region,
                error = undefined,
                access_key = AccessKey,
                secret_access_key = SecretAccessKey,
                expiration = Expiration,
                security_token = SecurityToken,
                imdsv2_token = undefined
            }};
        {error, Reason} ->
            ?LOG_ERROR(
                "Could not load AWS credentials from environment variables, AWS_CONFIG_FILE, AWS_SHARED_CREDENTIALS_FILE or EC2 metadata endpoint: ~tp. Will depend on config settings to be set~n",
                [Reason]
            ),
            {error, #state{
                region = Region,
                error = Reason,
                access_key = undefined,
                secret_access_key = undefined,
                expiration = undefined,
                security_token = undefined,
                imdsv2_token = undefined
            }}
    end.

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

-spec perform_request(
    State :: state(),
    Service :: string(),
    Method :: method(),
    Headers :: headers(),
    Path :: path(),
    Body :: body(),
    Options :: http_options(),
    Host :: string() | undefined
) ->
    {Result :: result(), NewState :: state()}.
%% @doc Make the API request and return the formatted response.
%% @end
perform_request(State, Service, Method, Headers, Path, Body, Options, Host) ->
    perform_request_has_creds(
        has_credentials(State),
        State,
        Service,
        Method,
        Headers,
        Path,
        Body,
        Options,
        Host
    ).

-spec perform_request_has_creds(
    HasCreds :: boolean(),
    State :: state(),
    Service :: string(),
    Method :: method(),
    Headers :: headers(),
    Path :: path(),
    Body :: body(),
    Options :: http_options(),
    Host :: string() | undefined
) ->
    {Result :: result(), NewState :: state()}.
%% @doc Invoked after checking to see if there are credentials. If there are,
%%      validate they have not or will not expire, performing the request if not,
%%      otherwise return an error result.
%% @end
perform_request_has_creds(true, State, Service, Method, Headers, Path, Body, Options, Host) ->
    perform_request_creds_expired(
        expired_credentials(State#state.expiration),
        State,
        Service,
        Method,
        Headers,
        Path,
        Body,
        Options,
        Host
    );
perform_request_has_creds(false, State, _, _, _, _, _, _, _) ->
    perform_request_creds_error(State).

-spec perform_request_creds_expired(
    CredsExp :: boolean(),
    State :: state(),
    Service :: string(),
    Method :: method(),
    Headers :: headers(),
    Path :: path(),
    Body :: body(),
    Options :: http_options(),
    Host :: string() | undefined
) ->
    {Result :: result(), NewState :: state()}.
%% @doc Invoked after checking to see if the current credentials have expired.
%%      If they haven't, perform the request, otherwise try and refresh the
%%      credentials before performing the request.
%% @end
perform_request_creds_expired(false, State, Service, Method, Headers, Path, Body, Options, Host) ->
    perform_request_with_creds(State, Service, Method, Headers, Path, Body, Options, Host);
perform_request_creds_expired(true, State, _, _, _, _, _, _, _) ->
    perform_request_creds_error(State#state{error = "Credentials expired!"}).

-spec perform_request_with_creds(
    State :: state(),
    Service :: string(),
    Method :: method(),
    Headers :: headers(),
    Path :: path(),
    Body :: body(),
    Options :: http_options(),
    Host :: string() | undefined
) ->
    {Result :: result(), NewState :: state()}.
%% @doc Once it is validated that there are credentials to try and that they have not
%%      expired, perform the request and return the response.
%% @end
perform_request_with_creds(State, Service, Method, Headers, Path, Body, Options, Host) ->
    URI = endpoint(State, Host, Service, Path),
    SignedHeaders = sign_headers(State, Service, Method, URI, Headers, Body),
    perform_request_with_creds(State, Method, URI, SignedHeaders, Body, Options).

-spec perform_request_with_creds(
    State :: state(),
    Method :: method(),
    URI :: string(),
    Headers :: headers(),
    Body :: body(),
    Options :: http_options()
) ->
    {Result :: result(), NewState :: state()}.
%% @doc Once it is validated that there are credentials to try and that they have not
%%      expired, perform the request and return the response.
%% @end
perform_request_with_creds(State, Method, URI, Headers, "", Options0) ->
    Response = gun_request(Method, URI, Headers, <<>>, Options0),
    {Response, State};
perform_request_with_creds(State, Method, URI, Headers, Body, Options0) ->
    Response = gun_request(Method, URI, Headers, Body, Options0),
    {Response, State}.

-spec perform_request_creds_error(State :: state()) ->
    {result_error(), NewState :: state()}.
%% @doc Return the error response when there are not any credentials to use with
%%      the request.
%% @end
perform_request_creds_error(State) ->
    {{error, {credentials, State#state.error}}, State}.

%% @doc Ensure that the timeout option is set and greater than 0 and less
%%      than about 1/2 of the default gen_server:call timeout. This gives
%%      enough time for a long connect and request phase to succeed.
%% @end
-spec ensure_timeout(Options :: http_options()) -> http_options().
ensure_timeout(Options) ->
    case proplists:get_value(timeout, Options) of
        undefined ->
            Options ++ [{timeout, ?DEFAULT_HTTP_TIMEOUT}];
        Value when is_integer(Value) andalso Value >= 0 andalso Value =< ?DEFAULT_HTTP_TIMEOUT ->
            Options;
        _ ->
            Options1 = proplists:delete(timeout, Options),
            Options1 ++ [{timeout, ?DEFAULT_HTTP_TIMEOUT}]
    end.

-spec sign_headers(
    State :: state(),
    Service :: string(),
    Method :: method(),
    URI :: string(),
    Headers :: headers(),
    Body :: body()
) -> headers().
%% @doc Build the signed headers for the API request.
%% @end
sign_headers(
    #state{
        access_key = AccessKey,
        secret_access_key = SecretKey,
        security_token = SecurityToken,
        region = Region
    },
    Service,
    Method,
    URI,
    Headers,
    Body
) ->
    rabbitmq_aws_sign:headers(#request{
        access_key = AccessKey,
        secret_access_key = SecretKey,
        security_token = SecurityToken,
        region = Region,
        service = Service,
        method = Method,
        uri = URI,
        headers = Headers,
        body = Body
    }).

-spec expired_imdsv2_token('undefined' | imdsv2token()) -> boolean().
%% @doc Determine whether or not an Imdsv2Token has expired.
%% @end
expired_imdsv2_token(undefined) ->
    ?LOG_DEBUG("EC2 IMDSv2 token has not yet been obtained"),
    true;
expired_imdsv2_token({_, _, undefined}) ->
    ?LOG_DEBUG("EC2 IMDSv2 token is not available"),
    true;
expired_imdsv2_token({_, _, Expiration}) ->
    Now = calendar:datetime_to_gregorian_seconds(local_time()),
    HasExpired = Now >= Expiration,
    ?LOG_DEBUG("EC2 IMDSv2 token has expired: ~tp", [HasExpired]),
    HasExpired.

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

-spec ensure_credentials_valid() -> ok.
%% @doc Invoked before each AWS service API request to check if the current credentials are available and that they have not expired.
%%      If the credentials are available and are still current, then move on and perform the request.
%%      If the credentials are not available or have expired, then refresh them before performing the request.
%% @end
ensure_credentials_valid() ->
    ?LOG_DEBUG("Making sure AWS credentials are available and still valid"),
    {ok, State} = gen_server:call(rabbitmq_aws, get_state),
    case has_credentials(State) of
        true ->
            case expired_credentials(State#state.expiration) of
                true -> refresh_credentials(State);
                _ -> ok
            end;
        _ ->
            refresh_credentials(State)
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
    Reply = direct_gun_request(GunPid, Method, Path, Headers, Body, ensure_timeout(Options)),
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
    ConnectTimeout = proplists:get_value(connect_timeout, Options, 5000),
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

%%====================================================================
%% New Concurrent API Helper Functions
%%====================================================================

%% Create a direct connection handle
-spec create_direct_connection(State :: state(), Service :: string(), Options :: list()) ->
    {ok, connection_handle()} | {error, term()}.
create_direct_connection(State, Service, Options) ->
    Region = State#state.region,
    Host = endpoint_host(Region, Service),
    Port = 443,
    GunPid = create_gun_connection(Host, Port, Options),
    CredContext = #{
        access_key => State#state.access_key,
        secret_access_key => State#state.secret_access_key,
        security_token => State#state.security_token,
        region => Region,
        service => Service
    },
    {ok, {GunPid, CredContext}}.

%% Sign headers using credential context (no gen_server state needed)
-spec sign_headers_with_context(
    CredContext :: credential_context(),
    Method :: method(),
    URI :: string(),
    Headers :: headers(),
    Body :: body()
) -> headers().
sign_headers_with_context(CredContext, Method, URI, Headers, Body) ->
    #{
        access_key := AccessKey,
        secret_access_key := SecretKey,
        security_token := SecurityToken,
        region := Region,
        service := Service
    } = CredContext,
    rabbitmq_aws_sign:headers(#request{
        access_key = AccessKey,
        secret_access_key = SecretKey,
        security_token = SecurityToken,
        region = Region,
        service = Service,
        method = Method,
        uri = URI,
        headers = Headers,
        body = Body
    }).

%% Direct Gun request (extracted from existing gun_request function)
-spec direct_gun_request(
    GunPid :: gun:conn_ref(),
    Method :: method(),
    Path :: path(),
    Headers :: headers(),
    Body :: body(),
    Options :: list()
) -> result().
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

%% Internal credential validation (extracted from existing logic)
-spec ensure_credentials_valid_internal(State :: state()) -> {ok, state()} | {error, term()}.
ensure_credentials_valid_internal(State) ->
    case has_credentials(State) of
        true ->
            case expired_credentials(State#state.expiration) of
                false -> {ok, State};
                true -> load_credentials(State)
            end;
        false ->
            load_credentials(State)
    end.
