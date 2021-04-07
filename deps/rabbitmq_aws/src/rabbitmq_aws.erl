%% ====================================================================
%% @author Gavin M. Roy <gavinmroy@gmail.com>
%% @copyright 2016, Gavin M. Roy
%% @doc rabbitmq_aws client library
%% @end
%% ====================================================================
-module(rabbitmq_aws).

-behavior(gen_server).

%% API exports
-export([get/2, get/3,
         post/4,
         refresh_credentials/0,
         request/5, request/6, request/7,
         set_credentials/2,
         has_credentials/0,
         set_region/1,
         ensure_imdsv2_token_valid/0,
         api_get_request/2]).

%% gen-server exports
-export([start_link/0,
         init/1,
         terminate/2,
         code_change/3,
         handle_call/3,
         handle_cast/2,
         handle_info/2]).

%% Export all for unit tests
-ifdef(TEST).
-compile(export_all).
-endif.

-include("rabbitmq_aws.hrl").

%%====================================================================
%% exported wrapper functions
%%====================================================================

-spec get(Service :: string(),
          Path :: path()) -> result().
%% @doc Perform a HTTP GET request to the AWS API for the specified service. The
%%      response will automatically be decoded if it is either in JSON or XML
%%      format.
%% @end
get(Service, Path) ->
  get(Service, Path, []).


-spec get(Service :: string(),
          Path :: path(),
          Headers :: headers()) -> result().
%% @doc Perform a HTTP GET request to the AWS API for the specified service. The
%%      response will automatically be decoded if it is either in JSON or XML
%%      format.
%% @end
get(Service, Path, Headers) ->
  request(Service, get, Path, "", Headers).


-spec post(Service :: string(),
           Path :: path(),
           Body :: body(),
           Headers :: headers()) -> result().
%% @doc Perform a HTTP Post request to the AWS API for the specified service. The
%%      response will automatically be decoded if it is either in JSON or XML
%%      format.
%% @end
post(Service, Path, Body, Headers) ->
  request(Service, post, Path, Body, Headers).


-spec refresh_credentials() -> ok | error.
%% @doc Manually refresh the credentials from the environment, filesystem or EC2
%%      Instance metadata service.
%% @end
refresh_credentials() ->
  gen_server:call(rabbitmq_aws, refresh_credentials).


-spec refresh_credentials(state()) -> ok | error.
%% @doc Manually refresh the credentials from the environment, filesystem or EC2
%%      Instance metadata service.
%% @end
refresh_credentials(State) ->
  rabbit_log:debug("Refreshing AWS credentials..."),
  {_, NewState} = load_credentials(State),
  rabbit_log:debug("AWS credentials has been refreshed."),
  set_credentials(NewState).


-spec request(Service :: string(),
              Method :: method(),
              Path :: path(),
              Body :: body(),
              Headers :: headers()) -> result().
%% @doc Perform a HTTP request to the AWS API for the specified service. The
%%      response will automatically be decoded if it is either in JSON or XML
%%      format.
%% @end
request(Service, Method, Path, Body, Headers) ->
  gen_server:call(rabbitmq_aws, {request, Service, Method, Headers, Path, Body, [], undefined}).


-spec request(Service :: string(),
              Method :: method(),
              Path :: path(),
              Body :: body(),
              Headers :: headers(),
              HTTPOptions :: http_options()) -> result().
%% @doc Perform a HTTP request to the AWS API for the specified service. The
%%      response will automatically be decoded if it is either in JSON or XML
%%      format.
%% @end
request(Service, Method, Path, Body, Headers, HTTPOptions) ->
  gen_server:call(rabbitmq_aws, {request, Service, Method, Headers, Path, Body, HTTPOptions, undefined}).


-spec request(Service :: string(),
              Method :: method(),
              Path :: path(),
              Body :: body(),
              Headers :: headers(),
              HTTPOptions :: http_options(),
              Endpoint :: host()) -> result().
%% @doc Perform a HTTP request to the AWS API for the specified service, overriding
%%      the endpoint URL to use when invoking the API. This is useful for local testing
%%      of services such as DynamoDB. The response will automatically be decoded
%%      if it is either in JSON or XML format.
%% @end
request(Service, Method, Path, Body, Headers, HTTPOptions, Endpoint) ->
  gen_server:call(rabbitmq_aws, {request, Service, Method, Headers, Path, Body, HTTPOptions, Endpoint}).

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


-spec get_imdsv2_token() -> imdsv2token().
%% @doc return the current Imdsv2Token used to perform instance metadata service requests.
%% @end
get_imdsv2_token() ->
  {ok, Imdsv2Token}=gen_server:call(rabbitmq_aws, get_imdsv2_token),
  Imdsv2Token.


%%====================================================================
%% gen_server functions
%%====================================================================

start_link() ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).


-spec init(list()) -> {ok, state()}.
init([]) ->
  {ok, #state{}}.


terminate(_, _) ->
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
    {Response, NewState} = perform_request(State, Service, Method, Headers, Path, Body, Options, Host),
    {reply, Response, NewState};

handle_msg(get_state, State) ->
    {reply, {ok, State}, State};

handle_msg(refresh_credentials, State) ->
    {Reply, NewState} = load_credentials(State),
    {reply, Reply, NewState};

handle_msg({set_credentials, AccessKey, SecretAccessKey}, State) ->
    {reply, ok, State#state{access_key = AccessKey,
                            secret_access_key = SecretAccessKey,
                            security_token = undefined,
                            expiration = undefined,
                            error = undefined}};

handle_msg({set_credentials, NewState}, State) ->
  {reply, ok, State#state{access_key = NewState#state.access_key,
                          secret_access_key = NewState#state.secret_access_key,
                          security_token = NewState#state.security_token,
                          expiration = NewState#state.expiration,
                          error = NewState#state.error}};

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


-spec endpoint(State :: state(), Host :: string(),
               Service :: string(), Path :: string()) -> string().
%% @doc Return the endpoint URL, either by constructing it with the service
%%      information passed in or by using the passed in Host value.
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
format_response({ok, {{_Version, StatusCode, Message}, Headers, Body}}) when StatusCode >= 400 ->
  {error, Message, {Headers, maybe_decode_body(get_content_type(Headers), Body)}};
format_response({error, Reason}) ->
  {error, Reason, undefined}.

-spec get_content_type(Headers :: headers()) -> {Type :: string(), Subtype :: string()}.
%% @doc Fetch the content type from the headers and return it as a tuple of
%%      {Type, Subtype}.
%% @end
get_content_type(Headers) ->
  Value = case proplists:get_value("content-type", Headers, undefined) of
    undefined ->
      proplists:get_value("Content-Type", Headers, "text/xml");
    Other -> Other
  end,
  parse_content_type(Value).

-spec has_credentials() -> true | false.
has_credentials() ->
  gen_server:call(rabbitmq_aws, has_credentials).

-spec has_credentials(state()) -> true | false.
%% @doc check to see if there are credentials made available in the current state
%%      returning false if not or if they have expired.
%% @end
has_credentials(#state{error = Error}) when Error /= undefined -> false;
has_credentials(#state{access_key = Key}) when Key /= undefined -> true;
has_credentials(_) -> false.


-spec expired_credentials(Expiration :: calendar:datetime()) -> true | false.
%% @doc Indicates if the date that is passed in has expired.
%% end
expired_credentials(undefined) -> false;
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
      {ok, #state{region = Region,
                  error = undefined,
                  access_key = AccessKey,
                  secret_access_key = SecretAccessKey,
                  expiration = Expiration,
                  security_token = SecurityToken,
                  imdsv2_token = undefined}};
    {error, Reason} ->
      error_logger:error_msg("Could not load AWS credentials from environment variables, AWS_CONFIG_FILE, AWS_SHARED_CREDENTIALS_FILE or EC2 metadata endpoint: ~p. Will depend on config settings to be set.~n.", [Reason]),
      {error, #state{region = Region,
                     error = Reason,
                     access_key = undefined,
                     secret_access_key = undefined,
                     expiration = undefined,
                     security_token = undefined,
                     imdsv2_token = undefined}}
  end.


-spec local_time() -> calendar:datetime().
%% @doc Return the current local time.
%% @end
local_time() ->
  [Value] = calendar:local_time_to_universal_time_dst(calendar:local_time()),
  Value.


-spec maybe_decode_body(ContentType :: {nonempty_string(), nonempty_string()}, Body :: body()) -> list() | body().
%% @doc Attempt to decode the response body based upon the mime type that is
%%      presented.
%% @end.
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
parse_content_type(ContentType) ->
  Parts = string:tokens(ContentType, ";"),
  [Type, Subtype] = string:tokens(lists:nth(1, Parts), "/"),
  {Type, Subtype}.


-spec perform_request(State :: state(), Service :: string(), Method :: method(),
                      Headers :: headers(), Path :: path(), Body :: body(),
                      Options :: http_options(), Host :: string() | undefined)
    -> {Result :: result(), NewState :: state()}.
%% @doc Make the API request and return the formatted response.
%% @end
perform_request(State, Service, Method, Headers, Path, Body, Options, Host) ->
  perform_request_has_creds(has_credentials(State), State, Service, Method,
                            Headers, Path, Body, Options, Host).


-spec perform_request_has_creds(true | false, State :: state(),
                                Service :: string(), Method :: method(),
                                Headers :: headers(), Path :: path(), Body :: body(),
                                Options :: http_options(), Host :: string() | undefined)
    -> {Result :: result(), NewState :: state()}.
%% @doc Invoked after checking to see if there are credentials. If there are,
%%      validate they have not or will not expire, performing the request if not,
%%      otherwise return an error result.
%% @end
perform_request_has_creds(true, State, Service, Method, Headers, Path, Body, Options, Host) ->
  perform_request_creds_expired(expired_credentials(State#state.expiration), State,
                                Service, Method, Headers, Path, Body, Options, Host);
perform_request_has_creds(false, State, _, _, _, _, _, _, _) ->
  perform_request_creds_error(State).


-spec perform_request_creds_expired(true | false, State :: state(),
                                    Service :: string(), Method :: method(),
                                    Headers :: headers(), Path :: path(), Body :: body(),
                                    Options :: http_options(), Host :: string() | undefined)
  -> {Result :: result(), NewState :: state()}.
%% @doc Invoked after checking to see if the current credentials have expired.
%%      If they haven't, perform the request, otherwise try and refresh the
%%      credentials before performing the request.
%% @end
perform_request_creds_expired(false, State, Service, Method, Headers, Path, Body, Options, Host) ->
  perform_request_with_creds(State, Service, Method, Headers, Path, Body, Options, Host);
perform_request_creds_expired(true, State, _, _, _, _, _, _, _) ->
  perform_request_creds_error(State#state{error = "Credentials expired!"}).


-spec perform_request_with_creds(State :: state(), Service :: string(), Method :: method(),
                                 Headers :: headers(), Path :: path(), Body :: body(),
                                 Options :: http_options(), Host :: string() | undefined)
    -> {Result :: result(), NewState :: state()}.
%% @doc Once it is validated that there are credentials to try and that they have not
%%      expired, perform the request and return the response.
%% @end
perform_request_with_creds(State, Service, Method, Headers, Path, Body, Options, Host) ->
  URI = endpoint(State, Host, Service, Path),
  SignedHeaders = sign_headers(State, Service, Method, URI, Headers, Body),
  ContentType = proplists:get_value("content-type", SignedHeaders, undefined),
  perform_request_with_creds(State, Method, URI, SignedHeaders, ContentType, Body, Options).


-spec perform_request_with_creds(State :: state(), Method :: method(), URI :: string(),
                                 Headers :: headers(), ContentType :: string() | undefined,
                                 Body :: body(), Options :: http_options())
    -> {Result :: result(), NewState :: state()}.
%% @doc Once it is validated that there are credentials to try and that they have not
%%      expired, perform the request and return the response.
%% @end
perform_request_with_creds(State, Method, URI, Headers, undefined, "", Options0) ->
  Options1 = ensure_timeout(Options0),
  Response = httpc:request(Method, {URI, Headers}, Options1, []),
  {format_response(Response), State};
perform_request_with_creds(State, Method, URI, Headers, ContentType, Body, Options0) ->
  Options1 = ensure_timeout(Options0),
  Response = httpc:request(Method, {URI, Headers, ContentType, Body}, Options1, []),
  {format_response(Response), State}.


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


-spec sign_headers(State :: state(), Service :: string(), Method :: method(),
                   URI :: string(), Headers :: headers(), Body :: body()) -> headers().
%% @doc Build the signed headers for the API request.
%% @end
sign_headers(#state{access_key = AccessKey,
                    secret_access_key = SecretKey,
                    security_token = SecurityToken,
                    region = Region}, Service, Method, URI, Headers, Body) ->
  rabbitmq_aws_sign:headers(#request{access_key = AccessKey,
                                  secret_access_key = SecretKey,
                                  security_token = SecurityToken,
                                  region = Region,
                                  service = Service,
                                  method = Method,
                                  uri = URI,
                                  headers = Headers,
                                  body = Body}).

-spec expired_imdsv2_token(imdsv2token()) -> boolean().
%% @doc Determine whether an Imdsv2Token has expired or not.
%% @end
expired_imdsv2_token(undefined) ->
  rabbit_log:debug("EC2 IMDSv2 token has not yet been obtained."),
  true;
expired_imdsv2_token({_, _, undefined}) ->
  rabbit_log:debug("EC2 IMDSv2 token is not available."),
  true;
expired_imdsv2_token({_, _, Expiration}) ->
  Now = calendar:datetime_to_gregorian_seconds(local_time()),
  HasExpired = Now >= Expiration,
  rabbit_log:debug("EC2 IMDSv2 token has expired: ~p", [HasExpired]),
  HasExpired.


-spec ensure_imdsv2_token_valid() -> imdsv2token().
ensure_imdsv2_token_valid() ->
  Imdsv2Token=get_imdsv2_token(),
  case expired_imdsv2_token(Imdsv2Token) of
    true -> Value=rabbitmq_aws_config:load_imdsv2_token(),
            Expiration=calendar:datetime_to_gregorian_seconds(local_time()) + ?METADATA_TOKEN_TLL_SECONDS,
            set_imdsv2_token(#imdsv2token{token = Value,
                                          expiration = Expiration}),
            Value;
    _    -> Imdsv2Token#imdsv2token.token
  end.

-spec ensure_credentials_valid() -> ok.
%% @doc Invoked before each AWS service API request checking to see if credentials available
%%      or whether the current credentials have expired.
%%      If they haven't, move on performing the request, otherwise try and refresh the
%%      credentials before performing the request.
%% @end
ensure_credentials_valid() ->
  rabbit_log:debug("Making sure AWS credentials is available and still valid."),
  {ok, State}=gen_server:call(rabbitmq_aws, get_state),
  case has_credentials(State) of
    true -> case expired_credentials(State#state.expiration) of
              true -> refresh_credentials(State);
              _    -> ok
            end;
    _    ->  refresh_credentials(State)
  end.


-spec api_get_request(string(), path()) -> result().
%% @doc Invoke an API call to an AWS service.
%% @end
api_get_request(Service, Path) ->
  rabbit_log:debug("Invoking AWS request {Service: ~p; Path: ~p}...", [Service, Path]),
  ensure_credentials_valid(),
  case get(Service, Path) of
    {ok, {_Headers, Payload}} -> rabbit_log:debug("AWS request: ~s~nResponse: ~p", [Path, Payload]),
                                 {ok, Payload};
    {error, {credentials, _}} -> {error, credentials};
    {error, Message, _}       -> {error, Message}
  end.
