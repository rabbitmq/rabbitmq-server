%% ====================================================================
%% @author Gavin M. Roy <gavinmroy@gmail.com>
%% @copyright 2016, Gavin M. Roy
%% @copyright 2016-2021 VMware, Inc. or its affiliates.
%% @headerfile
%% @private
%% @doc rabbitmq_aws client library constants and records
%% @end
%% ====================================================================

-define(MIME_AWS_JSON, "application/x-amz-json-1.0").
-define(SCHEME, https).

-define(DEFAULT_REGION, "us-east-1").
-define(DEFAULT_PROFILE, "default").

-define(INSTANCE_AZ, "placement/availability-zone").
-define(INSTANCE_HOST, "169.254.169.254").

% rabbitmq/rabbitmq-peer-discovery-aws#25

% Note: this timeout must not be greater than the default
% gen_server:call timeout of 5000ms. INSTANCE_HOST is
% a pseudo-ip that should have good performance, and the
% data should be returned quickly. Note that `timeout`,
% when set, is used as the connect and then request timeout
% by `httpc`
-define(DEFAULT_HTTP_TIMEOUT, 2250).

-define(INSTANCE_CREDENTIALS, "iam/security-credentials").
-define(INSTANCE_METADATA_BASE, "latest/meta-data").
-define(INSTANCE_ID, "instance-id").

-define(TOKEN_URL, "latest/api/token").

-define(METADATA_TOKEN_TLL_HEADER, "X-aws-ec2-metadata-token-ttl-seconds").

% EC2 Instance Metadata service version 2 (IMDSv2) uses session-oriented authentication.
% Instance metadata service requests are only needed for loading/refreshing credentials.
% We dont need to have long-live metadata token.
% In fact, we only need the token is valid for a sufficient period to successfully
% load/refresh credentials. 60 seconds is more than enough for that goal.
-define(METADATA_TOKEN_TLL_SECONDS, 60).

-define(METADATA_TOKEN, "X-aws-ec2-metadata-token").

-type access_key() :: nonempty_string().
-type secret_access_key() :: nonempty_string().
-type expiration() :: calendar:datetime() | undefined.
-type security_token() :: nonempty_string() | undefined.
-type region() :: nonempty_string() | undefined.
-type path() :: ssl:path().

-type sc_ok() :: {ok, access_key(), secret_access_key(), expiration(), security_token()}.
-type sc_error() :: {error, Reason :: atom()}.
-type security_credentials() :: sc_ok() | sc_error().

-record(imdsv2token, { token :: security_token() | undefined,
                       expiration :: expiration() | undefined}).

-type imdsv2token() :: #imdsv2token{}.

-record(state, {access_key :: access_key() | undefined,
                secret_access_key :: secret_access_key() | undefined,
                expiration :: expiration() | undefined,
                security_token :: security_token() | undefined,
                region :: region() | undefined,
                imdsv2_token:: imdsv2token() | undefined,
                error :: atom() | string() | undefined}).
-type state() :: #state{}.

-type scheme() :: atom().
-type username() :: string().
-type password() :: string().
-type host() :: string().
-type tcp_port() :: integer().
-type query_args() :: [tuple() | string()].
-type fragment() :: string().

-type userinfo() :: {undefined | username(),
                     undefined | password()}.

-type authority() :: {undefined | userinfo(),
                      host(),
                      undefined | tcp_port()}.
-record(uri, {scheme :: undefined | scheme(),
              authority :: authority(),
              path :: undefined | path(),
              query :: undefined | query_args(),
              fragment :: undefined | fragment()}).

-type method() :: head | get | put | post | trace | options | delete | patch.
-type http_version() :: string().
-type status_code() :: integer().
-type reason_phrase() :: string().
-type status_line() :: {http_version(), status_code(), reason_phrase()}.
-type field() :: string().
-type value() :: string().
-type header() :: {Field :: field(), Value :: value()}.
-type headers() :: [header()].
-type body() :: string() | binary().

-type ssl_options() :: [ssl:ssl_option()].

-type http_option() :: {timeout, timeout()} |
                       {connect_timeout, timeout()} |
                       {ssl, ssl_options()} |
                       {essl, ssl_options()} |
                       {autoredirect, boolean()} |
                       {proxy_auth, {User :: string(), Password :: string()}} |
                       {version, http_version()} |
                       {relaxed, boolean()} |
                       {url_encode, boolean()}.
-type http_options() :: [http_option()].


-record(request, {access_key :: access_key(),
                  secret_access_key :: secret_access_key(),
                  security_token :: security_token(),
                  service :: string(),
                  region = "us-east-1" :: string(),
                  method = get :: method(),
                  headers = [] :: headers(),
                  uri :: string(),
                  body = "" :: body()}).
-type request() :: #request{}.

-type httpc_result() :: {ok, {status_line(), headers(), body()}} |
                        {ok, {status_code(), body()}} |
                        {error, term()}.

-type result_ok() :: {ok, {ResponseHeaders :: headers(), Response :: list()}}.
-type result_error() :: {error, Message :: reason_phrase(), {ResponseHeaders :: headers(), Response :: list()} | undefined} |
                        {error, {credentials, Reason :: string()}}.
-type result() :: result_ok() | result_error().
