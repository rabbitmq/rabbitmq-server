%% ====================================================================
%% @author Gavin M. Roy <gavinmroy@gmail.com>
%% @copyright 2016, Gavin M. Roy
%% @private
%% @doc httpc_aws request signing methods
%% @end
%% ====================================================================
-module(httpc_aws_sign).

%% API
-export([headers/1]).

%% Export all for unit tests
-ifdef(TEST).
-compile(export_all).
-endif.

-include("httpc_aws.hrl").

-define(ALGORITHM, "AWS4-HMAC-SHA256").
-define(ISOFORMAT_BASIC, "~4.10.0b~2.10.0b~2.10.0bT~2.10.0b~2.10.0b~2.10.0bZ").

-spec headers(v4request()) -> httpc:headers().
%% @doc Create the signed request headers
%% end
headers(Request) ->
  RequestTimestamp = local_time(),
  PayloadHash = sha256(Request#v4request.body),
  URI = httpc_aws_urilib:parse(Request#v4request.uri),
  {_, Host, _} = URI#uri.authority,
  Headers = append_headers(RequestTimestamp,
                           length(Request#v4request.body),
                           PayloadHash,
                           Host,
                           Request#v4request.security_token,
                           Request#v4request.headers),
  RequestHash = request_hash(Request#v4request.method,
                             URI#uri.path,
                             URI#uri.query,
                             Headers,
                             Request#v4request.body),
  AuthValue = authorization(Request#v4request.access_key,
                            Request#v4request.secret_access_key,
                            RequestTimestamp,
                            Request#v4request.region,
                            Request#v4request.service,
                            Headers,
                            RequestHash),
  sort_headers(lists:merge([{"Authorization", AuthValue}], Headers)).


-spec amz_date(AMZTimestamp :: string()) -> string().
%% @doc Extract the date from the AMZ timestamp format.
%% @end
amz_date(AMZTimestamp) ->
  [RequestDate, _] = string:tokens(AMZTimestamp, "T"),
  RequestDate.


-spec append_headers(AMZDate :: string(),
                     ContentLength :: integer(),
                     PayloadHash :: string(),
                     Hostname :: string(),
                     SecurityToken :: string() | undefined,
                     Headers :: list()) -> list().
%% @doc Append the headers that need to be signed to the headers passed in with
%%      the request
%% @end
append_headers(AMZDate, ContentLength, PayloadHash, Hostname, undefined, Headers) ->
  sort_headers(lists:merge(base_headers(AMZDate, ContentLength, PayloadHash, Hostname), Headers));
append_headers(AMZDate, ContentLength, PayloadHash, Hostname, SecurityToken, Headers) ->
  sort_headers(lists:merge([{"X-Amz-Security-Token", SecurityToken}],
                            lists:merge(base_headers(AMZDate, ContentLength, PayloadHash, Hostname),
                                        Headers))).

-spec authorization(AccessKey :: access_key(),
                    SecretAccessKey :: secret_access_key(),
                    RequestTimestamp :: string(),
                    Region :: string(),
                    Service :: string(),
                    Headers :: list(),
                    RequestHash :: string()) -> string().
%% @doc Return the authorization header value
%% @end
authorization(AccessKey, SecretAccessKey, RequestTimestamp, Region, Service, Headers, RequestHash) ->
  RequestDate = amz_date(RequestTimestamp),
  Scope = scope(RequestDate, Region, Service),
  Credentials = ?ALGORITHM ++ " Credential=" ++ AccessKey ++ "/" ++ Scope,
  SignedHeaders = "SignedHeaders=" ++ signed_headers(Headers),
  StringToSign = string_to_sign(RequestTimestamp, RequestDate, Region, Service, RequestHash),
  SigningKey = signing_key(SecretAccessKey, RequestDate, Region, Service),
  Signature = string:join(["Signature", signature(StringToSign, SigningKey)], "="),
  string:join([Credentials, SignedHeaders, Signature], ", ").


-spec base_headers(RequestTimestamp :: string(),
                   ContentLength :: integer(),
                   PayloadHash :: string(),
                   Hostname :: string()) -> list().
%% @doc build the base headers that are merged in with the headers for every
%%      request.
%% @end
base_headers(RequestTimestamp, ContentLength, PayloadHash, Hostname) ->
  [{"Content-Length", integer_to_list(ContentLength)},
   {"Date", RequestTimestamp},
   {"Host", Hostname},
   {"X-Amz-Content-sha256", PayloadHash}].


-spec canonical_headers(Headers :: list()) -> string().
%% @doc Convert the headers list to a line-feed delimited string in the AWZ
%%      canonical headers format.
%% @end
canonical_headers(Headers) ->
  canonical_headers(sort_headers(Headers), []).

-spec canonical_headers(Headers :: list(), CanonicalHeaders :: list()) -> string().
%% @doc Convert the headers list to a line-feed delimited string in the AWZ
%%      canonical headers format.
%% @end
canonical_headers([], CanonicalHeaders) ->
  lists:flatten(CanonicalHeaders);
canonical_headers([{Key, Value}|T], CanonicalHeaders) ->
  Header = string:join([string:to_lower(Key), to_list(Value)], ":") ++ "\n",
  canonical_headers(T, lists:append(CanonicalHeaders, [Header])).


-spec credential_scope(RequestDate :: string(),
                       Region :: string(),
                       Service :: string()) -> string().
%% @doc Return the credential scope string used in creating the request string to sign.
%% @end
credential_scope(RequestDate, Region, Service) ->
  lists:flatten(string:join([RequestDate, Region, Service, "aws4_request"], "/")).


-spec hmac_sign(Key :: string(), Message :: string()) -> string().
%% @doc Return the SHA-256 hash for the specified value.
%% @end
hmac_sign(Key, Message) ->
  Context = crypto:hmac_init(sha256, Key),
  crypto:hmac_update(Context, Message),
  SignedValue = crypto:hmac_final(Context),
  binary_to_list(SignedValue).


-spec local_time() -> string().
%% @doc Return the current timestamp in GMT formatted in ISO8601 basic format.
%% @end
local_time() ->
  [LocalTime] = calendar:local_time_to_universal_time_dst(calendar:local_time()),
  local_time(LocalTime).


-spec local_time(calendar:datetime()) -> string().
%% @doc Return the current timestamp in GMT formatted in ISO8601 basic format.
%% @end
local_time({{Y,M,D},{HH,MM,SS}}) ->
  lists:flatten(io_lib:format(?ISOFORMAT_BASIC, [Y, M, D, HH, MM, SS])).


-spec query_string(QueryArgs :: list()) -> string().
%% @doc Return the sorted query string for the specified arguments.
%% @end
query_string(undefined) -> "";
query_string(QueryArgs) ->
  httpc_aws_urilib:build_query_string(lists:keysort(1, QueryArgs)).


-spec request_hash(Method :: httpc:method(),
                   Path :: string(),
                   QArgs :: list(),
                   Headers :: list(),
                   Payload :: string()) -> string().
%% @doc Create the request hash value
%% @end
request_hash(Method, Path, QArgs, Headers, Payload) ->
  EncodedPath = "/" ++ string:join([httpc_aws_urilib:percent_encode(P) || P <- string:tokens(Path, "/")], "/"),
  CanonicalRequest = string:join([string:to_upper(atom_to_list(Method)),
                                  EncodedPath,
                                  query_string(QArgs),
                                  canonical_headers(Headers),
                                  signed_headers(Headers),
                                  sha256(Payload)], "\n"),
  sha256(CanonicalRequest).


-spec scope(AMZDate :: string(), Region :: string(), Service :: string()) -> string().
%% @doc Create the Scope string
%% @end
scope(AMZDate, Region, Service) ->
  string:join([AMZDate, Region, Service, "aws4_request"], "/").


-spec sha256(Value :: string()) -> string().
%% @doc Return the SHA-256 hash for the specified value.
%% @end
sha256(Value) ->
  lists:flatten(io_lib:format("~64.16.0b",
                              [binary:decode_unsigned(crypto:hash(sha256, Value))])).


-spec signed_headers(Headers :: list()) -> string().
%% @doc Return the signed headers string of delimited header key names
%% @end
signed_headers(Headers) ->
  signed_headers(sort_headers(Headers), []).


-spec signed_headers(Headers :: list(), Values :: string()) -> string().
%% @doc Return the signed headers string of delimited header key names
%% @end
signed_headers([], Values) -> string:join(Values, ";");
signed_headers([{Key,_}|T], Values) ->
  signed_headers(T, lists:append(Values, [string:to_lower(Key)])).


-spec signature(StringToSign :: string(),
                SigningKey :: string()) -> string().
%% @doc Create the request signature.
%% @end
signature(StringToSign, SigningKey) ->
  Context = crypto:hmac_init(sha256, SigningKey),
  crypto:hmac_update(Context, StringToSign  ),
  SignedValue = crypto:hmac_final(Context),
  lists:flatten(io_lib:format("~64.16.0b", [binary:decode_unsigned(SignedValue)])).


-spec signing_key(SecretKey :: secret_access_key(),
                  AMZDate :: string(),
                  Region :: string(),
                  Service :: string()) -> string().
%% @doc Create the signing key
%% @end
signing_key(SecretKey, AMZDate, Region, Service) ->
  DateKey = hmac_sign("AWS4" ++ SecretKey, AMZDate),
  RegionKey = hmac_sign(DateKey, Region),
  ServiceKey = hmac_sign(RegionKey, Service),
  hmac_sign(ServiceKey, "aws4_request").


-spec string_to_sign(RequestTimestamp :: string(),
                     RequestDate :: string(),
                     Region :: string(),
                     Service :: string(),
                     RequestHash :: string()) -> string().
%% @doc Return the string to sign when creating the signed request.
%% @end
string_to_sign(RequestTimestamp, RequestDate, Region, Service, RequestHash) ->
  CredentialScope = credential_scope(RequestDate, Region, Service),
  lists:flatten(string:join([
    ?ALGORITHM,
    RequestTimestamp,
    CredentialScope,
    RequestHash
  ], "\n")).


-spec to_list(Value :: integer() | string()) -> string().
%% @doc Ensure the value is a string/list.
%% @end
to_list(Value) when is_integer(Value) -> integer_to_list(Value);
to_list(Value) -> Value.


sort_headers(Headers) ->
  lists:sort(fun({A,_}, {B, _}) -> string:to_lower(A) =< string:to_lower(B) end, Headers).

