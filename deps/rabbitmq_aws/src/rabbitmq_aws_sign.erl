%% ====================================================================
%% @author Gavin M. Roy <gavinmroy@gmail.com>
%% @copyright 2016, Gavin M. Roy
%% @private
%% @doc rabbitmq_aws request signing methods
%% @end
%% ====================================================================
-module(rabbitmq_aws_sign).

%% API
-export([headers/1, request_hash/5]).

%% Export all for unit tests
-ifdef(TEST).
-compile(export_all).
-endif.

-include("rabbitmq_aws.hrl").

-define(ALGORITHM, "AWS4-HMAC-SHA256").
-define(ISOFORMAT_BASIC, "~4.10.0b~2.10.0b~2.10.0bT~2.10.0b~2.10.0b~2.10.0bZ").

-spec headers(request()) -> headers().
%% @doc Create the signed request headers
%% end
headers(Request) ->
  RequestTimestamp = local_time(),
  PayloadHash = sha256(Request#request.body),
  URI = rabbitmq_aws_urilib:parse(Request#request.uri),
  {_, Host, _} = URI#uri.authority,
  Headers = append_headers(RequestTimestamp,
                           length(Request#request.body),
                           PayloadHash,
                           Host,
                           Request#request.security_token,
                           Request#request.headers),
  RequestHash = request_hash(Request#request.method,
                             URI#uri.path,
                             URI#uri.query,
                             Headers,
                             Request#request.body),
  AuthValue = authorization(Request#request.access_key,
                            Request#request.secret_access_key,
                            RequestTimestamp,
                            Request#request.region,
                            Request#request.service,
                            Headers,
                            RequestHash),
  sort_headers(lists:merge([{"authorization", AuthValue}], Headers)).


-spec amz_date(AMZTimestamp :: string()) -> string().
%% @doc Extract the date from the AMZ timestamp format.
%% @end
amz_date(AMZTimestamp) ->
  [RequestDate, _] = string:tokens(AMZTimestamp, "T"),
  RequestDate.


-spec append_headers(AMZDate :: string(),
                     ContentLength :: integer(),
                     PayloadHash :: string(),
                     Hostname :: host(),
                     SecurityToken :: security_token(),
                     Headers :: headers()) -> list().
%% @doc Append the headers that need to be signed to the headers passed in with
%%      the request
%% @end
append_headers(AMZDate, ContentLength, PayloadHash, Hostname, SecurityToken, Headers) ->
  Defaults = default_headers(AMZDate, ContentLength, PayloadHash, Hostname, SecurityToken),
  Headers1 = [{string:to_lower(Key), Value} || {Key, Value} <- Headers],
  Keys = lists:usort(lists:append([string:to_lower(Key) || {Key, _} <- Defaults],
                                  [Key || {Key, _} <- Headers1])),
  sort_headers([{Key, header_value(Key, Headers1, proplists:get_value(Key, Defaults))} || Key <- Keys]).


-spec authorization(AccessKey :: access_key(),
                    SecretAccessKey :: secret_access_key(),
                    RequestTimestamp :: string(),
                    Region :: region(),
                    Service :: string(),
                    Headers :: headers(),
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


-spec default_headers(RequestTimestamp :: string(),
                      ContentLength :: integer(),
                      PayloadHash :: string(),
                      Hostname :: host(),
                      SecurityToken :: security_token()) -> headers().
%% @doc build the base headers that are merged in with the headers for every
%%      request.
%% @end
default_headers(RequestTimestamp, ContentLength, PayloadHash, Hostname, undefined) ->
  [{"content-length", integer_to_list(ContentLength)},
   {"date", RequestTimestamp},
   {"host", Hostname},
   {"x-amz-content-sha256", PayloadHash}];
default_headers(RequestTimestamp, ContentLength, PayloadHash, Hostname, SecurityToken) ->
  [{"content-length", integer_to_list(ContentLength)},
   {"date", RequestTimestamp},
   {"host", Hostname},
   {"x-amz-content-sha256", PayloadHash},
   {"x-amz-security-token", SecurityToken}].


-spec canonical_headers(Headers :: headers()) -> string().
%% @doc Convert the headers list to a line-feed delimited string in the AWZ
%%      canonical headers format.
%% @end
canonical_headers(Headers) ->
  canonical_headers(sort_headers(Headers), []).

-spec canonical_headers(Headers :: headers(), CanonicalHeaders :: list()) -> string().
%% @doc Convert the headers list to a line-feed delimited string in the AWZ
%%      canonical headers format.
%% @end
canonical_headers([], CanonicalHeaders) ->
  lists:flatten(CanonicalHeaders);
canonical_headers([{Key, Value}|T], CanonicalHeaders) ->
  Header = string:join([string:to_lower(Key), Value], ":") ++ "\n",
  canonical_headers(T, lists:append(CanonicalHeaders, [Header])).


-spec credential_scope(RequestDate :: string(),
                       Region :: region(),
                       Service :: string()) -> string().
%% @doc Return the credential scope string used in creating the request string to sign.
%% @end
credential_scope(RequestDate, Region, Service) ->
  lists:flatten(string:join([RequestDate, Region, Service, "aws4_request"], "/")).


-spec header_value(Key :: string(),
                   Headers :: headers(),
                   Default :: string()) -> string().
%% @doc Return the the header value or the default value for the header if it
%%      is not specified.
%% @end
header_value(Key, Headers, Default) ->
  proplists:get_value(Key, Headers, proplists:get_value(string:to_lower(Key), Headers, Default)).


-spec hmac_sign(Key :: string(), Message :: string()) -> string().
%% @doc Return the SHA-256 hash for the specified value.
%% @end
hmac_sign(Key, Message) ->
  SignedValue = crypto:mac(hmac, sha256, Key, Message),
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
  rabbitmq_aws_urilib:build_query_string(lists:keysort(1, QueryArgs)).


-spec request_hash(Method :: method(),
                   Path :: path(),
                   QArgs :: query_args(),
                   Headers :: headers(),
                   Payload :: string()) -> string().
%% @doc Create the request hash value
%% @end
request_hash(Method, Path, QArgs, Headers, Payload) ->
  RawPath = case string:slice(Path, 0, 1) of
    "/" -> Path;
    _   -> "/" ++ Path
  end,
  EncodedPath = uri_string:recompose(#{path => RawPath}),
  CanonicalRequest = string:join([string:to_upper(atom_to_list(Method)),
                                  EncodedPath,
                                  query_string(QArgs),
                                  canonical_headers(Headers),
                                  signed_headers(Headers),
                                  sha256(Payload)], "\n"),
  sha256(CanonicalRequest).


-spec scope(AMZDate :: string(),
            Region :: region(),
            Service :: string()) -> string().
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


-spec signed_headers(Headers :: headers(), Values :: list()) -> string().
%% @doc Return the signed headers string of delimited header key names
%% @end
signed_headers([], SignedHeaders) -> string:join(SignedHeaders, ";");
signed_headers([{Key,_}|T], SignedHeaders) ->
  signed_headers(T, SignedHeaders ++ [string:to_lower(Key)]).


-spec signature(StringToSign :: string(),
                SigningKey :: string()) -> string().
%% @doc Create the request signature.
%% @end
signature(StringToSign, SigningKey) ->
  SignedValue = crypto:mac(hmac, sha256, SigningKey, StringToSign),
  lists:flatten(io_lib:format("~64.16.0b", [binary:decode_unsigned(SignedValue)])).


-spec signing_key(SecretKey :: secret_access_key(),
                  AMZDate :: string(),
                  Region :: region(),
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
                     Region :: region(),
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


-spec sort_headers(Headers :: headers()) -> headers().
%% @doc Case-insensitive sorting of the request headers
%% @end
sort_headers(Headers) ->
  lists:sort(fun({A,_}, {B, _}) -> string:to_lower(A) =< string:to_lower(B) end, Headers).
