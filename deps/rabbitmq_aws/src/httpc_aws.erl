%% ====================================================================
%% @author Gavin M. Roy <gavinmroy@gmail.com>
%% @copyright 2016, Gavin M. Roy
%% @doc httpc_aws client library
%% @end
%% ====================================================================
-module(httpc_aws).

-behavior(gen_server).

%% API exports
-export([request/4, request/5, request/6, request/7,
         get_credentials/0,
         set_credentials/2]).

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

-include("httpc_aws.hrl").

%%====================================================================
%% exported wrapper functions
%%====================================================================

-spec get_credentials() -> {ok, access_key(), secret_access_key()}.
get_credentials() ->
  gen_server:call(httpc_aws, get_credentials).


-spec set_credentials(access_key(), secret_access_key()) -> ok.
%% @spec set_credentials(AccessKey, SecretAccessKey) -> ok
%% where
%%       AccessKey = access_key()
%%       SecretAccessKey = secret_access_key()
%% @doc Manually set the access credentials for requests. This should
%%      be used in cases where the client application wants to control
%%      the credentials instead of automatically discovering them from
%%      configuration or the AWS Instance Metadata service.
%% @end
set_credentials(AccessKey, SecretAccessKey) ->
  gen_server:call(httpc_aws, {set_credentials, AccessKey, SecretAccessKey}).

request(Service, Method, Headers, Path) ->
  gen_server:call(httpc_aws, {request, Service, Method,Headers, Path, "", [], undefined}).

request(Service, Method, Headers, Path, Body) ->
  gen_server:call(httpc_aws, {request, Service, Method, Headers, Path, Body, [], undefined}).

request(Service, Method, Headers, Path, Body, HTTPOptions) ->
  gen_server:call(httpc_aws, {request, Service, Method, Headers, Path, Body, HTTPOptions, undefined}).

request(Service, Method, Headers, Path, Body, HTTPOptions, Endpoint) ->
  gen_server:call(httpc_aws, {request, Service, Method, Headers, Path, Body, HTTPOptions, Endpoint}).


%%====================================================================
%% gen_server functions
%%====================================================================

start_link() ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec init(list()) -> {ok, state()}.
init([]) ->
  {ok, Region} = httpc_aws_config:region(),
  case httpc_aws_config:credentials() of
    {ok, AccessKey, SecretAccessKey, Expiration, SecurityToken} ->
      {ok, #state{region = Region,
                  access_key = AccessKey,
                  secret_access_key = SecretAccessKey,
                  expiration = Expiration,
                  security_token = SecurityToken}};
    {error, Reason} ->
      error_logger:error_msg("Failed to retrieve AWS credentials: ~p~n", [Reason]),
      {ok, #state{region = Region, error = Reason}}
  end.

terminate(_, _) ->
  ok.

code_change(_, _, State) ->
  {ok, State}.

handle_call({request, Service, Method, Headers, Path, Body, HTTPOptions, Endpoint}, _From, State) ->
  URI = case Endpoint of
          undefined -> lists:flatten(["https://", endpoint(State#state.region, Service), Path]);
          Authority -> lists:flatten(["https://", Authority, Path])
  end,
  SignedHeaders = httpc_aws_sign:headers(#v4request{access_key = State#state.access_key,
                                                    secret_access_key = State#state.secret_access_key,
                                                    security_token = State#state.security_token,
                                                    region = State#state.region,
                                                    service = Service,
                                                    method = Method,
                                                    uri = URI,
                                                    headers = Headers,
                                                    body = Body}),
  Response = case Body of
    "" ->
      format_response(httpc:request(Method, {URI, SignedHeaders}, HTTPOptions, []));
    _ ->
      ContentType = proplists:get_value("Content-Type", Headers),
      format_response(httpc:request(Method, {URI, SignedHeaders, ContentType, Body}, HTTPOptions, []))
  end,
  {reply, Response, State};

%% @spec handle_call({set_credentials, AccessKey, SecretAccessKey}, From, State) -> Response
%% where
%%       AccessKey = access_key()
%%       SecretAccessKey = secret_access_key()
%%       From = pid()
%%       State = state()
%%       Response = {reply, ok, State}
%% @doc Manually set the access credentials for requests. This should
%%      be used in cases where the client application wants to control
%%      the credentials instead of automatically discovering them from
%%      configuration or the AWS Instance Metadata service.
%% @end
handle_call({set_credentials, AccessKey, SecretAccessKey}, _, State) ->
  {reply, ok, State#state{access_key = AccessKey,
                          secret_access_key = SecretAccessKey}};

handle_call(get_credentials, _, State) ->
  {reply, {ok, State#state.access_key, State#state.secret_access_key}, State};

handle_call(_Request, _From, State) ->
  {noreply, State}.

handle_cast(_Request, State) ->
  {noreply, State}.

handle_info(_Info, State) ->
  {noreply, State}.

%%====================================================================
%% Internal functions
%%====================================================================

endpoint(Region, Service) ->
  lists:flatten(string:join([Service, Region, "amazonaws.com"], ".")).

format_response({ok, {{_Version, 200, _Message}, Headers, Body}}) ->
  ContentType = proplists:get_value("content-type", Headers, undefined),
  {ok, {Headers, maybe_decode_body(ContentType, Body)}};

format_response({ok, {{_Version, StatusCode, Message}, Headers, Body}}) when StatusCode >= 400 ->
  ContentType = proplists:get_value("content-type", Headers, undefined),
  {error, Message, {Headers, maybe_decode_body(ContentType, Body)}}.

maybe_decode_body("application/x-amz-json-1.0", Body) ->
  jsx:decode(list_to_binary(Body));

maybe_decode_body("application/xml", Body) ->
  httpc_aws_xml:parse(Body);

maybe_decode_body(_, Body) -> Body.
