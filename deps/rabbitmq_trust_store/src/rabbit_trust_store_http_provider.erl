-module(rabbit_trust_store_http_provider).

-include_lib("public_key/include/public_key.hrl").

-behaviour(rabbit_trust_store_certificate_provider).

-export([list_certs/1, list_certs/2, load_cert/3]).

-record(http_state,{
    url :: string(),
    http_options :: list(),
    headers :: httpc:headers()
}).

list_certs(Config) ->
    init(),
    State = init_state(Config),
    list_certs(Config, State).

list_certs(_, #http_state{url = Url,
                          http_options = HttpOptions,
                          headers = Headers} = State) ->
    Res = httpc:request(get, {Url, Headers}, HttpOptions, [{body_format, binary}]),
    case Res of
        {ok, {{_, 200, _}, RespHeaders, Body}} ->
            Certs = decode_cert_list(Body),
            NewState = new_state(RespHeaders, State),
            {ok, Certs, NewState};
        {ok, {{_,304, _}, _, _}}  -> no_change;
        {ok, {{_,Code,_}, _, Body}} -> {error, {http_error, Code, Body}};
        {error, Reason} -> {error, Reason}
    end.

load_cert(_, Attributes, Config) ->
    CertPath = proplists:get_value(path, Attributes),
    #http_state{url = BaseUrl,
                http_options = HttpOptions,
                headers = Headers} = init_state(Config),
    Url = join_url(BaseUrl, CertPath),
    Res = httpc:request(get,
                        {Url, Headers},
                        HttpOptions,
                        [{body_format, binary}, {full_result, false}]),
    case Res of
        {ok, {200, Body}} ->
            [{'Certificate', Cert, not_encrypted}] = public_key:pem_decode(Body),
            {ok, Cert};
        {ok, {Code, Body}} -> {error, {http_error, Code, Body}};
        {error, Reason}    -> {error, Reason}
    end.

join_url(BaseUrl, CertPath)  ->
    string:strip(rabbit_data_coercion:to_list(BaseUrl), right, $/)
    ++ "/" ++
    string:strip(rabbit_data_coercion:to_list(CertPath), left, $/).

init() ->
    inets:start(),
    ssl:start().

init_state(Config) ->
    Url = proplists:get_value(url, Config),
    Headers = proplists:get_value(http_headers, Config, []),
    HttpOptions = case proplists:get_value(ssl_options, Config) of
        undefined -> [];
        SslOpts   -> [{ssl, SslOpts}]
    end,
    #http_state{url = Url, http_options = HttpOptions, headers = Headers}.

decode_cert_list(Body) ->
    {ok, Struct} = rabbit_misc:json_decode(Body),
    [{<<"certificates">>, Certs}] = rabbit_misc:json_to_term(Struct),
    lists:map(
        fun(Cert) ->
            Path = proplists:get_value(<<"path">>, Cert),
            CertId = proplists:get_value(<<"id">>, Cert),
            {CertId, [{path, Path}]}
        end,
        Certs).

new_state(RespHeaders, #http_state{headers = Headers} = State) ->
    LastModified = proplists:get_value("Last-Modified",
                                       RespHeaders,
                                       proplists:get_value("last-modified",
                                                           RespHeaders,
                                                           undefined)),
    case LastModified of
        undefined -> State;
        Value     ->
            NewHeaders = lists:ukeymerge(1, Headers,
                                            [{"If-Modified-Since", Value}]),
            State#http_state{headers = NewHeaders}
    end.







