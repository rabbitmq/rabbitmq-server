-module(uaa_mock).

-export([
         init/3
        ,rest_init/2
        ,allowed_methods/2
        ,is_authorized/2
        ]).

-export([
         content_types_accepted/2
        ]).

-export([
         process_post/2
        ]).

-export([register_context/0]).

-define(TOKEN, <<"valid_token">>).
-define(CLIENT, <<"client">>).
-define(SECRET, <<"secret">>).


register_context() ->
    rabbit_web_dispatch:register_context_handler(
      rabbit_test_uaa, [{port, 5678}], "", 
      cowboy_router:compile([{'_', [{"/uaa/check_token", uaa_mock, []}]}]), 
      "UAA mock").

init(_Transport, _Req, _Opts) ->
    %% Compile the DTL template used for the authentication
    %% form in the implicit grant flow.
    {upgrade, protocol, cowboy_rest}.

rest_init(Req, _Opts) ->
    {ok, Req, undefined_state}.

is_authorized(Req, State) ->
    case cowboy_req:parse_header(<<"authorization">>, Req) of
        {ok, {<<"basic">>, {Username, Password}}, _} ->
            case {Username, Password} of
                {?CLIENT, ?SECRET} -> {true, Req, State};
                _                  -> {{false, <<>>}, Req, State}
            end;
        _ ->
            {{false, <<>>}, Req, State}
    end.

content_types_accepted(Req, State) ->
    {[{{<<"application">>, <<"x-www-form-urlencoded">>, []}, process_post}],
     Req, State}.

allowed_methods(Req, State) ->
    {[<<"POST">>], Req, State}.

process_post(Req, State) ->
    {ok, Params, _Req2} = cowboy_req:body_qs(Req),
    Token = proplists:get_value(<<"token">>, Params),
    {ok, Reply} = case Token of
                      ?TOKEN -> 
                          cowboy_req:reply(200, 
                                           [{<<"content-type">>, 
                                             <<"application/json">>}], 
                                           response(), 
                                           Req);
                      _      -> 
                          cowboy_req:reply(400, 
                                           [{<<"content-type">>, 
                                             <<"application/json">>}], 
                                            <<"{\"error\":\"invalid_token\"}">>, 
                                            Req)
                  end,
    {halt, Reply, State}.

response() ->
    mochijson2:encode([
                       {<<"foo">>, <<"bar">>},
                       {<<"aud">>, [<<"rabbitmq">>]},
                       {<<"scope">>, [<<"rabbitmq.vhost_q_configure_foo">>, 
                                      <<"rabbitmq.vhost_ex_write_foo">>, 
                                      <<"rabbitmq.vhost_t_read_foo">>,
                                      <<"rabbitmq.vhost_custom_read_bar">>]}
                      ]).
