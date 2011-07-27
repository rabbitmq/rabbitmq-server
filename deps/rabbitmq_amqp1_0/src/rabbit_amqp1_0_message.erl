-module(rabbit_amqp1_0_message).

-export([assemble/1, annotated_message/1]).

-include_lib("amqp_client/include/amqp_client.hrl").
-include("rabbit_amqp1_0.hrl").

assemble(Msg) ->
    {Props, Content} = assemble(header, {#'P_basic'{}, undefined}, Msg),
    #amqp_msg{props = Props, payload = Content}.

%% TODO handle delivery-annotations, message-annotations and
%% application-properties

assemble(header, {P, C}, [H = #'v1_0.header'{} | Rest]) ->
    assemble(properties, {parse_header(H, P), C}, Rest);
assemble(header, {P, C}, Rest) ->
    assemble(properties, {P, C}, Rest);

assemble(properties, {P, C}, [X = #'v1_0.properties'{} | Rest]) ->
    assemble(properties, {parse_properties(X, P), C}, Rest);
assemble(properties, {P, C}, Rest) ->
    assemble(data, {P, C}, Rest);

assemble(data, {P, _C}, [#'v1_0.data'{content = Content} | Rest]) ->
    assemble(footer, {P, Content}, Rest);
assemble(data, {P, C}, Rest) ->
    assemble(footer, {P, C}, Rest);

assemble(footer, {P, C}, [#'v1_0.footer'{}]) ->
    %% TODO parse FOOTER
    {P, C};
assemble(footer, {P, C}, []) ->
    {P, C};
assemble(footer, _, [Left | _]) ->
    exit({unexpected_trailing_sections, Left});

assemble(Expected, {_, _}, Actual) ->
    exit({expected_section, Expected, Actual}).

parse_header(Header, Props) ->
    Props#'P_basic'{headers          = undefined, %% TODO
                    delivery_mode    = case Header#'v1_0.header'.durable of
                                           true -> 2;
                                           _    -> 1
                                       end,
                    priority         = Header#'v1_0.header'.priority,
                    expiration       = Header#'v1_0.header'.ttl,
                    type             = undefined, %% TODO
                    app_id           = undefined, %% TODO
                    cluster_id       = undefined}. %% TODO

parse_properties(Props10, Props) ->
    Props#'P_basic'{content_type     = Props10#'v1_0.properties'.content_type,
                    content_encoding = undefined, %% TODO parse from 1.0 version
                    correlation_id   = Props10#'v1_0.properties'.correlation_id,
                    reply_to         = Props10#'v1_0.properties'.reply_to,
                    message_id       = Props10#'v1_0.properties'.message_id,
                    user_id          = Props10#'v1_0.properties'.user_id}.

%%--------------------------------------------------------------------

%% TODO create delivery-annotations, message-annotations and
%% application-properties if we feel like it.

annotated_message(#amqp_msg{props = Props, payload = Content}) ->
    Header = #'v1_0.header'
      {durable           = case Props#'P_basic'.delivery_mode of
                               2 -> true;
                               _ -> false
                           end,
       priority          = Props#'P_basic'.priority,
       ttl               = Props#'P_basic'.expiration,
       first_acquirer    = undefined, %% TODO
       delivery_count    = undefined}, %% TODO
    Props10 = #'v1_0.properties'{
      message_id     = Props#'P_basic'.message_id,
      user_id        = Props#'P_basic'.user_id,
      to             = undefined, %% TODO
      subject        = undefined, %% TODO
      reply_to       = Props#'P_basic'.reply_to,
      correlation_id = Props#'P_basic'.correlation_id,
      content_type   = Props#'P_basic'.content_type}, %% TODO encode to 1.0 ver
    [Header, Props10, #'v1_0.data'{content = Content}, #'v1_0.footer'{}].
