-module(rabbit_amqp1_0_message).

%%-export([assemble/1, fragments/1]).

-include_lib("amqp_client/include/amqp_client.hrl").
-include("rabbit_amqp1_0.hrl").

%% %% TODO: we don't care about fragment_offset while reading. Should we?
%% assemble(Fragments) ->
%%     {Props, Content} = assemble(?V_1_0_HEADER, {#'P_basic'{}, undefined},
%%                                 Fragments),
%%     #amqp_msg{props = Props, payload = Content}.

%% assemble(?V_1_0_HEADER, {PropsIn, ContentIn},
%%          [#'v1_0.fragment'{first = true, last = true,
%%                            payload = {binary, Payload},
%%                            format_code = ?V_1_0_HEADER} | Fragments]) ->
%%     assemble(?V_1_0_PROPERTIES, {parse_header(Payload, PropsIn), ContentIn},
%%              Fragments);
%% assemble(?V_1_0_HEADER, State, Fragments) ->
%%     assemble(?V_1_0_PROPERTIES, State, Fragments);

%% assemble(?V_1_0_PROPERTIES, {PropsIn, ContentIn},
%%          [#'v1_0.fragment'{first = true, last = true,
%%                            payload = {binary, Payload},
%%                            format_code = ?V_1_0_PROPERTIES} | Fragments]) ->
%%     %% TODO allow for AMQP_DATA, _MAP, _LIST
%%     assemble(?V_1_0_DATA, {parse_properties(Payload, PropsIn), ContentIn},
%%              Fragments);
%% assemble(?V_1_0_PROPERTIES, State, Fragments) ->
%%     assemble(?V_1_0_DATA, State, Fragments);

%% assemble(?V_1_0_DATA, {PropsIn, _ContentIn},
%%          [#'v1_0.fragment'{first = true, last = true,
%%                            payload = {binary, Payload},
%%                            format_code = ?V_1_0_DATA} | Fragments]) ->
%%     %% TODO allow for multiple fragments
%%     assemble(?V_1_0_FOOTER, {PropsIn, Payload}, Fragments);
%% assemble(?V_1_0_DATA, State, Fragments) ->
%%     assemble(?V_1_0_FOOTER, State, Fragments);

%% assemble(?V_1_0_FOOTER, {PropsIn, ContentIn},
%%          [#'v1_0.fragment'{first = true, last = true,
%%                            payload = {binary, _Payload},
%%                            format_code = ?V_1_0_FOOTER}]) ->
%%     %% TODO parse FOOTER
%%     {PropsIn, ContentIn};
%% assemble(?V_1_0_FOOTER, State, []) ->
%%     State;
%% assemble(?V_1_0_FOOTER, _State, [Left | _]) ->
%%     exit({unexpected_trailing_fragments, Left});

%% assemble(Expected, {_, _}, Actual) ->
%%     exit({expected_fragment, Expected, Actual}).

parse_header(HeaderBin, Props) ->
    Parsed = parse(HeaderBin),
    Props#'P_basic'{headers          = undefined, %% TODO
                    delivery_mode    = case Parsed#'v1_0.header'.durable of
                                           true -> 2;
                                           _    -> 1
                                       end,
                    priority         = Parsed#'v1_0.header'.priority,
                    expiration       = Parsed#'v1_0.header'.ttl,
                    type             = undefined, %% TODO
                    app_id           = undefined, %% TODO
                    cluster_id       = undefined}. %% TODO

parse_properties(PropsBin, Props) ->
    Parsed = parse(PropsBin),
    Props#'P_basic'{content_type     = Parsed#'v1_0.properties'.content_type,
                    content_encoding = undefined, %% TODO parse from 1.0 version
                    correlation_id   = Parsed#'v1_0.properties'.correlation_id,
                    reply_to         = Parsed#'v1_0.properties'.reply_to,
                    message_id       = Parsed#'v1_0.properties'.message_id,
                    user_id          = Parsed#'v1_0.properties'.user_id}.

parse(Bin) ->
    rabbit_amqp1_0_framing:decode(rabbit_amqp1_0_binary_parser:parse(Bin)).

%%--------------------------------------------------------------------

%% fragments(#amqp_msg{props = Properties, payload = Content}) ->
%%     {HeaderBin, PropertiesBin} = enc_properties(Properties),
%%     FooterBin = enc(#'v1_0.footer'{}), %% TODO
%%     [fragment(?V_1_0_HEADER,     HeaderBin),
%%      fragment(?V_1_0_PROPERTIES, PropertiesBin),
%%      fragment(?V_1_0_DATA,       Content),
%%      fragment(?V_1_0_FOOTER,     FooterBin)].

%% fragment(Code, Content) ->
%%     #'v1_0.fragment'{first = true,
%%                      last = true,
%%                      format_code = Code,
%%                      %% TODO DUBIOUS this is definitely wrong but I don't see
%%                      %% the point
%%                      section_offset = {ulong, 0},
%%                      section_number = {uint, 0},
%%                      payload = {binary, Content}}.

enc_properties(Props) ->
    Header = #'v1_0.header'
      {durable           = case Props#'P_basic'.delivery_mode of
                               2 -> true;
                               _ -> false
                           end,
       priority          = Props#'P_basic'.priority,
       ttl               = Props#'P_basic'.expiration,
       first_acquirer    = undefined, %% TODO
       delivery_count    = undefined}, %% TODO
    Properties = #'v1_0.properties'{
      message_id     = Props#'P_basic'.message_id,
      user_id        = Props#'P_basic'.user_id,
      to             = undefined, %% TODO
      subject        = undefined, %% TODO
      reply_to       = Props#'P_basic'.reply_to,
      correlation_id = Props#'P_basic'.correlation_id,
      content_type   = Props#'P_basic'.content_type}, %% TODO encode to 1.0 ver
    {enc(Header), enc(Properties)}.

enc(Rec) ->
    iolist_to_binary(rabbit_amqp1_0_binary_generator:generate(
                       rabbit_amqp1_0_framing:encode(Rec))).
