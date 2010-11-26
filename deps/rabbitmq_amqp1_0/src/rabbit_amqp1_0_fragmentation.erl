-module(rabbit_amqp1_0_fragmentation).

-export([assemble/1, fragments/1]).

-include_lib("amqp_client/include/amqp_client.hrl").
-include("rabbit_amqp1_0.hrl").

-define(SECTION_HEADER,     {uint, 0}).
-define(SECTION_PROPERTIES, {uint, 1}).
-define(SECTION_FOOTER,     {uint, 2}).
-define(SECTION_DATA,       {uint, 3}).
-define(SECTION_AMQP_DATA,  {uint, 4}).
-define(SECTION_AMQP_MAP,   {uint, 5}).
-define(SECTION_AMQP_LIST,  {uint, 6}).

%% TODO: we don't care about fragment_offset while reading. Should we?
assemble(Fragments) ->
    %%io:format("Fragments: ~p~n", [Fragments]),
    {Props, Content} = assemble(?SECTION_HEADER, {#'P_basic'{}, undefined},
                                Fragments),
    %%io:format("Props: ~p~n", [Props]),
    %%io:format("Content: ~p~n", [Content]),
    #amqp_msg{props = Props, payload = Content}.

assemble(?SECTION_HEADER, {PropsIn, ContentIn},
         [#'v1_0.fragment'{first = true, last = true,
                           payload = {binary, Payload},
                           format_code = ?SECTION_HEADER} | Fragments]) ->
    assemble(?SECTION_PROPERTIES, {parse_header(Payload, PropsIn), ContentIn},
             Fragments);

assemble(?SECTION_PROPERTIES, {PropsIn, ContentIn},
         [#'v1_0.fragment'{first = true, last = true,
                           payload = {binary, Payload},
                           format_code = ?SECTION_PROPERTIES} | Fragments]) ->
    %% TODO allow for AMQP_DATA, _MAP, _LIST
    assemble(?SECTION_DATA, {parse_properties(Payload, PropsIn), ContentIn},
             Fragments);

assemble(?SECTION_DATA, {PropsIn, ContentIn},
         [#'v1_0.fragment'{first = true, last = true,
                           payload = {binary, Payload},
                           format_code = ?SECTION_DATA} | Fragments]) ->
    %% TODO allow for multiple fragments
    assemble(?SECTION_FOOTER, {PropsIn, Payload}, Fragments);

assemble(?SECTION_FOOTER, {PropsIn, ContentIn},
         [#'v1_0.fragment'{first = true, last = true,
                           payload = {binary, Payload},
                           format_code = ?SECTION_FOOTER}]) ->
    %% TODO parse FOOTER
    {PropsIn, ContentIn};

assemble(Expected, {_, _}, Actual) ->
    exit({expected_fragment, Expected, Actual}).

parse_header(HeaderBin, Props) ->
    Parsed = parse(HeaderBin),
    Props#'P_basic'{headers          = null, %% TODO
                    delivery_mode    = case Parsed#'v1_0.header'.durable of
                                           true -> 2;
                                           _    -> 1
                                       end,
                    priority         = Parsed#'v1_0.header'.priority,
                    expiration       = Parsed#'v1_0.header'.ttl,
                    timestamp        = Parsed#'v1_0.header'.transmit_time,
                    type             = null, %% TODO
                    app_id           = null, %% TODO
                    cluster_id       = null}. %% TODO

parse_properties(PropsBin, Props) ->
    Parsed = parse(PropsBin),
    Props#'P_basic'{content_type     = Parsed#'v1_0.properties'.content_type,
                    content_encoding = null, %% TODO parse from 1.0 version
                    correlation_id   = Parsed#'v1_0.properties'.correlation_id,
                    reply_to         = Parsed#'v1_0.properties'.reply_to,
                    message_id       = Parsed#'v1_0.properties'.message_id,
                    user_id          = Parsed#'v1_0.properties'.user_id}.

parse(Bin) ->
    rabbit_amqp1_0_framing:decode(rabbit_amqp1_0_binary_parser:parse(Bin)).

%%--------------------------------------------------------------------

fragments(#amqp_msg{props = Properties, payload = Content}) ->
    {HeaderBin, PropertiesBin} = enc_properties(Properties),
    FooterBin = enc(#'v1_0.footer'{}), %% TODO
    [fragment(?SECTION_HEADER,     HeaderBin),
     fragment(?SECTION_PROPERTIES, PropertiesBin),
     fragment(?SECTION_DATA,       Content),
     fragment(?SECTION_FOOTER,     FooterBin)].

fragment(Code, Content) ->
    #'v1_0.fragment'{first = true,
                     last = true,
                     format_code = Code,
                     fragment_offset = {ulong, 0}, %% TODO definitely wrong
                     payload = {binary, Content}}.

enc_properties(Props) ->
    Header = #'v1_0.header'
      {durable           = case Props#'P_basic'.delivery_mode of
                               1 -> false;
                               2 -> true
                           end,
       priority          = Props#'P_basic'.priority,
       transmit_time     = Props#'P_basic'.timestamp,
       ttl               = Props#'P_basic'.expiration,
       former_acquirers  = null, %% TODO
       delivery_failures = null, %% TODO
       format_code       = null, %% TODO
       message_attrs     = null, %% TODO
       delivery_attrs    = null}, %% TODO
    Properties = #'v1_0.properties'{
      message_id     = Props#'P_basic'.message_id,
      user_id        = Props#'P_basic'.user_id,
      to             = null, %% TODO
      subject        = null, %% TODO
      reply_to       = Props#'P_basic'.reply_to,
      correlation_id = Props#'P_basic'.correlation_id,
      content_length = null,
      content_type   = Props#'P_basic'.content_type}, %% TODO encode to 1.0 ver
    {enc(Header), enc(Properties)}.

enc(Rec) ->
    iolist_to_binary(rabbit_amqp1_0_binary_generator:generate(
                       rabbit_amqp1_0_framing:encode(Rec))).
