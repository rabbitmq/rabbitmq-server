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
    {Props, Content} = assemble(?SECTION_HEADER, {undefined, undefined},
                                Fragments),
    %%io:format("Payload= ~p~n", [Content]),
    %%io:format("Props= ~p~n", [Props]),
    #amqp_msg{props = Props, payload = Content}.

assemble(?SECTION_HEADER, {PropsIn, ContentIn},
         [#'v1_0.fragment'{first = true, last = true,
                           payload = {binary, Payload},
                           format_code = ?SECTION_HEADER} | Fragments]) ->
    %% TODO parse HEADER
    assemble(?SECTION_PROPERTIES, {PropsIn, ContentIn}, Fragments);

assemble(?SECTION_PROPERTIES, {PropsIn, ContentIn},
         [#'v1_0.fragment'{first = true, last = true,
                           payload = {binary, Payload},
                           format_code = ?SECTION_PROPERTIES} | Fragments]) ->
    %% TODO parse PROPERTIES
    %% TODO allow for AMQP_DATA, _MAP, _LIST
    assemble(?SECTION_DATA, {#'P_basic'{}, ContentIn}, Fragments);

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


fragments(#amqp_msg{props = Properties, payload = Content}) ->
    {list, [fragment(?SECTION_HEADER, <<"">>),
            fragment(?SECTION_PROPERTIES, <<"">>),
            fragment(?SECTION_DATA, Content),
            fragment(?SECTION_FOOTER, <<"">>)]}.

fragment(Code, Content) ->
    #'v1_0.fragment'{first = true,
                     last = true,
                     format_code = Code,
                     fragment_offset = {ulong, 0}, %% TODO definitely wrong
                     payload = {binary, Content}}.
