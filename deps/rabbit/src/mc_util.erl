-module(mc_util).

-export([is_valid_shortstr/1,
         is_utf8_no_null/1,
         uuid_to_string/1,
         infer_type/1,
         utf8_string_is_ascii/1,
         amqp_map_get/3,
         is_x_header/1
        ]).

-spec is_valid_shortstr(term()) -> boolean().
is_valid_shortstr(Bin) when byte_size(Bin) < 256 ->
    is_utf8_no_null(Bin);
is_valid_shortstr(_) ->
    false.

is_utf8_no_null(<<>>) ->
    true;
is_utf8_no_null(<<0, _/binary>>) ->
    false;
is_utf8_no_null(<<_/utf8, Rem/binary>>) ->
    is_utf8_no_null(Rem);
is_utf8_no_null(_) ->
    false.

-spec uuid_to_string(binary()) -> binary().
uuid_to_string(<<TL:32, TM:16, THV:16, CSR:8, CSL:8, N:48>>) ->
    list_to_binary(
      io_lib:format(<<"urn:uuid:~8.16.0b-~4.16.0b-~4.16.0b-~2.16.0b~2.16.0b-~12.16.0b">>,
                    [TL, TM, THV, CSR, CSL, N])).


infer_type(undefined) ->
    undefined;
infer_type(V) when is_binary(V) ->
    {utf8, V};
infer_type(V) when is_integer(V) ->
    {long, V};
infer_type(V) when is_boolean(V) ->
    {boolean, V};
infer_type({T, _} = V) when is_atom(T) ->
    %% looks like a pre-tagged type
    V.

utf8_string_is_ascii(UTF8String)
  when is_binary(UTF8String) ->
    List = unicode:characters_to_list(UTF8String),
    lists:all(fun(Char) ->
                      Char >= 0 andalso
                      Char < 128
              end, List).

amqp_map_get(Key, {map, List}, Default) ->
    amqp_map_get(Key, List, Default);
amqp_map_get(Key, List, Default) when is_list(List) ->
    case lists:search(fun ({{_, K}, _}) -> K == Key end, List) of
        {value, {_K, V}} ->
            V;
        false ->
            Default
    end;
amqp_map_get(_, _, Default) ->
    Default.

-spec is_x_header(binary()) -> boolean().
is_x_header(<<"x-", _/binary>>) ->
    true;
is_x_header(_) ->
    false.
