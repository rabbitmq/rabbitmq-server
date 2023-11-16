-module(mc_util).

-include("mc.hrl").

-export([is_valid_shortstr/1,
         is_utf8_no_null/1,
         uuid_to_urn_string/1,
         urn_string_to_uuid/1,
         infer_type/1,
         utf8_string_is_ascii/1,
         amqp_map_get/3,
         is_x_header/1
        ]).

-spec is_valid_shortstr(term()) -> boolean().
is_valid_shortstr(Bin) when ?IS_SHORTSTR_LEN(Bin) ->
    is_utf8_no_null(Bin);
is_valid_shortstr(_) ->
    false.

-spec is_utf8_no_null(term()) -> boolean().
is_utf8_no_null(Term) ->
    utf8_scan(Term, fun (C) -> C > 0 end).

-spec uuid_to_urn_string(binary()) -> binary().
uuid_to_urn_string(<<TL:4/binary, TM:2/binary, THV:2/binary,
                     CSR:1/binary, CSL:1/binary, N:6/binary>>) ->
    Delim = <<"-">>,
    iolist_to_binary(
      [<<"urn:uuid:">>,
       binary:encode_hex(TL, lowercase), Delim,
       binary:encode_hex(TM, lowercase), Delim,
       binary:encode_hex(THV, lowercase), Delim,
       binary:encode_hex(CSR, lowercase),
       binary:encode_hex(CSL, lowercase), Delim,
       binary:encode_hex(N, lowercase)]).

-spec urn_string_to_uuid(binary()) ->
    {ok, binary()} | {error, not_urn_string}.
urn_string_to_uuid(<<"urn:uuid:", UuidStr:36/binary>>) ->
    Parts = binary:split(UuidStr, <<"-">>, [global]),
    {ok, iolist_to_binary([binary:decode_hex(Part) || Part <- Parts])};
urn_string_to_uuid(_) ->
    {error, not_urn_string}.


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

utf8_string_is_ascii(UTF8String) ->
    utf8_scan(UTF8String, fun(Char) -> Char >= 0 andalso Char < 128 end).

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

%% INTERNAL

utf8_scan(<<>>, _Pred) ->
    true;
utf8_scan(<<C/utf8, Rem/binary>>, Pred) ->
    case Pred(C) of
        true ->
            utf8_scan(Rem, Pred);
        false ->
            false
    end;
utf8_scan(_, _Pred) ->
    false.
