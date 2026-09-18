%% ====================================================================
%% @author Gavin M. Roy <gavinmroy@gmail.com>
%% @copyright 2016, Gavin M. Roy
%% @doc Simple XML parser for AWS application/xml responses
%% @end
%% ====================================================================
-module(rabbitmq_aws_xml).

-export([parse/1]).

-include_lib("xmerl/include/xmerl.hrl").

-spec parse(Value :: string() | binary()) -> list() | {error, term()}.
parse(Value) when is_binary(Value) ->
    parse(binary_to_list(Value));
parse(Value) when is_list(Value) ->
    case scan(Value) of
        {ok, Element} ->
            parse_node(Element);
        {error, _} = Error ->
            Error
    end.

-spec scan(Value :: string()) -> {ok, #xmlElement{}} | {error, term()}.
%% @doc Scan a response body with entity declarations rejected and external
%%      references never resolved, converting scanner failures into an error
%%      term. xmerl resolves an external DTD reference as a local file path and
%%      exits on a malformed document, and neither is acceptable for a response
%%      body.
%% @end
scan(Value) ->
    Options = [
        {allow_entities, false},
        {fetch_fun, fun(_DTDSpec, State) -> {ok, not_fetched, State} end},
        {validation, off},
        {quiet, true}
    ],
    try xmerl_scan:string(Value, Options) of
        {Element, _Rest} -> {ok, Element}
    catch
        exit:Reason -> {error, Reason};
        error:Reason -> {error, Reason}
    end.

%% xmerl interns element names as atoms, so parsing grows the atom table. This
%% is accepted: response bodies only come from the AWS API endpoint over a TLS
%% connection whose server certificate httpc verifies, and avoiding the atoms
%% would mean replacing xmerl with xmerl_sax_parser and changing the response
%% shapes this module returns.
parse_node(#xmlElement{name = Name, content = Content}) ->
    Value = parse_content(Content, []),
    [{atom_to_list(Name), flatten_value(Value, Value)}].

flatten_text([], Value) ->
    Value;
flatten_text([{K, V} | T], Accum) when is_list(V) ->
    flatten_text(T, lists:append([{K, V}], Accum));
flatten_text([H | T], Accum) when is_list(H) ->
    flatten_text(T, lists:append(T, Accum)).

flatten_value([L], _) when is_list(L) -> L;
flatten_value(L, _) when is_list(L) -> flatten_text(L, []).

parse_content([], Value) ->
    Value;
parse_content(#xmlElement{} = Element, Accum) ->
    lists:append(parse_node(Element), Accum);
parse_content(#xmlText{value = Value}, Accum) ->
    case string:trim(Value) of
        "" -> Accum;
        "\n" -> Accum;
        Stripped -> lists:append([Stripped], Accum)
    end;
parse_content([H | T], Accum) ->
    parse_content(T, parse_content(H, Accum)).
