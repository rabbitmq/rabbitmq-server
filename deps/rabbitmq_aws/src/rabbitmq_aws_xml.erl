%% ====================================================================
%% @author Gavin M. Roy <gavinmroy@gmail.com>
%% @copyright 2016, Gavin M. Roy
%% @doc Simple XML parser for AWS application/xml responses
%% @end
%% ====================================================================
-module(rabbitmq_aws_xml).

-export([parse/1]).

-include_lib("xmerl/include/xmerl.hrl").

-spec parse(Value :: string() | binary()) -> list().
parse(Value) ->
  {Element, _} = xmerl_scan:string(Value),
  parse_node(Element).


parse_node(#xmlElement{name=Name, content=Content}) ->
  Value = parse_content(Content, []),
  [{atom_to_list(Name), flatten_value(Value, Value)}].


flatten_text([], Value) -> Value;
flatten_text([{K,V}|T], Accum) when is_list(V) ->
    flatten_text(T, lists:append([{K, V}], Accum));
flatten_text([H | T], Accum) when is_list(H) ->
    flatten_text(T, lists:append(T, Accum)).


flatten_value([L], _) when is_list(L) -> L;
flatten_value(L, _) when is_list(L) -> flatten_text(L, []).


parse_content([], Value) -> Value;
parse_content(#xmlElement{} = Element, Accum) ->
  lists:append(parse_node(Element), Accum);
parse_content(#xmlText{value=Value}, Accum) ->
  case string:strip(Value) of
    "" -> Accum;
    "\n" -> Accum;
    Stripped ->
      lists:append([Stripped], Accum)
  end;
parse_content([H|T], Accum) ->
  parse_content(T, parse_content(H, Accum)).
