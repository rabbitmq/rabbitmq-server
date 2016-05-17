%% ====================================================================
%% @author Gavin M. Roy <gavinmroy@gmail.com>
%% @copyright 2016, Gavin M. Roy
%% @doc Simple XML parser for AWS application/xml responses
%% @end
%% ====================================================================
-module(httpc_aws_xml).

-export([parse/1]).

-include_lib("xmerl/include/xmerl.hrl").

parse(Value) ->
  {Element, _} = xmerl_scan:string(Value),
  parse_node(Element).


parse_node(#xmlElement{name=Name, content=Content}) ->
  Value = parse_content(Content, []),
  [{atom_to_list(Name), flatten_value(Value, Value)}].


flatten_text([], Value) -> Value;
flatten_text([{K,V}|T], Accum) when is_list(V) ->
  New = case length(V) of
          1 -> lists:append([{K, lists:nth(1, V)}], Accum);
          _ -> lists:append([{K, V}], Accum)
        end,
  flatten_text(T, New);
flatten_text([H|T], Accum) ->
  flatten_text(T, lists:append([H], Accum)).


flatten_value([L], _) when is_list(L) -> L;
flatten_value([L|_], Content) when is_list(L) -> Content;
flatten_value(L, _) when is_list(L) -> flatten_text(L, []);
flatten_value(_, Content) -> Content.


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
