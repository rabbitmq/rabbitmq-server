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
  [{list_to_binary(atom_to_list(Name)), parse_content(Content, [])}].

parse_content([], Value) -> Value;
parse_content(#xmlElement{} = Element, Accum) ->
  lists:append(parse_node(Element), Accum);
parse_content(#xmlText{value=Value}, _) ->
  list_to_binary(Value);
parse_content([H|T], Accum) ->
  parse_content(T, parse_content(H, Accum)).
