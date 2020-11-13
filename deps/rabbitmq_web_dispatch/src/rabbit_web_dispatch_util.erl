%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at https://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2017 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_web_dispatch_util).

-export([parse_auth_header/1]).
-export([relativise/2, unrelativise/2]).

%% @todo remove
parse_auth_header(Header) ->
    case Header of
        "Basic " ++ Base64 ->
            Str = base64:mime_decode_to_string(Base64),
            case string:chr(Str, $:) of
                0 -> invalid;
                N -> [list_to_binary(string:sub_string(Str, 1, N - 1)),
                      list_to_binary(string:sub_string(Str, N + 1))]
            end;
         _ ->
            invalid
    end.

relativise("/" ++ F, "/" ++ T) ->
    From = string:tokens(F, "/"),
    To = string:tokens(T, "/"),
    string:join(relativise0(From, To), "/").

relativise0([H], [H|_] = To) ->
    To;
relativise0([H|From], [H|To]) ->
    relativise0(From, To);
relativise0(From, []) ->
    lists:duplicate(length(From), "..");
relativise0([_|From], To) ->
    lists:duplicate(length(From), "..") ++ To;
relativise0([], To) ->
    To.

unrelativise(_, "/"   ++ T) -> "/" ++ T;
unrelativise(F, "./"  ++ T) -> unrelativise(F, T);
unrelativise(F, "../" ++ T) -> unrelativise(strip_tail(F), T);
unrelativise(F, T)          -> case string:str(F, "/") of
                                   0 -> T;
                                   _ -> strip_tail(F) ++ "/" ++ T
                               end.

strip_tail("") -> exit(not_enough_to_strip);
strip_tail(S)  -> case string:rstr(S, "/") of
                      0 -> "";
                      I -> string:left(S, I - 1)
                  end.
