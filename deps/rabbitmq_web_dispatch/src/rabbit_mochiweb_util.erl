%%   The contents of this file are subject to the Mozilla Public License
%%   Version 1.1 (the "License"); you may not use this file except in
%%   compliance with the License. You may obtain a copy of the License at
%%   http://www.mozilla.org/MPL/
%%
%%   Software distributed under the License is distributed on an "AS IS"
%%   basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%%   License for the specific language governing rights and limitations
%%   under the License.
%%
%%   The Original Code is RabbitMQ Mochiweb Embedding.
%%
%%   The Initial Developers of the Original Code are Rabbit Technologies Ltd.
%%
%%   Copyright (C) 2010 Rabbit Technologies Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%

-module(rabbit_mochiweb_util).

-export([parse_auth_header/1]).
-export([relativise/2]).

parse_auth_header(Header) ->
    case Header of
        "Basic " ++ Base64 ->
            Str = base64:mime_decode_to_string(Base64),
            Tokens = [list_to_binary(S) || S <- string:tokens(Str, ":")],
            case length(Tokens) of
                2 -> Tokens;
                _ -> invalid
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
