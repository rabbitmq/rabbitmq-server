%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2014 GoPivotal, Inc.  All rights reserved.
%%

-module(rabbit_trunc_term_tests).

-ifdef(use_proper_qc).

-export([prop_trunc_any_term/0]).
-include_lib("proper/include/proper.hrl").

prop_trunc_any_term() ->
    ?FORALL({GenAny, MaxSz}, {gen_top_level(5000), gen_max_len()},
            case size_of_thing(GenAny) >= MaxSz of
                false -> io:format("Skipping ~p~n", [{GenAny, MaxSz}]),
                         true;
                true  -> SzInitial = erts_debug:size(GenAny),
                         try
                             Shrunk  = rabbit_trunc_term:shrink_term(GenAny,
                                                                     MaxSz),
                             SzShrunk = erts_debug:size(Shrunk),
                             ?WHENFAIL(begin
                                           io:format("Input: ~p\n", [GenAny]),
                                           io:format("MaxLen: ~p\n", [MaxSz]),
                                           io:format("Input-Size: ~p\n",
                                                     [SzInitial]),
                                           io:format("Shrunk-Size: ~p\n",
                                                     [SzShrunk])
                                       end,
                                       true = SzShrunk < SzInitial)
                         catch
                             _:Err ->
                                 io:format("\nException: ~p\n", [Err]),
                                 io:format("Input: ~p\n", [GenAny]),
                                 io:format("Max-Size: ~p\n", [MaxSz]),
                                 false
                         end
            end).

size_of_thing(Thing) when is_binary(Thing)    -> size(Thing);
size_of_thing(Thing) when is_bitstring(Thing) -> byte_size(Thing);
size_of_thing(Thing) when is_list(Thing)      -> length(Thing);
size_of_thing(Thing) when is_tuple(Thing)     -> size(Thing);
size_of_thing(Thing)                          -> error({cannot_size, Thing}).

%% Generates a printable string
gen_print_str() ->
    ?LET(Xs, list(char()), [X || X <- Xs, io_lib:printable_list([X]), X /= $~, X < 255]).

gen_print_bin() ->
    ?LET(Xs, gen_print_str(), list_to_binary(Xs)).

gen_top_level(MaxDepth) ->
    oneof([gen_print_str(),
           gen_iolist(1000),
           binary(),
           gen_bitstring(),
           gen_print_bin()] ++
           [?LAZY(list(gen_any(MaxDepth - 1))) || MaxDepth /= 0] ++
           [?LAZY(gen_tuple(gen_any(MaxDepth - 1))) || MaxDepth /= 0]).

gen_any(MaxDepth) ->
    oneof([largeint(),
           gen_atom(),
           gen_quoted_atom(),
           nat(),
           %real(),
           gen_print_str(),
           gen_iolist(1000),
           binary(),
           gen_bitstring(),
           gen_print_bin(),
           gen_pid(),
           gen_port(),
           gen_ref(),
           gen_fun()] ++
           [?LAZY(list(gen_any(MaxDepth - 1))) || MaxDepth > 1] ++
           [?LAZY(gen_tuple(gen_any(MaxDepth - 1))) || MaxDepth > 1]).

gen_iolist(0) ->
    [];
gen_iolist(Depth) ->
    list(oneof([gen_char(),
                gen_print_str(),
                gen_print_bin(),
                gen_iolist(Depth-1)])).

gen_atom() ->
    elements([abc, def, ghi]).

gen_quoted_atom() ->
    elements(['noname@nonode', '@localhost', '10gen']).

gen_bitstring() ->
    ?LET(XS, binary(), <<XS/binary, 1:7>>).

gen_tuple(Gen) ->
    ?LET(Xs, list(Gen), list_to_tuple(Xs)).

gen_max_len() -> %% 3..N
    ?LET(Xs, int(), 3 + abs(Xs)).

gen_pid() ->
    ?LAZY(spawn(fun() -> ok end)).

gen_port() ->
    ?LAZY(begin
              Port = erlang:open_port({spawn, "true"}, []),
              catch(erlang:port_close(Port)),
              Port
          end).

gen_ref() ->
    ?LAZY(make_ref()).

gen_fun() ->
    ?LAZY(fun() -> ok end).

gen_char() ->
    oneof(lists:seq($A, $z)).

-else.

-export([prop_disabled/0]).

prop_disabled() ->
    exit({compiled_without_proper,
          "PropEr was not present during compilation of the test module. "
          "Hence all tests are disabled."}).

-endif.

