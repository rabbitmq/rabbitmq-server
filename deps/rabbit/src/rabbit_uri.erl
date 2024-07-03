%% Copyright (c) 2016-2024, Loïc Hoguin <essen@ninenines.eu>
%%
%% Permission to use, copy, modify, and/or distribute this software for any
%% purpose with or without fee is hereby granted, provided that the above
%% copyright notice and this permission notice appear in all copies.
%%
%% THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
%% WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
%% MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
%% ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
%% WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
%% ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
%% OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.

%% ------------------------------------------------------------------------- %%
%% This file is a partial copy of
%% https://github.com/ninenines/cowlib/blob/optimise-urldecode/src/cow_uri.erl
%% We use this copy because:
%% 1. uri_string:unquote/1 is lax: It doesn't validate that characters that are
%%    required to be percent encoded are indeed percent encoded. In RabbitMQ,
%%    we want to enforce that proper percent encoding is done by AMQP clients.
%% 2. uri_string:unquote/1 and cow_uri:urldecode/1 in cowlib v2.13.0 are both
%%    slow because they allocate a new binary for the common case where no
%%    character was percent encoded.
%% When a new cowlib version is released, we should make app rabbit depend on
%% app cowlib calling cow_uri:urldecode/1 and delete this file (rabbit_uri.erl).
%% ------------------------------------------------------------------------- %%

-module(rabbit_uri).

-export([urldecode/1]).

-define(UNHEX(H, L), (?UNHEX(H) bsl 4 bor ?UNHEX(L))).

-define(UNHEX(C),
	case C of
		$0 -> 0;
		$1 -> 1;
		$2 -> 2;
		$3 -> 3;
		$4 -> 4;
		$5 -> 5;
		$6 -> 6;
		$7 -> 7;
		$8 -> 8;
		$9 -> 9;
		$A -> 10;
		$B -> 11;
		$C -> 12;
		$D -> 13;
		$E -> 14;
		$F -> 15;
		$a -> 10;
		$b -> 11;
		$c -> 12;
		$d -> 13;
		$e -> 14;
		$f -> 15
	end
).

%% Decode a percent encoded string. (RFC3986 2.1)
%%
%% Inspiration for some of the optimisations done here come
%% from the new `json` module as it was in mid-2024.
%%
%% Possible input includes:
%%
%% * nothing encoded (no % character):
%%   We want to return the binary as-is to avoid an allocation.
%%
%% * small number of encoded characters:
%%   We can "skip" words of text.
%%
%% * mostly encoded characters (non-ascii languages)
%%   We can decode characters in bulk.

-define(IS_PLAIN(C), (
	(C =:= $!) orelse (C =:= $$) orelse (C =:= $&) orelse (C =:= $') orelse
	(C =:= $() orelse (C =:= $)) orelse (C =:= $*) orelse (C =:= $+) orelse
	(C =:= $,) orelse (C =:= $-) orelse (C =:= $.) orelse (C =:= $0) orelse
	(C =:= $1) orelse (C =:= $2) orelse (C =:= $3) orelse (C =:= $4) orelse
	(C =:= $5) orelse (C =:= $6) orelse (C =:= $7) orelse (C =:= $8) orelse
	(C =:= $9) orelse (C =:= $:) orelse (C =:= $;) orelse (C =:= $=) orelse
	(C =:= $@) orelse (C =:= $A) orelse (C =:= $B) orelse (C =:= $C) orelse
	(C =:= $D) orelse (C =:= $E) orelse (C =:= $F) orelse (C =:= $G) orelse
	(C =:= $H) orelse (C =:= $I) orelse (C =:= $J) orelse (C =:= $K) orelse
	(C =:= $L) orelse (C =:= $M) orelse (C =:= $N) orelse (C =:= $O) orelse
	(C =:= $P) orelse (C =:= $Q) orelse (C =:= $R) orelse (C =:= $S) orelse
	(C =:= $T) orelse (C =:= $U) orelse (C =:= $V) orelse (C =:= $W) orelse
	(C =:= $X) orelse (C =:= $Y) orelse (C =:= $Z) orelse (C =:= $_) orelse
	(C =:= $a) orelse (C =:= $b) orelse (C =:= $c) orelse (C =:= $d) orelse
	(C =:= $e) orelse (C =:= $f) orelse (C =:= $g) orelse (C =:= $h) orelse
	(C =:= $i) orelse (C =:= $j) orelse (C =:= $k) orelse (C =:= $l) orelse
	(C =:= $m) orelse (C =:= $n) orelse (C =:= $o) orelse (C =:= $p) orelse
	(C =:= $q) orelse (C =:= $r) orelse (C =:= $s) orelse (C =:= $t) orelse
	(C =:= $u) orelse (C =:= $v) orelse (C =:= $w) orelse (C =:= $x) orelse
	(C =:= $y) orelse (C =:= $z) orelse (C =:= $~)
)).

urldecode(Binary) ->
	skip_dec(Binary, Binary, 0).

%% This functions helps avoid a binary allocation when
%% there is nothing to decode.
skip_dec(Binary, Orig, Len) ->
	case Binary of
		<<C1, C2, C3, C4, Rest/bits>>
				when ?IS_PLAIN(C1) andalso ?IS_PLAIN(C2)
				andalso ?IS_PLAIN(C3) andalso ?IS_PLAIN(C4) ->
			skip_dec(Rest, Orig, Len + 4);
		_ ->
			dec(Binary, [], Orig, 0, Len)
	end.

-dialyzer({no_improper_lists, [dec/5]}).
%% This clause helps speed up decoding of highly encoded values.
dec(<<$%, H1, L1, $%, H2, L2, $%, H3, L3, $%, H4, L4, Rest/bits>>, Acc, Orig, Skip, Len) ->
	C1 = ?UNHEX(H1, L1),
	C2 = ?UNHEX(H2, L2),
	C3 = ?UNHEX(H3, L3),
	C4 = ?UNHEX(H4, L4),
	case Len of
		0 ->
			dec(Rest, [Acc|<<C1, C2, C3, C4>>], Orig, Skip + 12, 0);
		_ ->
			Part = binary_part(Orig, Skip, Len),
			dec(Rest, [Acc, Part|<<C1, C2, C3, C4>>], Orig, Skip + Len + 12, 0)
	end;
dec(<<$%, H, L, Rest/bits>>, Acc, Orig, Skip, Len) ->
	C = ?UNHEX(H, L),
	case Len of
		0 ->
			dec(Rest, [Acc|<<C>>], Orig, Skip + 3, 0);
		_ ->
			Part = binary_part(Orig, Skip, Len),
			dec(Rest, [Acc, Part|<<C>>], Orig, Skip + Len + 3, 0)
	end;
%% This clause helps speed up decoding of barely encoded values.
dec(<<C1, C2, C3, C4, Rest/bits>>, Acc, Orig, Skip, Len)
				when ?IS_PLAIN(C1) andalso ?IS_PLAIN(C2)
				andalso ?IS_PLAIN(C3) andalso ?IS_PLAIN(C4) ->
	dec(Rest, Acc, Orig, Skip, Len + 4);
dec(<<C, Rest/bits>>, Acc, Orig, Skip, Len) when ?IS_PLAIN(C) ->
	dec(Rest, Acc, Orig, Skip, Len + 1);
dec(<<>>, _, Orig, 0, _) ->
	Orig;
dec(<<>>, Acc, _, _, 0) ->
	iolist_to_binary(Acc);
dec(<<>>, Acc, Orig, Skip, Len) ->
	Part = binary_part(Orig, Skip, Len),
	iolist_to_binary([Acc|Part]);
dec(_, _, Orig, Skip, Len) ->
	error({invalid_byte, binary:at(Orig, Skip + Len)}).
