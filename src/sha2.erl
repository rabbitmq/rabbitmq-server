%%% @author  Steve Vinoski <vinoski@ieee.org> [http://steve.vinoski.net/]
%%% @doc Implementations of SHA-224, SHA-256, SHA-384, SHA-512 in Erlang.
%%% @reference See <a href="http://csrc.nist.gov/publications/fips/fips180-2/fips180-2withchangenotice.pdf">
%%%            the Secure Hash Standard</a> and the <a href="http://en.wikipedia.org/wiki/SHA1">Wikipedia
%%%            SHA1 article</a>. Find the code <a href="http://steve.vinoski.net/code/sha2.erl">here</a>.
%%% @since 03 Jan 2009
%%%
%%% @copyright 2009 Stephen B. Vinoski, All rights reserved. Open source, BSD License
%%% @version 1.1
%%%

%%%
%%% Copyright (c) 2009 Stephen B. Vinoski
%%% All rights reserved.
%%%
%%% Redistribution and use in source and binary forms, with or without
%%% modification, are permitted provided that the following conditions
%%% are met:
%%% 1. Redistributions of source code must retain the above copyright
%%%    notice, this list of conditions and the following disclaimer.
%%% 2. Redistributions in binary form must reproduce the above copyright
%%%    notice, this list of conditions and the following disclaimer in the
%%%    documentation and/or other materials provided with the distribution.
%%% 3. Neither the name of the copyright holder nor the names of contributors
%%%    may be used to endorse or promote products derived from this software
%%%    without specific prior written permission.
%%% 
%%% THIS SOFTWARE IS PROVIDED BY THE AUTHOR AND CONTRIBUTOR(S) ``AS IS'' AND
%%% ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
%%% IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
%%% ARE DISCLAIMED.  IN NO EVENT SHALL THE AUTHOR OR CONTRIBUTOR(S) BE LIABLE
%%% FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
%%% DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
%%% OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
%%% HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
%%% LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
%%% OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
%%% SUCH DAMAGE.
%%%

-module(sha2).
-export([hexdigest224/1, hexdigest256/1, hexdigest384/1, hexdigest512/1]).
-export([test/0, test224/0, test256/0, test384/0, test512/0]).
-version(1.1).

-define(H224, [16#C1059ED8, 16#367CD507, 16#3070DD17, 16#F70E5939,
               16#FFC00B31, 16#68581511, 16#64F98FA7, 16#BEFA4FA4]).

-define(H256, [16#6A09E667, 16#BB67AE85, 16#3C6EF372, 16#A54FF53A,
               16#510E527F, 16#9B05688C, 16#1F83D9AB, 16#5BE0CD19]).

-define(H384, [16#CBBB9D5DC1059ED8, 16#629A292A367CD507, 16#9159015A3070DD17,
               16#152FECD8F70E5939, 16#67332667FFC00B31, 16#8EB44A8768581511,
               16#DB0C2E0D64F98FA7, 16#47B5481DBEFA4FA4]).

-define(H512, [16#6A09E667F3BCC908, 16#BB67AE8584CAA73B, 16#3C6EF372FE94F82B,
               16#A54FF53A5F1D36F1, 16#510E527FADE682D1, 16#9B05688C2B3E6C1F,
               16#1F83D9ABFB41BD6B, 16#5BE0CD19137E2179]).

-define(K256, <<16#428A2F98:32/big-unsigned, 16#71374491:32/big-unsigned, 16#B5C0FBCF:32/big-unsigned,
               16#E9B5DBA5:32/big-unsigned, 16#3956C25B:32/big-unsigned, 16#59F111F1:32/big-unsigned,
               16#923F82A4:32/big-unsigned, 16#AB1C5ED5:32/big-unsigned, 16#D807AA98:32/big-unsigned,
               16#12835B01:32/big-unsigned, 16#243185BE:32/big-unsigned, 16#550C7DC3:32/big-unsigned,
               16#72BE5D74:32/big-unsigned, 16#80DEB1FE:32/big-unsigned, 16#9BDC06A7:32/big-unsigned,
               16#C19BF174:32/big-unsigned, 16#E49B69C1:32/big-unsigned, 16#EFBE4786:32/big-unsigned,
               16#0FC19DC6:32/big-unsigned, 16#240CA1CC:32/big-unsigned, 16#2DE92C6F:32/big-unsigned,
               16#4A7484AA:32/big-unsigned, 16#5CB0A9DC:32/big-unsigned, 16#76F988DA:32/big-unsigned,
               16#983E5152:32/big-unsigned, 16#A831C66D:32/big-unsigned, 16#B00327C8:32/big-unsigned,
               16#BF597FC7:32/big-unsigned, 16#C6E00BF3:32/big-unsigned, 16#D5A79147:32/big-unsigned,
               16#06CA6351:32/big-unsigned, 16#14292967:32/big-unsigned, 16#27B70A85:32/big-unsigned,
               16#2E1B2138:32/big-unsigned, 16#4D2C6DFC:32/big-unsigned, 16#53380D13:32/big-unsigned,
               16#650A7354:32/big-unsigned, 16#766A0ABB:32/big-unsigned, 16#81C2C92E:32/big-unsigned,
               16#92722C85:32/big-unsigned, 16#A2BFE8A1:32/big-unsigned, 16#A81A664B:32/big-unsigned,
               16#C24B8B70:32/big-unsigned, 16#C76C51A3:32/big-unsigned, 16#D192E819:32/big-unsigned,
               16#D6990624:32/big-unsigned, 16#F40E3585:32/big-unsigned, 16#106AA070:32/big-unsigned,
               16#19A4C116:32/big-unsigned, 16#1E376C08:32/big-unsigned, 16#2748774C:32/big-unsigned,
               16#34B0BCB5:32/big-unsigned, 16#391C0CB3:32/big-unsigned, 16#4ED8AA4A:32/big-unsigned,
               16#5B9CCA4F:32/big-unsigned, 16#682E6FF3:32/big-unsigned, 16#748F82EE:32/big-unsigned,
               16#78A5636F:32/big-unsigned, 16#84C87814:32/big-unsigned, 16#8CC70208:32/big-unsigned,
               16#90BEFFFA:32/big-unsigned, 16#A4506CEB:32/big-unsigned, 16#BEF9A3F7:32/big-unsigned,
               16#C67178F2:32/big-unsigned>>).

-define(K512, <<16#428A2F98D728AE22:64/big-unsigned, 16#7137449123EF65CD:64/big-unsigned,
               16#B5C0FBCFEC4D3B2F:64/big-unsigned, 16#E9B5DBA58189DBBC:64/big-unsigned,
               16#3956C25BF348B538:64/big-unsigned, 16#59F111F1B605D019:64/big-unsigned,
               16#923F82A4AF194F9B:64/big-unsigned, 16#AB1C5ED5DA6D8118:64/big-unsigned,
               16#D807AA98A3030242:64/big-unsigned, 16#12835B0145706FBE:64/big-unsigned,
               16#243185BE4EE4B28C:64/big-unsigned, 16#550C7DC3D5FFB4E2:64/big-unsigned, 
               16#72BE5D74F27B896F:64/big-unsigned, 16#80DEB1FE3B1696B1:64/big-unsigned,
               16#9BDC06A725C71235:64/big-unsigned, 16#C19BF174CF692694:64/big-unsigned,
               16#E49B69C19EF14AD2:64/big-unsigned, 16#EFBE4786384F25E3:64/big-unsigned,
               16#0FC19DC68B8CD5B5:64/big-unsigned, 16#240CA1CC77AC9C65:64/big-unsigned,
               16#2DE92C6F592B0275:64/big-unsigned, 16#4A7484AA6EA6E483:64/big-unsigned, 
               16#5CB0A9DCBD41FBD4:64/big-unsigned, 16#76F988DA831153B5:64/big-unsigned,
               16#983E5152EE66DFAB:64/big-unsigned, 16#A831C66D2DB43210:64/big-unsigned,
               16#B00327C898FB213F:64/big-unsigned, 16#BF597FC7BEEF0EE4:64/big-unsigned,
               16#C6E00BF33DA88FC2:64/big-unsigned, 16#D5A79147930AA725:64/big-unsigned,
               16#06CA6351E003826F:64/big-unsigned, 16#142929670A0E6E70:64/big-unsigned,
               16#27B70A8546D22FFC:64/big-unsigned, 16#2E1B21385C26C926:64/big-unsigned,
               16#4D2C6DFC5AC42AED:64/big-unsigned, 16#53380D139D95B3DF:64/big-unsigned,
               16#650A73548BAF63DE:64/big-unsigned, 16#766A0ABB3C77B2A8:64/big-unsigned,
               16#81C2C92E47EDAEE6:64/big-unsigned, 16#92722C851482353B:64/big-unsigned,
               16#A2BFE8A14CF10364:64/big-unsigned, 16#A81A664BBC423001:64/big-unsigned,
               16#C24B8B70D0F89791:64/big-unsigned, 16#C76C51A30654BE30:64/big-unsigned,
               16#D192E819D6EF5218:64/big-unsigned, 16#D69906245565A910:64/big-unsigned,
               16#F40E35855771202A:64/big-unsigned, 16#106AA07032BBD1B8:64/big-unsigned,
               16#19A4C116B8D2D0C8:64/big-unsigned, 16#1E376C085141AB53:64/big-unsigned,
               16#2748774CDF8EEB99:64/big-unsigned, 16#34B0BCB5E19B48A8:64/big-unsigned,
               16#391C0CB3C5C95A63:64/big-unsigned, 16#4ED8AA4AE3418ACB:64/big-unsigned,
               16#5B9CCA4F7763E373:64/big-unsigned, 16#682E6FF3D6B2B8A3:64/big-unsigned,
               16#748F82EE5DEFB2FC:64/big-unsigned, 16#78A5636F43172F60:64/big-unsigned,
               16#84C87814A1F0AB72:64/big-unsigned, 16#8CC702081A6439EC:64/big-unsigned,
               16#90BEFFFA23631E28:64/big-unsigned, 16#A4506CEBDE82BDE9:64/big-unsigned,
               16#BEF9A3F7B2C67915:64/big-unsigned, 16#C67178F2E372532B:64/big-unsigned,
               16#CA273ECEEA26619C:64/big-unsigned, 16#D186B8C721C0C207:64/big-unsigned,
               16#EADA7DD6CDE0EB1E:64/big-unsigned, 16#F57D4F7FEE6ED178:64/big-unsigned,
               16#06F067AA72176FBA:64/big-unsigned, 16#0A637DC5A2C898A6:64/big-unsigned,
               16#113F9804BEF90DAE:64/big-unsigned, 16#1B710B35131C471B:64/big-unsigned,
               16#28DB77F523047D84:64/big-unsigned, 16#32CAAB7B40C72493:64/big-unsigned,
               16#3C9EBE0A15C9BEBC:64/big-unsigned, 16#431D67C49C100D4C:64/big-unsigned,
               16#4CC5D4BECB3E42B6:64/big-unsigned, 16#597F299CFC657E2A:64/big-unsigned,
               16#5FCB6FAB3AD6FAEC:64/big-unsigned, 16#6C44198C4A475817:64/big-unsigned>>).

-define(ADD32(X, Y), (X + Y) band 16#FFFFFFFF).
-define(ADD64(X, Y), (X + Y) band 16#FFFFFFFFFFFFFFFF).

%% @spec hexdigest224(message()) -> digest()
%% where
%%       message() = binary() | string()
%%       digest()  = binary() | string()
%% @doc Returns a SHA-224 hexadecimal digest.
%%      If the argument is a binary, the result is a binary, otherwise the argument is
%%      expected to be a string and the result is a string.
%%
hexdigest224(M) when is_binary(M) ->
    digest_bin(M, ?H224, 64, fun sha256_pad/1, fun sha224/2, 32);
hexdigest224(Str) ->
    digest_str(Str, ?H224, 64, fun sha256_pad/1, fun sha224/2, "~8.16.0b").

%% @spec hexdigest256(message()) -> digest()
%% where
%%       message() = binary() | string()
%%       digest()  = binary() | string()
%% @doc Returns a SHA-256 hexadecimal digest.
%%      If the argument is a binary, the result is a binary, otherwise the argument is
%%      expected to be a string and the result is a string.
%%
hexdigest256(M) when is_binary(M) ->
    digest_bin(M, ?H256, 64, fun sha256_pad/1, fun sha256/2, 32);
hexdigest256(Str) ->
    digest_str(Str, ?H256, 64, fun sha256_pad/1, fun sha256/2, "~8.16.0b").

%% @spec hexdigest384(message()) -> digest()
%% where
%%       message() = binary() | string()
%%       digest()  = binary() | string()
%% @doc Returns a SHA-384 hexadecimal digest.
%%      If the argument is a binary, the result is a binary, otherwise the argument is
%%      expected to be a string and the result is a string.
%%
hexdigest384(M) when is_binary(M) ->
    digest_bin(M, ?H384, 128, fun sha512_pad/1, fun sha384/2, 64);
hexdigest384(Str) ->
    digest_str(Str, ?H384, 128, fun sha512_pad/1, fun sha384/2, "~16.16.0b").

%% @spec hexdigest512(message()) -> digest()
%% where
%%       message() = binary() | string()
%%       digest()  = binary() | string()
%% @doc Returns a SHA-512 hexadecimal digest.
%%      If the argument is a binary, the result is a binary, otherwise the argument is
%%      expected to be a string and the result is a string.
%%
hexdigest512(M) when is_binary(M) ->
    digest_bin(M, ?H512, 128, fun sha512_pad/1, fun sha512/2, 64);
hexdigest512(Str) ->
    digest_str(Str, ?H512, 128, fun sha512_pad/1, fun sha512/2, "~16.16.0b").

digest_bin(M, Hashes, Bit_len, Pad, Sha, Word_size) ->
    list_to_binary([<<V:Word_size/big-unsigned>> || V <- Sha(split_binary(Pad(M), Bit_len), Hashes)]).
digest_str(Str, Hashes, Bit_len, Pad, Sha, Io_fmt) ->
    M = list_to_binary(Str),
    lists:flatten([io_lib:format(Io_fmt, [V]) || V <- Sha(split_binary(Pad(M), Bit_len), Hashes)]).
    

rotate32(V, Count) ->
    Rest = 32 - Count,
    <<Top:Rest/unsigned, Bottom:Count/unsigned>> = <<V:32/big-unsigned>>,
    <<New:32/big-unsigned>> = <<Bottom:Count/unsigned, Top:Rest/unsigned>>,
    New.

rotate64(V, Count) ->
    Rest = 64 - Count,
    <<Top:Rest/unsigned, Bottom:Count/unsigned>> = <<V:64/big-unsigned>>,
    <<New:64/big-unsigned>> = <<Bottom:Count/unsigned, Top:Rest/unsigned>>,
    New.

sha_pad(M, Base) ->
    Len = size(M),
    Len_bits = Len*8,
    Pad_bits = (Len + 1 + Base div 8) rem Base,
    Pad = case Pad_bits of
              0 -> 0;
              _ -> (Base - Pad_bits) * 8
          end,
    list_to_binary([M, <<16#80:8, 0:Pad, Len_bits:Base/big-unsigned>>]).

sha256_pad(M) ->
    sha_pad(M, 64).

sha512_pad(M) ->
    sha_pad(M, 128).

sha256_extend(W, 64) ->
    W;
sha256_extend(W, Count) ->
    Off1 = (Count - 15) * 4,
    Off2 = (Count - 2) * 4 - Off1 - 4,
    <<_:Off1/binary, Word1:32/big-unsigned, _:Off2/binary, Word2:32/big-unsigned, _/binary>> = <<W/binary>>,
    S0 = rotate32(Word1, 7) bxor rotate32(Word1, 18) bxor (Word1 bsr 3),
    S1 = rotate32(Word2, 17) bxor rotate32(Word2, 19) bxor (Word2 bsr 10),
    Off3 = (Count - 16) * 4,
    Off4 = (Count - 7) * 4 - Off3 - 4,
    <<_:Off3/binary, W16:32/big-unsigned, _:Off4/binary, W7:32/big-unsigned, _/binary>> = <<W/binary>>,
    Next = (W16 + S0 + W7 + S1) band 16#FFFFFFFF,
    sha256_extend(<<W/binary, Next:32/big-unsigned>>, Count+1).

sha512_extend(W, 80) ->
    W;
sha512_extend(W, Count) ->
    Off1 = (Count - 15) * 8,
    Off2 = (Count - 2) * 8 - Off1 - 8,
    <<_:Off1/binary, Word1:64/big-unsigned, _:Off2/binary, Word2:64/big-unsigned, _/binary>> = <<W/binary>>,
    S0 = rotate64(Word1, 1) bxor rotate64(Word1, 8) bxor (Word1 bsr 7),
    S1 = rotate64(Word2, 19) bxor rotate64(Word2, 61) bxor (Word2 bsr 6),
    Off3 = (Count - 16) * 8,
    Off4 = (Count - 7) * 8 - Off3 - 8,
    <<_:Off3/binary, W16:64/big-unsigned, _:Off4/binary, W7:64/big-unsigned, _/binary>> = <<W/binary>>,
    Next = (W16 + S0 + W7 + S1) band 16#FFFFFFFFFFFFFFFF,
    sha512_extend(<<W/binary, Next:64/big-unsigned>>, Count+1).

sha256_loop(_W, Hashes, Next, 64) ->
    lists:map(fun({X, Y}) -> ?ADD32(X, Y) end, lists:zip(Hashes, Next));
sha256_loop(W, Hashes, [A, B, C, D, E, F, G, H], Count) ->
    S0 = rotate32(A, 2) bxor rotate32(A, 13) bxor rotate32(A, 22),
    Maj = (A band B) bxor (A band C) bxor (B band C),
    T2 = ?ADD32(S0, Maj),
    S1 = rotate32(E, 6) bxor rotate32(E, 11) bxor rotate32(E, 25),
    Ch = (E band F) bxor (((bnot E) + 1 + 16#FFFFFFFF) band G),
    Offset = Count * 4,
    <<_:Offset/binary, K:32/big-unsigned, _/binary>> = ?K256,
    <<_:Offset/binary, Wval:32/big-unsigned, _/binary>> = <<W/binary>>,
    T1 = (H + S1 + Ch + K + Wval) band 16#FFFFFFFF,
    sha256_loop(W, Hashes, [?ADD32(T1, T2), A, B, C, ?ADD32(D, T1), E, F, G], Count+1).

sha512_loop(_W, Hashes, Next, 80) ->
    lists:map(fun({X, Y}) -> ?ADD64(X, Y) end, lists:zip(Hashes, Next));
sha512_loop(W, Hashes, [A, B, C, D, E, F, G, H], Count) ->
    S0 = rotate64(A, 28) bxor rotate64(A, 34) bxor rotate64(A, 39),
    Maj = (A band B) bxor (A band C) bxor (B band C),
    T2 = ?ADD64(S0, Maj),
    S1 = rotate64(E, 14) bxor rotate64(E, 18) bxor rotate64(E, 41),
    Ch = (E band F) bxor (((bnot E) + 1 + 16#FFFFFFFFFFFFFFFF) band G),
    Offset = Count * 8,
    <<_:Offset/binary, K:64/big-unsigned, _/binary>> = ?K512,
    <<_:Offset/binary, Wval:64/big-unsigned, _/binary>> = <<W/binary>>,
    T1 = (H + S1 + Ch + K + Wval) band 16#FFFFFFFFFFFFFFFF,
    sha512_loop(W, Hashes, [?ADD64(T1, T2), A, B, C, ?ADD64(D, T1), E, F, G], Count+1).

sha256(M, Hashes) when is_binary(M) ->
    Words64 = sha256_extend(M, 16),
    sha256_loop(Words64, Hashes, Hashes, 0);
sha256({M, <<>>}, Hashes) ->
    sha256(M, Hashes);
sha256({M, T}, Hashes) ->
    sha256(split_binary(T, 64), sha256(M, Hashes)).

sha224({M, <<>>}, Hashes) ->
    [H0, H1, H2, H3, H4, H5, H6, _H7] = sha256(M, Hashes),
    [H0, H1, H2, H3, H4, H5, H6];
sha224({M, T}, Hashes) ->
    sha224(split_binary(T, 64), sha256(M, Hashes)).

sha512(M, Hashes) when is_binary(M) ->
    Words128 = sha512_extend(M, 16),
    sha512_loop(Words128, Hashes, Hashes, 0);
sha512({M, <<>>}, Hashes) ->
    sha512(M, Hashes);
sha512({M, T}, Hashes) ->
    sha512(split_binary(T, 128), sha512(M, Hashes)).

sha384({M, <<>>}, Hashes) ->
    [H0, H1, H2, H3, H4, H5 | _] = sha512(M, Hashes),
    [H0, H1, H2, H3, H4, H5];
sha384({M, T}, Hashes) ->
    sha384(split_binary(T, 128), sha512(M, Hashes)).



%%% These tests come from <http://www.aarongifford.com/computers/sha.html>. The BASENAME macro above the
%%% read_test_vectors/0 fun is expected to be the basename of the test vectors filenames from that
%%% website. The tests read the test data from those files and compare against the expected results. Download
%%% this file to get the test data: <http://www.aarongifford.com/computers/sha2-1.0.tar.gz>. The test data in
%%% the "Expected" lists were either generated using the Python hashlib module, or were copied from the
%%% aforementioned URI/file; the copied data are subject to the following license:
%%%
%%% Copyright (c) 2000-2001, Aaron D. Gifford
%%% All rights reserved.
%%%
%%% Redistribution and use in source and binary forms, with or without
%%% modification, are permitted provided that the following conditions
%%% are met:
%%% 1. Redistributions of source code must retain the above copyright
%%%    notice, this list of conditions and the following disclaimer.
%%% 2. Redistributions in binary form must reproduce the above copyright
%%%    notice, this list of conditions and the following disclaimer in the
%%%    documentation and/or other materials provided with the distribution.
%%% 3. Neither the name of the copyright holder nor the names of contributors
%%%    may be used to endorse or promote products derived from this software
%%%    without specific prior written permission.
%%% 
%%% THIS SOFTWARE IS PROVIDED BY THE AUTHOR AND CONTRIBUTOR(S) ``AS IS'' AND
%%% ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
%%% IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
%%% ARE DISCLAIMED.  IN NO EVENT SHALL THE AUTHOR OR CONTRIBUTOR(S) BE LIABLE
%%% FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
%%% DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
%%% OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
%%% HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
%%% LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
%%% OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
%%% SUCH DAMAGE.

%% @hidden
test() ->
    Vectors = read_test_vectors(),
    test224(Vectors),
    test256(Vectors),
    test384(Vectors),
    test512(Vectors).

%% @hidden
test224() ->
    test224(read_test_vectors()).
test224(Vectors) ->
    Expected224 = [<<16#23097d223405d8228642a477bda255b32aadbce4bda0b3f7e36c9da7:224/big-unsigned>>,
                   <<16#75388b16512776cc5dba5da1fd890150b0c6455cb4f58b1952522525:224/big-unsigned>>,
                   <<16#c97ca9a559850ce97a04a96def6d99a9e0e0e2ab14e6b8df265fc0b3:224/big-unsigned>>,
                   <<16#62a41ab0961bcdd22db70b896db3955c1d04096af6de47f5aaad1226:224/big-unsigned>>,
                   <<16#d14a028c2a3a2bc9476102bb288234c415a2b01f828ea62ac5b3e42f:224/big-unsigned>>,
                   <<16#d92622d56f83d869a884f6cc0763e90c4520a21e1cc429841e4584d2:224/big-unsigned>>,
                   <<16#0873433e1c8749dad0e34f92aff11c4b2ca310356283817747aa6940:224/big-unsigned>>,
                   <<16#5a69ccca0b5e7f84efda7c026d010fa46569c03f97b4440eba32b941:224/big-unsigned>>,
                   <<16#49e54148d21d457f2ffe28532543d91da98724c9883e67682301dec4:224/big-unsigned>>,
                   <<16#6417acfccd1d78cc14f1dd2de4ffcafe9cff0f92f0e28139866c2e2d:224/big-unsigned>>,
                   <<16#d4126ce69e15fc0c06cb1bf763f112b139ffd81189e3899e4e275560:224/big-unsigned>>,
                   <<16#0ace93ff0cfa76006af9db847f4ff2e702c2518dc946948807be0a47:224/big-unsigned>>,
                   <<16#91e452cfc8f22f9c69e637ec9dcf80d5798607a52234686fcf8880ad:224/big-unsigned>>,
                   <<16#bdaac28698611eba163f232785d8f4caffe29ac2fd8133651baf8212:224/big-unsigned>>,
                   <<16#4f41e1e6839ed85883ee0f259ac9025d19ecccbfc4d9d72f075ba5f2:224/big-unsigned>>,
                   <<16#4215dc642269cfd6d9b4b6da78fd01a9094bc89f4780905714b0a896:224/big-unsigned>>,
                   <<16#a1b0964a6d8188eb2980e126fefc70eb79d0745a91cc2f629af34ece:224/big-unsigned>>,
                   <<16#cc9286e04c4a39a6bb92a42f2ffabce02156090b6882b0ca22026294:224/big-unsigned>>],
    io:format("testing SHA-224~n"),
    test(fun hexdigest224/1, Vectors, Expected224, 1).

%% @hidden
test256() ->
    test256(read_test_vectors()).
test256(Vectors) ->
    Expected256 = [<<16#ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad:256/big-unsigned>>,
                   <<16#248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1:256/big-unsigned>>,
                   <<16#cf5b16a778af8380036ce59e7b0492370b249b11e8f07a51afac45037afee9d1:256/big-unsigned>>,
                   <<16#4d25fccf8752ce470a58cd21d90939b7eb25f3fa418dd2da4c38288ea561e600:256/big-unsigned>>,
                   <<16#e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855:256/big-unsigned>>,
                   <<16#ab64eff7e88e2e46165e29f2bce41826bd4c7b3552f6b382a9e7d3af47c245f8:256/big-unsigned>>,
                   <<16#f08a78cbbaee082b052ae0708f32fa1e50c5c421aa772ba5dbb406a2ea6be342:256/big-unsigned>>,
                   <<16#0ab803344830f92089494fb635ad00d76164ad6e57012b237722df0d7ad26896:256/big-unsigned>>,
                   <<16#e4326d0459653d7d3514674d713e74dc3df11ed4d30b4013fd327fdb9e394c26:256/big-unsigned>>,
                   <<16#a7f001d996dd25af402d03b5f61aef950565949c1a6ad5004efa730328d2dbf3:256/big-unsigned>>,
                   <<16#6dcd63a07b0922cc3a9b3315b158478681cc32543b0a4180abe58a73c5e14cc2:256/big-unsigned>>,
                   <<16#af6ebfde7d93d5badb6cde6287ecc2061c1cafc5b1c1217cd984fbcdb9c61aaa:256/big-unsigned>>,
                   <<16#8ff59c6d33c5a991088bc44dd38f037eb5ad5630c91071a221ad6943e872ac29:256/big-unsigned>>,
                   <<16#1818e87564e0c50974ecaabbb2eb4ca2f6cc820234b51861e2590be625f1f703:256/big-unsigned>>,
                   <<16#5e3dfe0cc98fd1c2de2a9d2fd893446da43d290f2512200c515416313cdf3192:256/big-unsigned>>,
                   <<16#80fced5a97176a5009207cd119551b42c5b51ceb445230d02ecc2663bbfb483a:256/big-unsigned>>,
                   <<16#88ee6ada861083094f4c64b373657e178d88ef0a4674fce6e4e1d84e3b176afb:256/big-unsigned>>,
                   <<16#5a2e925a7f8399fa63a20a1524ae83a7e3c48452f9af4df493c8c51311b04520:256/big-unsigned>>],
    io:format("testing SHA-256~n"),
    test(fun hexdigest256/1, Vectors, Expected256, 1).

%% @hidden
test384() ->
    test384(read_test_vectors()).
test384(Vectors) ->
    Expected384 = [<<16#cb00753f45a35e8bb5a03d699ac65007272c32ab0eded1631a8b605a43ff5bed:256/big-unsigned,
                    16#8086072ba1e7cc2358baeca134c825a7:128/big-unsigned>>,
                   <<16#3391fdddfc8dc7393707a65b1b4709397cf8b1d162af05abfe8f450de5f36bc6:256/big-unsigned,
                    16#b0455a8520bc4e6f5fe95b1fe3c8452b:128/big-unsigned>>,
                   <<16#09330c33f71147e83d192fc782cd1b4753111b173b3b05d22fa08086e3b0f712:256/big-unsigned,
                    16#fcc7c71a557e2db966c3e9fa91746039:128/big-unsigned>>,
                   <<16#69cc75b95280bdd9e154e743903e37b1205aa382e92e051b1f48a6db9d0203f8:256/big-unsigned,
                    16#a17c1762d46887037275606932d3381e:128/big-unsigned>>,
                   <<16#38b060a751ac96384cd9327eb1b1e36a21fdb71114be07434c0cc7bf63f6e1da:256/big-unsigned,
                    16#274edebfe76f65fbd51ad2f14898b95b:128/big-unsigned>>,
                   <<16#e28e35e25a1874908bf0958bb088b69f3d742a753c86993e9f4b1c4c21988f95:256/big-unsigned,
                    16#8bd1fe0315b195aca7b061213ac2a9bd:128/big-unsigned>>,
                   <<16#37b49ef3d08de53e9bd018b0630067bd43d09c427d06b05812f48531bce7d2a6:256/big-unsigned,
                    16#98ee2d1ed1ffed46fd4c3b9f38a8a557:128/big-unsigned>>,
                   <<16#e3e3602f4d90c935321d788f722071a8809f4f09366f2825cd85da97ccd2955e:256/big-unsigned,
                    16#b6b8245974402aa64789ed45293e94ba:128/big-unsigned>>,
                   <<16#1ca650f38480fa9dfb5729636bec4a935ebc1cd4c0055ee50cad2aa627e06687:256/big-unsigned,
                    16#1044fd8e6fdb80edf10b85df15ba7aab:128/big-unsigned>>,
                   <<16#b8261ddcd7df7b3969a516b72550de6fbf0e394a4a7bb2bbc60ec603c2ceff64:256/big-unsigned,
                    16#3c5bf62bc6dcbfa5beb54b62d750b969:128/big-unsigned>>,
                   <<16#548e4e9a1ff57f469ed47b023bf5279dfb4d4ca08c65051e3a5c41fab84479a2:256/big-unsigned,
                    16#05496276906008b4b3c5b0970b2f5446:128/big-unsigned>>,
                   <<16#c6fec3a3278dd6b5afc8c0971d32d38faf5802f1a21527c32563b32a1ac34065:256/big-unsigned,
                    16#6b433b44fe2648aa2232206f4301193a:128/big-unsigned>>,
                   <<16#92dca5655229b3c34796a227ff1809e273499adc2830149481224e0f54ff4483:256/big-unsigned,
                    16#bd49834d4865e508ef53d4cd22b703ce:128/big-unsigned>>,
                   <<16#310fbb2027bdb7042f0e09e7b092e9ada506649510a7aa029825c8e8019e9c30:256/big-unsigned,
                    16#749d723f2de1bd8c043d8d89d3748c2f:128/big-unsigned>>,
                   <<16#0d5e45317bc7997cb9c8a23bad9bac9170d5bc81789b51af6bcd74ace379fd64:256/big-unsigned,
                    16#9a2b48cb56c4cb4ec1477e6933329e0e:128/big-unsigned>>,
                   <<16#aa1e77c094e5ce6db81a1add4c095201d020b7f8885a4333218da3b799b9fc42:256/big-unsigned,
                    16#f00d60cd438a1724ae03bd7b515b739b:128/big-unsigned>>,
                   <<16#78cc6402a29eb984b8f8f888ab0102cabe7c06f0b9570e3d8d744c969db14397:256/big-unsigned,
                    16#f58ecd14e70f324bf12d8dd4cd1ad3b2:128/big-unsigned>>,
                   <<16#72ec26cc742bc5fb1ef82541c9cadcf01a15c8104650d305f24ec8b006d7428e:256/big-unsigned,
                    16#8ebe2bb320a465dbdd5c6326bbd8c9ad:128/big-unsigned>>],
    io:format("testing SHA-384~n"),
    test(fun hexdigest384/1, Vectors, Expected384, 1).

%% @hidden
test512() ->
    test512(read_test_vectors()).
test512(Vectors) ->
    Expected512 = [<<16#ddaf35a193617abacc417349ae20413112e6fa4e89a97ea20a9eeee64b55d39a:256/big-unsigned,
                    16#2192992a274fc1a836ba3c23a3feebbd454d4423643ce80e2a9ac94fa54ca49f:256/big-unsigned>>,
                   <<16#204a8fc6dda82f0a0ced7beb8e08a41657c16ef468b228a8279be331a703c335:256/big-unsigned,
                    16#96fd15c13b1b07f9aa1d3bea57789ca031ad85c7a71dd70354ec631238ca3445:256/big-unsigned>>,
                   <<16#8e959b75dae313da8cf4f72814fc143f8f7779c6eb9f7fa17299aeadb6889018:256/big-unsigned,
                    16#501d289e4900f7e4331b99dec4b5433ac7d329eeb6dd26545e96e55b874be909:256/big-unsigned>>,
                   <<16#23450737795d2f6a13aa61adcca0df5eef6df8d8db2b42cd2ca8f783734217a7:256/big-unsigned,
                    16#3e9cabc3c9b8a8602f8aeaeb34562b6b1286846060f9809b90286b3555751f09:256/big-unsigned>>,
                   <<16#cf83e1357eefb8bdf1542850d66d8007d620e4050b5715dc83f4a921d36ce9ce:256/big-unsigned,
                    16#47d0d13c5d85f2b0ff8318d2877eec2f63b931bd47417a81a538327af927da3e:256/big-unsigned>>,
                   <<16#70aefeaa0e7ac4f8fe17532d7185a289bee3b428d950c14fa8b713ca09814a38:256/big-unsigned,
                    16#7d245870e007a80ad97c369d193e41701aa07f3221d15f0e65a1ff970cedf030:256/big-unsigned>>,
                   <<16#b3de4afbc516d2478fe9b518d063bda6c8dd65fc38402dd81d1eb7364e72fb6e:256/big-unsigned,
                    16#6663cf6d2771c8f5a6da09601712fb3d2a36c6ffea3e28b0818b05b0a8660766:256/big-unsigned>>,
                   <<16#97fb4ec472f3cb698b9c3c12a12768483e5b62bcdad934280750b4fa4701e5e0:256/big-unsigned,
                    16#550a80bb0828342c19631ba55a55e1cee5de2fda91fc5d40e7bee1d4e6d415b3:256/big-unsigned>>,
                   <<16#d399507bbf5f2d0da51db1ff1fc51c1c9ff1de0937e00d01693b240e84fcc340:256/big-unsigned,
                    16#0601429f45c297acc6e8fcf1e4e4abe9ff21a54a0d3d88888f298971bd206cd5:256/big-unsigned>>,
                   <<16#caf970d3638e21053173a638c4b94d6d1ff87bc47b58f8ee928fbe9e245c23ab:256/big-unsigned,
                    16#f81019e45bf017ecc8610e5e0b95e3b025ccd611a772ca4fb3dfba26f0859725:256/big-unsigned>>,
                   <<16#ee5d07460183b130687c977e9f8d43110989b0864b18fe6ee00a53dec5eda111:256/big-unsigned,
                    16#f3aaa3bac7ab8dae26ed545a4de33ed45190f18fa0c327c44642ab9424265330:256/big-unsigned>>,
                   <<16#73ffeb67716c3495fbc33f2d62fe08e2616706a5599881c7e67e9ef2b68f4988:256/big-unsigned,
                    16#ea8b3b604ba87e50b07962692705c420fa31a00be41d6aaa9f3b11eafe9cf49b:256/big-unsigned>>,
                   <<16#0e928db6207282bfb498ee871202f2337f4074f3a1f5055a24f08e912ac118f8:256/big-unsigned,
                    16#101832cdb9c2f702976e629183db9bacfdd7b086c800687c3599f15de7f7b9dd:256/big-unsigned>>,
                   <<16#a001636f3ff1ce34f432f8e8f7785b78be84318beb8485a406650a8b243c419f:256/big-unsigned,
                    16#7db6435cf6bf3000c6524adb5b52bad01afb76b3ceff701331e18b85b0e4cbd3:256/big-unsigned>>,
                   <<16#735bd6bebfe6f8070d70069105bc761f35ed1ac3742f2e372fdc14d2a51898e6:256/big-unsigned,
                    16#153ccaff9073324130abdc451c730dc5dab5a0452487b1171c4dd97f92e267b7:256/big-unsigned>>,
                   <<16#fae25ec70bcb3bbdef9698b9d579da49db68318dbdf18c021d1f76aaceff9628:256/big-unsigned,
                    16#38873235597e7cce0c68aabc610e0deb79b13a01c302abc108e459ddfbe9bee8:256/big-unsigned>>,
                   <<16#211bec83fbca249c53668802b857a9889428dc5120f34b3eac1603f13d1b4796:256/big-unsigned,
                    16#5c387b39ef6af15b3a44c5e7b6bbb6c1096a677dc98fc8f472737540a332f378:256/big-unsigned>>,
                   <<16#ebad464e6d9f1df7e8aadff69f52db40a001b253fbf65a018f29974dcc7fbf8e:256/big-unsigned,
                    16#58b69e247975fbadb4153d7289357c9b6212752d0ab67dd3d9bbc0bb908aa98c:256/big-unsigned>>],
    io:format("testing SHA-512~n"),
    test(fun hexdigest512/1, Vectors, Expected512, 1).

test(_, [], _, _) ->
    ok;
test(Func, [Vector|Vectors], [Expect|Expected], Count) ->
    Result = Func(Vector),
    try
        Result = Expect,
        io:format("  test ~p passed~n", [Count]),
        test(Func, Vectors, Expected, Count+1)
    catch _:_ ->
            io:format("error: expected ~s, got ~s~n", [Expect, Result]),
            error
    end.

-define(BASENAME, "/usr/local/src/sha2-1.0/testvectors/vector0").

read_test_vectors() ->
    read_test_vectors(?BASENAME, [], 1).
read_test_vectors(_Base, Vectors, 19) ->
    lists:reverse(Vectors);
read_test_vectors(Base, Vectors, Num) ->
    {ok, Vector} = file:read_file(lists:flatten(io_lib:format("~s~2.10.0b.dat", [Base, Num]))),
    read_test_vectors(Base, [Vector | Vectors], Num+1).
