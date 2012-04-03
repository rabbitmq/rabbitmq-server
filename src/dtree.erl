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
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2007-2012 VMware, Inc.  All rights reserved.
%%

%% A dual-index tree.
%%
%% Conceptually, what we want is a map that has two distinct sets of
%% keys (referred to here as primary and secondary, although that
%% shouldn't imply a hierarchy) pointing to one set of
%% values. However, in practice what we'll always want to do is insert
%% a value that's pointed at by (one primary, many secondaries) and
%% remove values that are pointed at by (one secondary, many
%% primaries) or (one secondary, all primaries). Thus the API.
%%
%% Entries exists while they have a non-empty secondary key set. The
%% 'take' operations return the entries that got removed, i.e. that
%% had no remaining secondary keys. 'take' ignores missing supplied
%% keys.

-module(dtree).

-export([empty/0, insert/4, take/3, take/2,
         is_defined/2, is_empty/1, smallest/1, size/1]).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-export_type([?MODULE/0]).

-opaque(?MODULE()  :: {gb_tree(), gb_tree()}).

-type(pk()         :: any()).
-type(sk()         :: any()).
-type(val()        :: any()).
-type(kv()         :: {pk(), val()}).

-spec(empty/0      :: () -> ?MODULE()).
-spec(insert/4     :: (pk(), [sk()], val(), ?MODULE()) -> ?MODULE()).
-spec(take/3       :: ([pk()], sk(), ?MODULE()) -> {[kv()], ?MODULE()}).
-spec(take/2       :: (sk(), ?MODULE()) -> {[kv()], ?MODULE()}).
-spec(is_defined/2 :: (sk(), ?MODULE()) -> boolean()).
-spec(is_empty/1   :: (?MODULE()) -> boolean()).
-spec(smallest/1   :: (?MODULE()) -> kv()).
-spec(size/1       :: (?MODULE()) -> non_neg_integer()).

-endif.

%%----------------------------------------------------------------------------

empty() -> {gb_trees:empty(), gb_trees:empty()}.

insert(PK, SKs, V, {P, S}) ->
    {gb_trees:insert(PK, {gb_sets:from_list(SKs), V}, P),
     lists:foldl(fun (SK, S0) ->
                         case gb_trees:lookup(SK, S0) of
                             {value, PKS} -> PKS1 = gb_sets:insert(PK, PKS),
                                             gb_trees:update(SK, PKS1, S0);
                             none         -> PKS = gb_sets:singleton(PK),
                                             gb_trees:insert(SK, PKS, S0)
                         end
                 end, S, SKs)}.

take(PKs, SK, {P, S}) ->
    case gb_trees:lookup(SK, S) of
        none         -> {[], {P, S}};
        {value, PKS} -> TakenPKS = gb_sets:from_list(PKs),
                        PKSInter = gb_sets:intersection(PKS, TakenPKS),
                        PKSDiff  = gb_sets:difference  (PKS, TakenPKS),
                        {KVs, P1} = take2(PKSInter, SK, P),
                        {KVs, {P1, case gb_sets:is_empty(PKSDiff) of
                                       true  -> gb_trees:delete(SK, S);
                                       false -> gb_trees:update(SK, PKSDiff, S)
                                   end}}
    end.

take(SK, {P, S}) ->
    case gb_trees:lookup(SK, S) of
        none         -> {[], {P, S}};
        {value, PKS} -> {KVs, P1} = take2(PKS, SK, P),
                        {KVs, {P1, gb_trees:delete(SK, S)}}
    end.

is_defined(SK, {_P, S}) -> gb_trees:is_defined(SK, S).

is_empty({P, _S}) -> gb_trees:is_empty(P).

smallest({P, _S}) -> {K, {_SKS, V}} = gb_trees:smallest(P),
                     {K, V}.

size({P, _S}) -> gb_trees:size(P).

%%----------------------------------------------------------------------------

take2(PKS, SK, P) ->
    gb_sets:fold(fun (PK, {KVs, P0}) ->
                         {SKS, V} = gb_trees:get(PK, P0),
                         SKS1 = gb_sets:delete(SK, SKS),
                         case gb_sets:is_empty(SKS1) of
                             true  -> KVs1 = [{PK, V} | KVs],
                                      {KVs1, gb_trees:delete(PK, P0)};
                             false -> {KVs,  gb_trees:update(PK, {SKS1, V}, P0)}
                         end
                 end, {[], P}, PKS).
