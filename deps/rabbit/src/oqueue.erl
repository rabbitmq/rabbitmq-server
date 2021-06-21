-module(oqueue).

-export([new/0,
         %% O(1) when item is larger than largest item inserted
         %% worst O(n)
         in/2,
         %% O(1) (amortised)
         out/1,
         %% fast when deleting in the order of insertion
         %% worst O(n)
         delete/2,
         %% O(1) (amortised)
         peek/1,
         %% O(1)
         len/1,
         to_list/1,
         from_list/1
        ]).

-record(oqueue, {length = 0 :: non_neg_integer(),
                 rear = [] :: list(),
                 rear_deletes = #{} :: map(),
                 front = [] :: list(),
                 last_front_item :: undefined | term()}).

-opaque oqueue() :: #oqueue{}.

-export_type([oqueue/0]).

-spec new() -> oqueue().
new() -> #oqueue{}.

-spec in(term(), oqueue()) -> oqueue().
in(Item, #oqueue{length = Len,
                 front = [_ | _] = Front,
                 last_front_item = LastFrontItem} = Q)
  when Item < LastFrontItem ->
    Q#oqueue{length = Len + 1,
             front = enqueue_front(Item, Front)};
in(Item, #oqueue{length = Len,
                 rear = Rear} = Q) ->
    Q#oqueue{length = Len + 1,
             rear = enqueue_rear(Item, Rear)}.

-spec out(oqueue()) ->
    {empty | {value, term()}, oqueue()}.
out(#oqueue{length = Len, rear_deletes = Dels} = Q)
  when Len - map_size(Dels) == 0 ->
    {empty, Q};
out(#oqueue{front = [Item], length  = Len} = Q) ->
    {{value, Item}, Q#oqueue{front = [],
                             last_front_item = undefined,
                             length = Len - 1}};
out(#oqueue{front = [Item | Rem], length  = Len} = Q) ->
    {{value, Item}, Q#oqueue{front = Rem,
                             length = Len - 1}};
out(#oqueue{front = []} = Q) ->
    out(maybe_reverse(Q)).

-spec delete(term(), oqueue()) ->
    oqueue() | {error, not_found}.
delete(Item, #oqueue{length = Len,
                     last_front_item = LFI,
                     front = [_ | _] = Front0,
                     rear = Rear0,
                     rear_deletes = Dels0} = Q) ->
    %% TODO: check if item is out of range to avoid scan
    case Item > LFI of
        true when map_size(Dels0) == 31 ->
            Rear = Rear0 -- maps:keys(Dels0#{Item => Item}),
            %% TODO we don't know all were actually deleted
            Q#oqueue{rear = Rear,
                     rear_deletes = #{},
                     length = Len - 32};
            %% item is not in front, scan rear list
            %% TODO: this will walk the rear list in the least effective order
            %% assuming most deletes will be from the front
            % case catch remove(Item, Rear0) of
            %     not_found ->
            %         {error, not_found};
            %     Rear ->
            %         Q#oqueue{rear = Rear,
            %                  length = Len - 1}
            % end;
        true ->
            %% cache delete
            Q#oqueue{rear_deletes = Dels0#{Item => Item}};
        false ->
            case catch remove(Item, Front0) of
                not_found ->
                    {error, not_found};
                [] ->
                    maybe_reverse(Q#oqueue{front = [],
                                           last_front_item = undefined,
                                           length = Len - 1});
                Front when LFI == Item ->
                    %% the last item of the front list was removed but we still have
                    %% items in the front list, inefficient to take last but this should
                    %% be a moderately rare case given the use case of the oqueue
                    Q#oqueue{front = Front,
                             last_front_item = lists:last(Front),
                             length = Len - 1};
                Front ->
                    Q#oqueue{front = Front,
                             length = Len - 1}
            end
    end;
delete(_Item, #oqueue{front = [], rear = []}) ->
    {error, not_found};
delete(Item, #oqueue{front = []} = Q) ->
    delete(Item, maybe_reverse(Q)).

-spec peek(oqueue()) ->
    empty | {value, term(), oqueue()}.
peek(#oqueue{front = [H | _]} = Q) ->
    {value, H, Q};
peek(#oqueue{rear = [_|_]} = Q) ->
    %% the front is empty, reverse rear now
    %% so that future peek ops are cheap
    peek(maybe_reverse(Q));
peek(_) ->
    empty.

-spec len(oqueue()) -> non_neg_integer().
len(#oqueue{rear_deletes = Dels, length = Len}) ->
    Len - map_size(Dels).

-spec to_list(oqueue()) -> list().
to_list(#oqueue{rear = Rear0, rear_deletes = Dels, front = Front}) ->
    Rear = Rear0 -- maps:keys(Dels),
    Front ++ lists:reverse(Rear).

-spec from_list(list()) -> oqueue().
from_list(List) ->
    lists:foldl(fun in/2, new(), List).

%% internal

remove(_Item, []) ->
    throw(not_found);
remove(Item, [Item | Tail]) ->
    Tail;
remove(Item, [H | Tail]) ->
    [H | remove(Item, Tail)].

% remove_all(Items, []) ->
%     throw({empty_list, Items});
% remove_all([], Tail) ->
%     Tail;
% remove_all([Item | RemItems], [Item | Tail]) ->
%     %% how to record an item was deleted?
%     remove_all(RemItems, Tail);
% remove_all([Item | Items], [H | Tail]) when Item < H ->
%     [H | remove_all(Item, Tail)].

enqueue_rear(Item, [H | T]) when Item < H->
    [H | enqueue_rear(Item, T)];
enqueue_rear(Item, List) ->
    [Item | List].

enqueue_front(Item, [H | T]) when Item > H->
    [H | enqueue_front(Item, T)];
enqueue_front(Item, List) ->
    [Item | List].

maybe_reverse(#oqueue{front = [],
                      length = Len,
                      rear_deletes = Dels,
                      rear = [_|_] = Rear0} = Q) ->
    Rear = Rear0 -- maps:keys(Dels),
    Q#oqueue{front = lists:reverse(Rear),
             rear_deletes = #{},
             length = Len - map_size(Dels),
             rear = [],
             last_front_item = hd(Rear)};
maybe_reverse(Q) ->
    Q.

