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
out(#oqueue{length = 0} = Q) ->
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
                     rear = Rear0} = Q) ->
    case catch remove(Item, Front0) of
        not_found ->
            %% TODO: this will walk the rear list in the least effective order
            %% assuming most deletes will be from the front
            case catch remove(Item, Rear0) of
                not_found ->
                    {error, not_found};
                Rear ->
                    Q#oqueue{rear = Rear,
                             length = Len - 1}
            end;
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
len(#oqueue{length = Len}) ->
    Len.

-spec to_list(oqueue()) -> list().
to_list(#oqueue{rear = Rear, front = Front}) ->
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

enqueue_rear(Item, [H | T]) when Item < H->
    [H | enqueue_rear(Item, T)];
enqueue_rear(Item, List) ->
    [Item | List].

enqueue_front(Item, [H | T]) when Item > H->
    [H | enqueue_front(Item, T)];
enqueue_front(Item, List) ->
    [Item | List].

maybe_reverse(#oqueue{front = [],
                      rear = [_|_] = Rear} = Q) ->
    Q#oqueue{front = lists:reverse(Rear),
             rear = [],
             last_front_item = hd(Rear)};
maybe_reverse(Q) ->
    Q.

