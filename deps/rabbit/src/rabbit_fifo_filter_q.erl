%% A filterable queue with efficient random access and deletion.
-module(rabbit_fifo_filter_q).

-include("rabbit_fifo.hrl").
-export([
         new/0,
         in/3,
         size/1,
         take/3,
         take/4,
         is_fully_scanned/2,
         ra_indexes/1,
         overview/1
        ]).


-record(?MODULE, {
           hi :: gb_trees:tree(ra:index(), msg_header()),
           no :: gb_trees:tree(ra:index(), msg_header()),
           %% cache largest to avoid frequent gb_trees:largest/1 calls
           hi_max :: ra:index(),
           no_max :: ra:index()
          }).

-opaque state() :: #?MODULE{}.

-export_type([state/0]).

-spec new() -> state().
new() ->
    #?MODULE{hi = gb_trees:empty(),
             no = gb_trees:empty(),
             hi_max = 0,
             no_max = 0}.

-spec in(hi | no, msg(), state()) -> state().
in(hi, ?MSG(RaIdx, Hdr), State = #?MODULE{hi = Hi,
                                          hi_max = HiMax})
  when RaIdx > HiMax ->
    State#?MODULE{hi = gb_trees:insert(RaIdx, Hdr, Hi),
                  hi_max = RaIdx};
in(no, ?MSG(RaIdx, Hdr), State = #?MODULE{no = No,
                                          no_max = NoMax})
  when RaIdx > NoMax ->
    State#?MODULE{no = gb_trees:insert(RaIdx, Hdr, No),
                  no_max = RaIdx}.

-spec size(state()) -> non_neg_integer().
size(#?MODULE{hi = Hi,
              no = No}) ->
    gb_trees:size(Hi) +
    gb_trees:size(No).

%% Assumes the provided Ra index exists in the tree.
-spec take(ra:index(), hi | no, state()) ->
    {msg(), state()}.
take(Idx, hi, State0 = #?MODULE{hi = Hi0,
                                hi_max = HiMax0}) ->
    {Hdr, Hi} = gb_trees:take(Idx, Hi0),
    HiMax = new_largest(Idx, HiMax0, Hi),
    State = State0#?MODULE{hi = Hi,
                           hi_max = HiMax},
    {?MSG(Idx, Hdr), State};
take(Idx, no, State0 = #?MODULE{no = No0,
                                no_max = NoMax0}) ->
    {Hdr, No} = gb_trees:take(Idx, No0),
    NoMax = new_largest(Idx, NoMax0, No),
    State = State0#?MODULE{no = No,
                           no_max = NoMax},
    {?MSG(Idx, Hdr), State}.

-spec take(timestamp(),
           rabbit_amqp_filtex:filter_expressions(),
           {ra:index(), ra:index()},
           state()) ->
    {empty, {ra:index(), ra:index()}} |
    {msg(), {ra:index(), ra:index()}, state()}.
take(RaTs, Filter, {HiIdx, NoIdx}, State0 = #?MODULE{hi = Hi0,
                                                     hi_max = HiMax0})
  when HiIdx < HiMax0 ->
    case filter(RaTs, Filter, HiIdx, Hi0) of
        none ->
            take(RaTs, Filter, {HiMax0, NoIdx}, State0);
        Msg = ?MSG(Idx, _Hdr) ->
            Hi = gb_trees:delete(Idx, Hi0),
            HiMax = new_largest(Idx, HiMax0, Hi),
            State = State0#?MODULE{hi = Hi,
                                   hi_max = HiMax},
            {Msg, {Idx, NoIdx}, State}
    end;
take(RaTs, Filter, {HiIdx, NoIdx}, State0 = #?MODULE{no = No0,
                                                     no_max = NoMax0})
  when NoIdx < NoMax0 ->
    case filter(RaTs, Filter, NoIdx, No0) of
        none ->
            {empty, {HiIdx, NoMax0}};
        Msg = ?MSG(Idx, _Hdr) ->
            No = gb_trees:delete(Idx, No0),
            NoMax = new_largest(Idx, NoMax0, No),
            State = State0#?MODULE{no = No,
                                   no_max = NoMax},
            {Msg, {HiIdx, Idx}, State}
    end;
take(_RaTs, _Filter, Idxs, _State) ->
    {empty, Idxs}.

new_largest(Idx, Idx, Tree) ->
    %% The largest index was taken.
    case gb_trees:is_empty(Tree) of
        true ->
            Idx;
        false ->
            {NewLargest, _Val} = gb_trees:largest(Tree),
            NewLargest
    end;
new_largest(_IdxTaken, OldLargest, _Tree) ->
    OldLargest.

filter(RaTs, Filter, ScannedIdx, Tree) ->
    FromIdx = ScannedIdx + 1,
    Iter = gb_trees:iterator_from(FromIdx, Tree),
    filter_msg0(RaTs, Filter, gb_trees:next(Iter)).

filter_msg0(_RaTs, _Filter, none) ->
    none;
filter_msg0(RaTs, Filter, {Idx, Hdr = #{meta := Meta}, Iter}) ->
    case rabbit_fifo:get_header(expiry, Hdr) of
        ExpiryTs when is_integer(ExpiryTs) andalso RaTs >= ExpiryTs ->
            %% Message expired.
            filter_msg0(RaTs, Filter, gb_trees:next(Iter));
        _ ->
            case rabbit_fifo_filter:eval(Filter, Meta) of
                true ->
                    ?MSG(Idx, Hdr);
                false ->
                    filter_msg0(RaTs, Filter, gb_trees:next(Iter))
            end
    end.

-spec is_fully_scanned({ra:index(), ra:index()}, state()) ->
    boolean().
is_fully_scanned({HiIdx, NoIdx}, #?MODULE{hi_max = HiMax,
                                          no_max = NoMax}) ->
    HiIdx >= HiMax andalso
    NoIdx >= NoMax.

-spec ra_indexes(state()) ->
    {non_neg_integer(), [ra:index()]}.
ra_indexes(#?MODULE{hi = Hi,
                    no = No}) ->
    SizeHi = gb_trees:size(Hi),
    SizeNo = gb_trees:size(No),
    KeysHi = gb_trees:keys(Hi),
    KeysNo = gb_trees:keys(No),
    Keys = case SizeHi < SizeNo of
               true ->
                   KeysHi ++ KeysNo;
               false ->
                   KeysNo ++ KeysHi
           end,
    {SizeHi + SizeNo, Keys}.

-spec overview(state()) ->
    #{len := non_neg_integer(),
      num_hi := non_neg_integer(),
      num_no := non_neg_integer()}.
overview(#?MODULE{hi = Hi,
                  no = No}) ->
    NumHi = gb_trees:size(Hi),
    NumNo = gb_trees:size(No),
    #{len => NumHi + NumNo,
      num_hi => NumHi,
      num_no => NumNo}.
