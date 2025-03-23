-module(rabbit_fifo_filter).

-include("rabbit_fifo.hrl").

-export([filter/2]).

filter(Filters, MsgMetaData) ->
    %% "A message will pass through a filter-set if and only if
    %% it passes through each of the named filters." [3.5.8]
    lists:all(fun(Filter) ->
                      filter0(Filter, MsgMetaData)
              end, Filters).

filter0({properties, KVList}, MsgMetaData) ->
    %% "The filter evaluates to true if all properties enclosed in the filter expression
    %% match the respective properties in the message."
    %% [filtex-v1.0-wd09 4.2.4]
    lists:all(
      fun({RefField, RefVal}) ->
              case lists:search(fun({?PROPERTIES_SECTION, Field, _Val})
                                      when Field =:= RefField ->
                                        true;
                                   (_) ->
                                        false
                                end, MsgMetaData) of
                  {value, {_, _, RefVal}} ->
                      true;
                  _ ->
                      false
              end
      end, KVList).
