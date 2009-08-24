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
%%   The Original Code is RabbitMQ.
%%
%%   The Initial Developers of the Original Code are LShift Ltd,
%%   Cohesive Financial Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created before 22-Nov-2008 00:00:00 GMT by LShift Ltd,
%%   Cohesive Financial Technologies LLC, or Rabbit Technologies Ltd
%%   are Copyright (C) 2007-2008 LShift Ltd, Cohesive Financial
%%   Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created by LShift Ltd are Copyright (C) 2007-2009 LShift
%%   Ltd. Portions created by Cohesive Financial Technologies LLC are
%%   Copyright (C) 2007-2009 Cohesive Financial Technologies
%%   LLC. Portions created by Rabbit Technologies Ltd are Copyright
%%   (C) 2007-2009 Rabbit Technologies Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%

-module(rabbit_handle_cache).

-export([init/2, close_all/1, close_file/2, with_file_handle_at/4]).

-record(hcstate,
        { limit,   %% how many file handles can we open?
          handles, %% dict of the files to their handles, age and offset
          ages,    %% gb_tree of the files, keyed by age
          mode     %% the mode to open the files as
        }).

init(Limit, OpenMode) ->
    #hcstate { limit   = Limit,
               handles = dict:new(),
               ages    = gb_trees:empty(),
               mode    = OpenMode
             }.

close_all(State = #hcstate { handles = Handles }) ->
    dict:fold(fun (_File, {Hdl, _Offset, _Then}, _Acc) ->
                      file:close(Hdl)
              end, ok, Handles),
    State #hcstate { handles = dict:new(), ages = gb_trees:empty() }.

close_file(File, State = #hcstate { handles = Handles,
                                    ages = Ages }) ->
    case dict:find(File, Handles) of
        error ->
            State;
        {ok, {Hdl, _Offset, Then}} ->
            ok = file:close(Hdl),
            State #hcstate { handles = dict:erase(File, Handles),
                             ages = gb_trees:delete(Then, Ages)
                           }
    end.

with_file_handle_at(File, Offset, Fun, State = #hcstate { handles = Handles,
                                                          ages    = Ages,
                                                          limit   = Limit,
                                                          mode    = Mode }) ->
    {FileHdl, OldOffset, Handles1, Ages1} =
        case dict:find(File, Handles) of
            error ->
                {ok, Hdl} = file:open(File, Mode),
                case dict:size(Handles) < Limit of
                    true ->
                        {Hdl, 0, Handles, Ages};
                    false ->
                        {Then, OldFile, Ages2} = gb_trees:take_smallest(Ages),
                        {ok, {OldHdl, _Offset, Then}} =
                            dict:find(OldFile, Handles),
                        ok = file:close(OldHdl),
                        {Hdl, 0, dict:erase(OldFile, Handles), Ages2}
                end;
            {ok, {Hdl, OldOffset1, Then}} ->
                {Hdl, OldOffset1, Handles, gb_trees:delete(Then, Ages)}
        end,
    SeekRes = case Offset == OldOffset of
                  true -> ok;
                  false -> case file:position(FileHdl, {bof, Offset}) of
                               {ok, Offset} -> ok;
                               KO -> KO
                           end
              end,
    {NewOffset, Result} = case SeekRes of
                              ok -> Fun(FileHdl);
                              KO1 -> {Offset, KO1}
                          end,
    Now = now(),
    Handles2 = dict:store(File, {FileHdl, NewOffset, Now}, Handles1),
    Ages3 = gb_trees:enter(Now, File, Ages1),
    {Result, State #hcstate { handles = Handles2, ages = Ages3 }}.
