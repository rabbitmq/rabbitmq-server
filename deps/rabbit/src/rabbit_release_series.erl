%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_release_series).

-define(EOL_DATE_KEY, release_series_eol_date).

-export([
  eol_date/0,
  is_currently_supported/0,
  readable_support_status/0
]).

-spec eol_date() -> rabbit_types:maybe(calendar:date()).
eol_date() ->
  case application:get_env(rabbit, ?EOL_DATE_KEY) of
    undefined                  -> none;
    {ok, none}                -> none;
    {ok, {_Y, _M, _D} = Date} -> Date;
      _                       -> none
  end.

-spec is_currently_supported() -> boolean().
is_currently_supported() ->
  case eol_date() of
    none -> true;
    Date -> not rabbit_date_time:is_in_the_past(Date)
  end.

-spec readable_support_status() -> binary().
readable_support_status() ->
    case is_currently_supported() of
      false -> <<"out of support">>;
      _     -> <<"supported">>
  end.