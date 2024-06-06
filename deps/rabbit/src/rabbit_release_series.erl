%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.
%%

-module(rabbit_release_series).

-define(EOL_DATE_KEY, release_series_eol_date).

-export([
  is_currently_supported/0,
  readable_support_status/0
]).

%% Retained for backwards compatibility with older CLI tools.
-spec is_currently_supported() -> boolean().
is_currently_supported() ->
  true.

%% Retained for backwards compatibility with older CLI tools.
-spec readable_support_status() -> binary().
readable_support_status() ->
<<<<<<< HEAD
    case is_currently_supported() of
      false -> <<"out of support">>;
      _     -> <<"supported">>
  end.
=======
  <<"supported">>.
>>>>>>> 53c67ce45b (rabbitmq-diagnostics status: drop date-based support status field)
