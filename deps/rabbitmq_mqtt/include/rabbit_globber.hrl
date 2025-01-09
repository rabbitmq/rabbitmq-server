-record(globber,
        {separator = <<".">>,
         wildcard_one = <<"*">>,
         wildcard_some = <<"#">>,
         trie = maps:new()}).

-type globber() :: #globber{}.
