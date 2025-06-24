-record(rabbit_cli, {progname :: string(),
                     args :: argparse:args(),
                     argparse_def :: argparse:command() | undefined,
                     arg_map :: argparse:arg_map() | undefined,
                     cmd_path :: argparse:cmd_path() | undefined,
                     command :: argparse:command() | undefined,
                     legacy :: boolean() | undefined,

                     priv}).
