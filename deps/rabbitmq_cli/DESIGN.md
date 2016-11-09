# Design Decisions

## Warning: This Document is Out of Date

**This document is very much out of date and needs a rewrite**

## Principles
* *No Command Implementation in the Main Module*: In order to keep the CLI easily extensible, any behavior unique to a command should be implemented within the command's module. It's okay to use helper modules for this, but nothing command-specific should make it into the main module. As long as the command interface (see below) is followed, a new command doesn't even have to be mentioned by name within the main module.

* *Separation of Concerns Before DRYness*: As long as it's possible to implement a command in a single module, it will be easy for new developers to extend the CLI, even if they don't know Elixir that well. A certain amount of repetition across modules is a small price to pay for this. We can always refactor later.


## Boundaries
* The main module is responsible for the flow of the program. Anything that happens on any given execution of `rabbitmqctl` belongs in the main function. Currently its responsibilities are:
	* parsing input (using `OptionParser`)
	* auto-filling some default options
	* execute commands using `eval_string`
	* return specific exit codes based on output from the commands

* The individual modules are responsible for any behavior unique to the command:
	* Determining via pattern-matching whether the user-defined arguments and options are valid
	* Interacting with the RabbitMQ server via RPC calls
	* Returning particular outputs based on results from the server
	* Printing any output associated with the command
	* Providing any usage information via a `usage/0` method.

## Things The CLI Is Not Responsible For:

* *Server behavior*: The CLI passes information to the server and triggers actions within it via RPC calls. It's up to the server to handle those calls properly. If the CLI passes valid credentials to RabbitMQ and RabbitMQ returns `{:refused, ...} `, the CLI has still functioned correctly.

* *Authentication/authorization*: RabbitMQ-CLI does not prompt users for a password, nor does it use any user credentials to authorize an operation. Any restriction on what a given user can do with the CLI comes from the targeted RabbitMQ broker, not the CLI itself. If you want a command that requires authorization, this is probably not the tool for you.
