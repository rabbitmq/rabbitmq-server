# Protocol extensions

The `amqp_codegen.py` AMQP specification compiler has recently been
enhanced to take more than a single specification file, which allows
AMQP library authors to include extensions to the core protocol
without needing to modify the core AMQP specification file as
distributed.

The compiler is invoked with the path to a single "main" specification
document and zero or more paths to "extension" documents.

The order of the extensions matters: any later class property
definitions, for instance, are added to the list of definitions in
order of appearance. In general, composition of extensions with a core
specification document is therefore non-commutative.

## The main document

Written in the style of a
[json-shapes](http://github.com/tonyg/json-shapes) schema:

    DomainDefinition = _and(array_of(string()), array_length_equals(2));

    ConstantDefinition = {
        "name": string(),
        "value": anything(),
        "class": optional(_or("soft-error", "hard-error"))
    };

    FieldDefinition = {
        "name": string(),
        "type": string(),
        "default-value": optional(anything())
    };

    MethodDefinition = {
        "name": string(),
        "id": number(),
        "arguments": array_of(FieldDefinition),
        "synchronous": optional(boolean())
    };

    ClassDefinition = {
        "name": string(),
        "id": number(),
        "methods": array_of(MethodDefinition),
        "properties": optional(array_of(FieldDefinition))
    };

    MainDocument = {
        "major-version": number(),
        "minor-version": number(),
        "revision": optional(number()),
        "port": number(),
        "domains": array_of(DomainDefinition),
        "constants": array_of(ConstantDefinition),
        "classes": array_of(ClassDefinition),
    }

Within a `FieldDefinition`, the keyword `domain` can be used instead
of `type`, but `type` is preferred and `domain` is deprecated.

## Extensions

Written in the style of a
[json-shapes](http://github.com/tonyg/json-shapes) schema, and
referencing some of the type definitions given above:

    ExtensionDocument = {
        "extension": anything(),
        "domains": array_of(DomainDefinition),
        "constants": array_of(ConstantDefinition),
        "classes": array_of(ClassDefinition)
    };

The `extension` keyword is used to describe the extension informally
for human readers. Typically it will be a dictionary, with members
such as:

    {
        "name": "The name of the extension",
        "version": "1.0",
        "copyright": "Copyright (C) 1234 Yoyodyne, Inc."
    }

## Merge behaviour

In the case of conflicts between values specified in the main document
and in any extension documents, type-specific merge operators are
invoked.

 - Any doubly-defined domain names are regarded as true
   conflicts. Otherwise, all the domain definitions from all the main
   and extension documents supplied to the compiler are merged into a
   single dictionary.

 - Constant definitions are treated as per domain names above,
   *mutatis mutandis*.

 - Classes and their methods are a little trickier: if an extension
   defines a class with the same name as one previously defined, then
   only the `methods` and `properties` fields of the extension's class
   definition are attended to.

    - Any doubly-defined method names or property names within a class
      are treated as true conflicts.

    - Properties defined in an extension are added to the end of the
      extant property list for the class.

   (Extensions are of course permitted to define brand new classes as
   well as to extend existing ones.)

 - Any other kind of conflict leads to a raised
   `AmqpSpecFileMergeConflict` exception.

## Invoking the spec compiler

Your code generation code should invoke `amqp_codegen.do_main` with
two functions as arguments: one for generating "header-file" text, and
one for generating "implementation-file" text. The `do_main` function
will parse the command-line arguments supplied when python was
invoked.

The command-line will be parsed as:

    python your_codegen.py <headerorbody> <mainspec> [<extspec> ...] <outfile>

where `<headerorbody>` is either the word `header` or the word `body`,
to select which generation function is called by `do_main`. The
`<mainspec>` and `<extspec>` arguments are file names of specification
documents containing expressions in the syntax given above. The
*final* argument on the command line, `<outfile>`, is the name of the
source-code file to generate.

Here's a tiny example of the layout of a code generation module that
uses `amqp_codegen`:

    import amqp_codegen

    def generateHeader(specPath):
        spec = amqp_codegen.AmqpSpec(specPath)
        ...

    def generateImpl(specPath):
        spec = amqp_codegen.AmqpSpec(specPath)
        ...

    if __name__ == "__main__":
        amqp_codegen.do_main(generateHeader, generateImpl)

The reasons for this split, such as they are, are that

 - many languages have separate "header"-type files (C and Erlang, to
   name two)
 - `Makefile`s often require separate rules for generating the two
   kinds of file, but it's convenient to keep the generation code
   together in a single python module

The main reason things are laid out this way, however, is simply that
it's an accident of the history of the code. We may change the API to
`amqp_codegen` in future to clean things up a little.
