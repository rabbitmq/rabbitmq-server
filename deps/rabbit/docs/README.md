# Manual Pages and Documentation Extras

This directory contains [CLI tools](https://rabbitmq.com/docs/cli/) man page sources as well as a few documentation extras:

 * An [annotated rabbitmq.conf example](./rabbitmq.conf.example) (see [new style configuration format](https://www.rabbitmq.com/docs/configure#config-file-formats))
 * An [annotated advanced.config example](./advanced.config.example) (see [The advanced.config file](https://www.rabbitmq.com/docs/configure#advanced-config-file))
 * A [systemd unit file example](./rabbitmq-server.service.example)

Please [see rabbitmq.com](https://rabbitmq.com/docs/) for documentation guides.


## man Pages

### Dependencies

 * `man`
 * [`tidy5`](https://binaries.html-tidy.org/) (a.k.a. `tidy-html5`)

On macOS, `tidy5` can be installed with Homebrew:

``` shell
brew install tidy-html5
```

and then be found under the `bin` directory of the Homebrew cellar:

``` shell
/opt/homebrew/bin/tidy --help
```

### Source Files

This directory contains man pages in ntroff, the man page format.

To inspect a local version, use `man`:

``` shell
man docs/rabbitmq-diagnostics.8

man docs/rabbitmq-queues.8
```

To converted all man pages to HTML using `mandoc`:

``` shell
gmake web-manpages
```

The result then must be post-processed and copied to the website repository:

``` shell
# cd deps/rabbit/docs
#
# clear all generated HTML and Markdown files
rm *.html *.md
# export tidy5 path
export TIDY5_BIN=/opt/homebrew/bin/tidy;
# run the post-processing script, in this case it updates the 3.13.x version of the docs
./postprocess_man_html.sh . /path/to/rabbitmq-website.git/versioned_docs/version-3.13/man/
```

### Contributions

Since deployed man pages are generated, it is important to keep them in sync with the source.
Accepting community contributions — which will always come as website pull requests —
is fine but the person who merges them is responsible for backporting all changes
to the source pages in this repo.
