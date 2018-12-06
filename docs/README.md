# Manual Pages and Documentation Extras

This directory contains [CLI tool](https://rabbitmq.com/cli.html) man page sources as well as a few documentation extras:

 * An [annotated rabbitmq.conf example](./rabbitmq.conf.example) in the [new style configuration format](https://rabbitmq.com/configure.html)
 * An [annotated advanced.config example](./advanced.config.example) to accompany `rabbitmq.conf.example`
 * An [annotated rabbitmq.config example](./rabbitmq.config.example) in the classic configuration format
 * A [systemd unit file example](./rabbitmq-server.service.example)

Please [see rabbitmq.com](https://rabbitmq.com/documentation.html) for documentation guides.

## man Pages

### Source Files

This directory contains man pages that are are converted to HTML using `mandoc`:

    gmake web-manpages

The result is then copied to the [website repository](https://github.com/rabbitmq/rabbitmq-website/tree/live/site/man)

### Contributions

Since deployed man pages are generated, it is important to keep them in sync with the source.
Accepting community contributions — which will always come as website pull requests —
is fine but the person who merges them is responsible for backporting all changes
to the source pages in this repo.
