# Manual Pages and Documentation Extras

This directory contains [CLI tool](https://rabbitmq.com/cli.html) man page sources as well as a few documentation extras:

 * An [annotated rabbitmq.conf example](./rabbitmq.conf.example) (see [new style configuration format](https://www.rabbitmq.com/configure.html#config-file-formats))
 * An [annotated advanced.config example](./advanced.config.example) (see [The advanced.config file](https://www.rabbitmq.com/configure.html#advanced-config-file))
 * A [systemd unit file example](./rabbitmq-server.service.example)

Please [see rabbitmq.com](https://rabbitmq.com/documentation.html) for documentation guides.


## Classic Config File Format Example

Feeling nostalgic and looking for the [classic configuration file example](https://github.com/rabbitmq/rabbitmq-server/blob/v3.7.x/docs/rabbitmq.config.example)?
Now that's old school! Keep in mind that classic configuration file **should be considered deprecated**.
Prefer `rabbitmq.conf` (see [new style configuration format](https://www.rabbitmq.com/configure.html#config-file-formats))
with an `advanced.config` to complement it as needed.


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
