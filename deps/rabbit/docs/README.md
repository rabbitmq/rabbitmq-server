# Manual Pages and Documentation Extras

This directory contains [CLI tools](https://rabbitmq.com/docs/cli/) man page sources as well as a few documentation extras:

 * An [annotated rabbitmq.conf example](./rabbitmq.conf.example) (see [new style configuration format](https://www.rabbitmq.com/docs/configure#config-file-formats))
 * An [annotated advanced.config example](./advanced.config.example) (see [The advanced.config file](https://www.rabbitmq.com/docs/configure#advanced-config-file))
 * A [systemd unit file example](./rabbitmq-server.service.example)

Please [see rabbitmq.com](https://rabbitmq.com/docs/) for documentation guides.




## man Pages

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

The result is then copied to the [website repository](https://github.com/rabbitmq/rabbitmq-website/tree/live/site/man)

### Contributions

Since deployed man pages are generated, it is important to keep them in sync with the source.
Accepting community contributions — which will always come as website pull requests —
is fine but the person who merges them is responsible for backporting all changes
to the source pages in this repo.
