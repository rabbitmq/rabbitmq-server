Rabbit Trust-Store
=====

TLS certificate whitelisting.

Rational
-----

This plugin whitelists the .PEM formatted TLS certificates in a given
directory, refreshing at configurable intervals, or when `rabbitmqct
eval 'rabbit_trust_store:refresh().'` is invoked.

While Rabbit can be configured to accepted self-signed certificates
through various SSL socket options, namely the `ca_certs` and
`partial_chain` properties, this configuration is somewhat static.
There is no convenient means with which to change it in realtime, that
is, without making configuration changes to SSL listening sockets.

How
-----

Configure the trust-store with a directory of whitelisted certificates
and a refresh interval:

```
    {rabbitmq_trust_store,
     [{directory, "$HOME/rabbit/whitelist"}, %% DEFAULT PATH.
      {interval,  30}                        %% INTERPRETED AS SECONDS!
    ]}
```

The configuration data for manual refresh is `{interval, 0}`.

When the trust-store starts it'll whitelist the certificates in the
given directory, then install and remove certificate details which are
written-to and deleted-from the directory, respectively after the
given refresh time.
