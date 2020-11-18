# Build RabbitMQ Packages using Linux


## Environment

Debian Jesse using this `Vagrantfile`:

```
$script = <<SCRIPT
export LANG='C.UTF-8'
export DEBIAN_FRONTEND=noninteractive

echo 'deb http://cdn-fastly.deb.debian.org/debian jessie-backports main' >> /etc/apt/sources.list.d/backports.list

wget https://packages.erlang-solutions.com/erlang-solutions_1.0_all.deb
dpkg -i erlang-solutions_1.0_all.deb

apt-get clean
apt-get update
apt-get install -y --fix-missing --no-install-recommends \
	build-essential \
	ca-certificates \
	debhelper \
	dh-systemd \
	elinks \
	esl-erlang \
	elixir \
	fakeroot \
	git \
	libfile-fcntllock-perl \
	mandoc \
	nsis \
	python-lxml \
	python-markdown \
	python-simplejson \
	rpm \
	rsync \
	tofrodos \
	unzip \
	xmlto \
	xsltproc \
	zip \
	curl
date > /etc/vagrant_provisioned_at
SCRIPT

Vagrant.configure('2') do |config|
  config.vm.box = "debian/jessie64"
  config.vm.hostname = 'DEBIAN-JESSIE64'
  config.vm.provision 'shell', inline: $script
end
```

## Instructions

Bring up a Debian Jesse instance using Vagrant, or, use the provisioning script
on a Debian Jesse server of your own. When it is done running, all necessary
package build requirements for either `apt`-based or `rpm`-based distros will
be present. See the [`README.md`](README.md#tldr) document for instructions on
building packages.
