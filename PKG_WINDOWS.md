# Build RabbitMQ Windows Package using Windows

## Environment

Windows 8.1 using this `Vagrantfile`:

```
Vagrant.configure("2") do |config|
  config.vm.box = "inclusivedesign/windows81-eval-x64"
  config.vm.provider "virtualbox" do |v|
    v.gui = true
  end
end
```

Note that these steps should work on more recent versions of Windows as well.
If you have issues using a newer version of Windows, please provide full
details in a message to the
[`rabbitmq-users`](https://groups.google.com/forum/#!forum/rabbitmq-users)
mailing list.

## Initial Steps

Bring up the VM and go through the process of updating Windows and (optionall)
VirtualBox tools. You'll notice that the evaluation license is expired. Re-arm
it by running this command via an administrative prompt:

```
slmgr -rearm
```

This part of the process will take a while as Windows is updated. Go make some
coffee and check your email.

## Install Erlang and Elixir

Using Chocolatey is the easiest method to install the most recent version of
Erlang and Elixir. Install Chocolatey [using these
instructions](https://chocolatey.org/install#installing-chocolatey)
([link](https://chocolatey.org/install#installing-chocolatey)), then install
both Erlang and Elixir using this command from an *administrative* `cmd.exe` or
Powershell terminal:

```
choco install elixir which
```

To confirm installation, open a new command prompt and run the following:

```
erl -version
elixir -v
which erl
which elixir
```

## Install MSYS2 and NSIS

Chocolatey is also an easy way to install `msys2` and the NSIS install script
builder. Run the following from an *administrative* command prompt:

```
choco install msys2 nsis
```

As a bonus, it will update your `msys2` installation for you during the initial
install process.

## Install MSYS2 packages

Start up an `msys2` shell by running the following command (does not have to be
admin):

```
C:\tools\msys64\msys2_shell.cmd
```

In that shell, install all of these dependencies. If you are prompted for
input, just hit ENTER to choose the default:

```
pacman -S --needed git make tar rsync python zip unzip dos2unix man
```

## Build RabbitMQ

### Clone this repository

From within your MSYS2 shell:

```
git clone https://github.com/rabbitmq/rabbitmq-server-release.git
```

### Set `PATH`

```
export PATH="$PATH:/c/ProgramData/Chocolatey/bin:/c/ProgramData/Chocolatey/lib/Elixir/bin:/c/Program Files (x86)/NSIS/bin"
```

### Fetch and build deps

*Note:* as of this writing, RabbitMQ `3.7.8` is the latest version. Be sure to check out the tag appropriate for your use:

```
cd rabbitmq-server-release

# the following checks out the "next to be released" branch
# this branch and version 3.7.9 have a necessary fix for building
# on windows, see this: https://github.com/rabbitmq/rabbitmq-server-release/pull/89

git checkout v3.7.x

make deps
```

### Build Windows package

```
make UNIX_TO_DOS=unix2dos package-windows
```
