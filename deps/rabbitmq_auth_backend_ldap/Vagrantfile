# -*- mode: ruby -*-
# vi: set ft=ruby :

Vagrant.configure(2) do |config|
  config.vm.box = 'ubuntu/bionic64'
  config.vm.network "forwarded_port", guest: 389, host: 3890
  config.vm.provision "shell", inline: "sudo apt-get -y update"
  config.vm.provision "file", source: "example", destination: "~/example"
  config.vm.provision "shell", inline: "/bin/sh /home/vagrant/example/setup.sh"
end
