# -*- mode: ruby -*-
VAGRANTFILE_API_VERSION = "2"

Vagrant.configure(VAGRANTFILE_API_VERSION) do |config|
  config.vm.box = "phusion/ubuntu-14.04-amd64"
  config.vm.provision :shell, :path => 'script/setup_vm.sh', :privileged => false
  config.ssh.forward_agent = true
end
