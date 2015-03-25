!#/bin/sh

if (! (sudo dpkg-query -Wf'${db:Status-abbrev}' emacs  | grep -q '^i')); then
    echo oracle-java8-installer shared/accepted-oracle-license-v1-1 select true | sudo /usr/bin/debconf-set-selections
    sudo apt-get -qy update
    sudo apt-get install software-properties-common -y
    sudo add-apt-repository ppa:ubuntu-elisp/ppa
    sudo add-apt-repository ppa:webupd8team/java
    sudo apt-get update
    sudo apt-get install git emacs-snapshot-nox oracle-java8-installer -y
else
    echo System has emacs24
fi

if [ ! -d /home/vagrant/.emacs.d ]; then
    git clone git://github.com/bbatsov/prelude.git /home/vagrant/.emacs.d
else
    echo System has .emacs
fi

if (! (/home/vagrant/bin/lein version 2> /dev/null | grep -q 'Leiningen')); then
    echo 'Installing Leiningen'
    mkdir -p /home/vagrant/bin
    sed '$ a\

    export PATH=/home/vagrant/bin:$PATH' /home/vagrant/.bashrc >/dev/null 2>&1
    wget -O /home/vagrant/bin/lein https://raw.github.com/technomancy/leiningen/stable/bin/lein 2>&1
    chmod +x /home/vagrant/bin/lein
    /home/vagrant/bin/lein self-install
else
    echo System has Leiningen installed
fi
