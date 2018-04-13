Personal Python installation

If you have troubles with installing modules to system-wide Python you can build your own one. A new "python" executable will be reused then in you virtual environments.

Here are the steps to buils it:

wget https://www.python.org/ftp/python/2.7.14/Python-2.7.14.tgz
tar zxf Python-2.7.14.tgz
cd Python-2.7.14
./configure —prefix=${HOME}/bin/python-2.7.14 --with-ensurepip
make all install
alias pip='~/bin/python-2.7.14/bin/pip'
alias python='~/bin/python-2.7.14/bin/python'
pip install pip --upgrade
pip install --user virtualenv

Check that new python has sqlite modules:

python -c 'import sqlite'

If it does not and you you do not have sqlite-dev package installed:

wget https://sqlite.org/2018/sqlite-autoconf-3220000.tar.gz
rm -rf sqlite-amalgamation-3220000
tar zxf sqlite-autoconf-3220000.tar.gz
cd sqlite-autoconf-3220000
./configure --prefix=${HOME}/bin/sqlite
make
make install
export CPPFLAGS=-I${HOME}/bin/sqlite/include
export LDFLAGS=-L${HOME}/bin/sqlite/lib
cd ../Python-2.7.14
./configure --prefix=${HOME}/bin/python-2.7.14 --with-ensurepip
make
make install
alias pip='~/bin/python-2.7.14/bin/pip'
alias python='~/bin/python-2.7.14/bin/python'
pip install pip --upgrade
pip install --user virtualenv
