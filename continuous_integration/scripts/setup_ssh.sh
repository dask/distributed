# Enable `ssh localhost` without a password.
# If you change this script, make sure to retest both on Ubuntu and on MacOSX!
# See .github/workflows/ssh_debug.yaml

set -o errexit
set -o nounset
set -o xtrace

mkdir -p ~/.ssh
chmod 700 ~ ~/.ssh
ssh-keygen -t rsa -f ~/.ssh/id_rsa -N "" -q
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
ssh-keyscan -H localhost 127.0.0.1 $(hostname) >> ~/.ssh/known_hosts
