# On OS X builders, the default open files limit is too small (256)
if [[ $TRAVIS_OS_NAME == osx ]]; then
    ulimit -n 8192
fi

echo "--"
echo "-- Soft limits"
echo "--"
ulimit -a -S

echo "--"
echo "-- Hard limits"
echo "--"
ulimit -a -H

py.test -m "not avoid_travis" distributed --verbose -r s --timeout-method=thread --timeout=300 --durations=20 -x distributed/comm
