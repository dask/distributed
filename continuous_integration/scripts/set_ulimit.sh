# On OS X builders, the default open files limit is too small (256)
if [[ "$OSTYPE" == "darwin"* ]]; then
    ulimit -n 8192
fi

echo "-- Soft limits"
ulimit -a -S
echo "-- Hard limits"
ulimit -a -H
