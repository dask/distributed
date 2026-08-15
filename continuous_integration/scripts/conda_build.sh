#!/bin/bash
set -o errexit
set -o pipefail
set -o nounset
set -o xtrace

export VERSION_SUFFIX=a$(date +%y%m%d)
export DASK_CORE_VERSION=$(conda search --override-channels -c dask/label/dev dask-core | tail -n 1 | awk '{print $2}')

conda build continuous_integration/recipes/distributed \
  --channel conda-forge --channel dask/label/dev --override-channels --output-folder dist/conda
conda build continuous_integration/recipes/dask \
  --channel conda-forge --channel dask/label/dev --override-channels --output-folder dist/conda

echo "Generated conda repository:"
find dist/conda -type f
