# Install distributed
python -m pip install --no-deps -e .

# For debugging
echo -e "--\n--Conda Environment (re-create this with \`conda env create --name <name> -f <output_file>\`)\n--"
conda env export | grep -E -v '^prefix:.*$'

echo -e "--\n--Pip Environment\n--"
python -m pip list --format=columns
