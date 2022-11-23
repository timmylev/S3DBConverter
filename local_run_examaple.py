from lambdas import common
from tqdm import tqdm

dest_prefix = "version5/arrow/zst"

# all
# keys = list(common.list_source_keys())

# only nyiso
keys = list(common.list_source_keys("nyiso"))

for k in tqdm(keys):
    common.migrate_file(k, dest_prefix)
