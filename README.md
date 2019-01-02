## rawRsync
This utility was designed to improve rsync performance on non-optimal structures.

### Use cases
- Many small files
- Deep folder structure
- Unstable connection
- Requires resume capabilities

#### Usage
python3 rawrsync.py -s 'source_root' -d 'target_root'

##### TODO
- [x] Improve logging
- [x] Improve reporting
- [x] Add UI options
