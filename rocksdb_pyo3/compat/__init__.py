from enum import Enum
from abc import abstractmethod, ABC
from functools import cached_property
from .. import rocksdb_pyo3 as _rocksdb_pyo3
from . import errors

Snapshot = _rocksdb_pyo3.Snapshot


def _prop(setter):
    return property(None, setter)


class CompressionType(Enum):
    no_compression = b'no_compression'
    snappy_compression = b'snappy_compression'
    zlib_compression = b'zlib_compression'
    bzip2_compression = b'bzip2_compression'
    lz4_compression = b'lz4_compression'
    lz4hc_compression = b'lz4hc_compression'
    zstd_compression = b'zstd_compression'
    xpress_compression = b'xpress_compression'
    zstdnotfinal_compression = b'zstdnotfinal_compression'
    disable_compression = b'disable_compression'

    def _to_internal(self) -> _rocksdb_pyo3.DBCompressionType:
        match self:
            case CompressionType.no_compression:
                return _rocksdb_pyo3.DBCompressionType.NO
            case CompressionType.snappy_compression:
                return _rocksdb_pyo3.DBCompressionType.SNAPPY
            case CompressionType.zlib_compression:
                return _rocksdb_pyo3.DBCompressionType.ZLIB
            case CompressionType.bzip2_compression:
                return _rocksdb_pyo3.DBCompressionType.BZ2
            case CompressionType.lz4_compression:
                return _rocksdb_pyo3.DBCompressionType.LZ4
            case CompressionType.lz4hc_compression:
                return _rocksdb_pyo3.DBCompressionType.LZ4HC
            case CompressionType.zstd_compression:
                return _rocksdb_pyo3.DBCompressionType.ZSTD
            case CompressionType.xpress_compression:
                raise NotImplementedError
            case CompressionType.zstdnotfinal_compression:
                raise NotImplementedError
            case CompressionType.disable_compression:
                raise NotImplementedError
            case _:
                raise ValueError('not a valid enum')


class ColumnFamilyOptions:
    __slots__ = ('_opts',)
    _opts: _rocksdb_pyo3.Options

    def __init__(self, **kwargs):
        self._opts = _rocksdb_pyo3.Options()
        for key, value in kwargs.items():
            setattr(self, key, value)

    @_prop
    def write_buffer_size(self, value):
        self._opts.set_write_buffer_size(value)

    @_prop
    def compression(self, value):
        self._opts.set_compression_type(value._to_internal())

    #property table_factory:

    # TODO:
    #property max_write_buffer_number:
    #property min_write_buffer_number_to_merge:
    #property compression_opts:
    #property compaction_pri:
    #property max_compaction_bytes:
    #property num_levels:
    #property level0_file_num_compaction_trigger:
    #property level0_slowdown_writes_trigger:
    #property level0_stop_writes_trigger:
    #property target_file_size_base:
    #property target_file_size_multiplier:
    #property max_bytes_for_level_base:
    #property max_bytes_for_level_multiplier:
    #property max_bytes_for_level_multiplier_additional:
    #property arena_block_size:
    #property disable_auto_compactions:
    #property compaction_style:
    #property compaction_options_universal:
    #property max_sequential_skip_in_iterations:
    #property inplace_update_support:
    #property memtable_factory:
    #property inplace_update_num_locks:
    #property comparator:
    #property merge_operator:
    #property prefix_extractor:
    #property optimize_filters_for_hits:
    #property paranoid_file_checks:


class Options(ColumnFamilyOptions):
    @_prop
    def create_if_missing(self, value):
        self._opts.create_if_missing(value)

    @_prop
    def error_if_exists(self, value):
        self._opts.set_error_if_exists(value)

    @_prop
    def allow_mmap_reads(self, value):
        self._opts.set_allow_mmap_reads(value)

    @_prop
    def allow_mmap_writes(self, value):
        self._opts.set_allow_mmap_writes(value)

    # TODO:
    #def IncreaseParallelism(self, int total_threads=16):
    #property create_missing_column_families:
    #property paranoid_checks:
    #property max_open_files:
    #property use_fsync:
    #property db_log_dir:
    #property wal_dir:
    #property delete_obsolete_files_period_micros:
    #property max_background_compactions:
    #property max_background_jobs:
    #property max_background_flushes:
    #property max_log_file_size:
    #property log_file_time_to_roll:
    #property keep_log_file_num:
    #property max_manifest_file_size:
    #property table_cache_numshardbits:
    #property wal_ttl_seconds:
    #property wal_size_limit_mb:
    #property manifest_preallocation_size:
    #property enable_write_thread_adaptive_yield:
    #property allow_concurrent_memtable_write:
    #property is_fd_close_on_exec:
    #property stats_dump_period_sec:
    #property advise_random_on_open:
    #property use_adaptive_mutex:
    #property bytes_per_sync:
    #property row_cache:


class ColumnFamilyHandle:
    __slots__ = ('_handle', '_name', '_id')
    _handle: _rocksdb_pyo3.ColumnFamilyHandle | None
    _name: bytes
    _id: int

    def __init__(self, handle: _rocksdb_pyo3.ColumnFamilyHandle):
        assert handle is not None
        self._handle = handle
        self._name = handle.get_name().encode('utf-8')
        self._id = handle.get_id()

    def _invalidate(self) -> None:
        del self._handle
        self._handle = None

    @property
    def _state(self) -> str:
        if self.is_valid:
            return 'valid'
        else:
            return 'invalid'

    def __repr__(self) -> str:
        return f'<ColumnFamilyHandle name: {self._name}, id: {self._id}, state: {self._state}>'

    def __str__(self) -> str:
        return repr(self)

    @property
    def id(self) -> int:
        return self._id

    @property
    def name(self) -> bytes:
        return self._name

    @property
    def is_valid(self) -> str:
        return self._handle is not None


Key = bytes | tuple[ColumnFamilyHandle, bytes]


def list_column_families(db_name: str, opts: Options) -> list[bytes]:
    return list(cf_name.encode('utf-8') for cf_name in _rocksdb_pyo3.DB.list_cf(opts._opts, db_name))


def repair_db(db_name: str, opts: Options) -> None:
    _rocksdb_pyo3.DB.repair(opts._opts, db_name)


class _BaseIterator(ABC):
    __slots__ = ('_iterator', '_cf')
    _iterator: _rocksdb_pyo3.DBIterator
    _cf: ColumnFamilyHandle | None

    def __init__(self, iterator: _rocksdb_pyo3.DBIterator, cf: ColumnFamilyHandle | None = None):
        self._iterator = iterator
        self._cf = cf

    @abstractmethod
    def _get_object(self):
        raise NotImplementedError

    def _advance(self):
        self._iterator.next()

    def __iter__(self):
        return self

    def __next__(self):
        if not self._iterator.valid():
            raise StopIteration()
        ret = self._get_object()
        self._advance()
        self._iterator.status()
        return ret

    def get(self):
        return self._get_object()

    def seek_to_first(self):
        self._iterator.seek_to_first()
        self._iterator.status()

    def seek_to_last(self):
        self._iterator.seek_to_last()
        self._iterator.status()

    def seek(self, key: bytes):
        self._iterator.seek(key)
        self._iterator.status()

    def seek_for_prev(self, key: bytes):
        self._iterator.seek_for_prev(key)
        self._iterator.status()


class _ReversedBase:
    def _advance(self):
        self._iterator.prev()


class KeysIterator(_BaseIterator):
    def __reversed__(self):
        return ReversedKeysIterator(self._iterator, self._cf)

    def _get_object(self):
        k = self._iterator.key()
        if k is None:
            return None
        if self._cf is not None:
            return self._cf, k
        else:
            return k


class ReversedKeysIterator(_ReversedBase, KeysIterator):
    def __reversed__(self):
        return KeysIterator(self._iterator, self._cf)


class ValuesIterator(_BaseIterator):
    def __reversed__(self):
        return ReversedValuesIterator(self._iterator, self._cf)

    def _get_object(self):
        v = self._iterator.value()
        if v is None:
            return None
        else:
            return v


class ReversedValuesIterator(_ReversedBase, ValuesIterator):
    def __reversed__(self):
        return ValuesIterator(self._iterator, self._cf)


class ItemsIterator(_BaseIterator):
    def __reversed__(self):
        return ReversedItemsIterator(self._iterator, self._cf)

    def _get_object(self):
        k, v = self._iterator.item()
        if k is None or v is None:
            return None
        if self._cf is not None:
            return (self._cf, k), v
        else:
            return k, v


class ReversedItemsIterator(_ReversedBase, ItemsIterator):
    def __reversed__(self):
        return ItemsIterator(self._iterator, self._cf)


class WriteBatch:
    __slots__ = ('_batch',)
    _batch: _rocksdb_pyo3.WriteBatch

    def __init__(self, data: bytes | None = None):
        if data is not None:
            self._batch = _rocksdb_pyo3.WriteBatch.from_data(data)
        else:
            self._batch = _rocksdb_pyo3.WriteBatch()

    def put(self, key: Key, value: bytes) -> None:
        match key:
            case (cf, k):
                self._batch.put_cf(cf._handle, k, value)
            case _:
                self._batch.put(key, value)

    def merge(self, key: Key, value: bytes) -> None:
        match key:
            case (cf, k):
                self._batch.merge_cf(cf._handle, k, value)
            case _:
                self._batch.merge(key, value)

    def delete(self, key: Key) -> None:
        match key:
            case (cf, k):
                self._batch.delete_cf(cf._handle, k)
            case _:
                self._batch.delete(key)

    def clear(self):
        self._batch.clear()

    def data(self):
        return self._batch.data()

    def count(self):
        return self._batch.len()

    #def __iter__(self):
    #    pass


class DB:
    _db: _rocksdb_pyo3.DB
    _opts: Options

    def __init__(self, db_name: str, opts: Options | None = None, column_families: dict[bytes, ColumnFamilyOptions] | None = None, read_only: bool = False):
        match opts, column_families, read_only:
            case None, None, False:
                db = _rocksdb_pyo3.DB.open(db_name)
            case None, _, False:
                db = _rocksdb_pyo3.DB.open_cf_with_opts(db_name, Options(), _map_cfs(_ensure_default_cf(column_families)))
            case None, _, True:
                db = _rocksdb_pyo3.DB.open_cf_with_opts_for_read_only(db_name, Options(), _map_cfs(_ensure_default_cf(column_families)), False)
            case _, None, False:
                db = _rocksdb_pyo3.DB.open_with_opts(db_name, opts._opts)
            case _, _, False:
                db = _rocksdb_pyo3.DB.open_cf_with_opts(db_name, opts._opts, _map_cfs(_ensure_default_cf(column_families)))
            case _, _, True:
                db = _rocksdb_pyo3.DB.open_cf_with_opts_for_read_only(db_name, opts._opts, _map_cfs(_ensure_default_cf(column_families)), False)
        self._db = db
        if opts is not None:
            self._opts = opts
        else:
            self._opts = Options()

    @property
    def options(self) -> Options:
        return self._opts

    @property
    def column_families(self) -> list[ColumnFamilyHandle]:
        return [ColumnFamilyHandle(cf) for cf in self._db.cf_handles()]

    @cached_property
    def _default_column_family(self) -> ColumnFamilyHandle:
        return self.get_column_family(b'default')

    def close(self, safe: bool = True) -> None:
        if not hasattr(self, '_db'):
            # XXX: already closed
            return
        self._db.cancel_all_background_work(safe)
        self._db.close()
        del self._db

    def get_column_family(self, name: bytes) -> ColumnFamilyHandle | None:
        sname = name.decode('utf-8')
        cf = self._db.cf_handle(sname)
        if cf is not None:
            return ColumnFamilyHandle(cf)
        else:
            return cf

    def create_column_family(self, name: bytes, copts: ColumnFamilyOptions) -> ColumnFamilyHandle:
        sname = name.decode('utf-8')
        self._db.create_cf(sname, copts._opts)
        cf = self._db.cf_handle(sname)
        assert cf is not None
        return ColumnFamilyHandle(cf)

    def drop_column_family(self, weak_handle: ColumnFamilyHandle) -> None:
        if weak_handle.name == b'default':
            raise ValueError('cannot drop default column family')
        self._db.drop_cf(weak_handle._name.decode('utf-8'))
        weak_handle._invalidate()

    def get(self, key: Key, *args, **kwargs) -> bytes | None:
        readopts = _readopts(*args, **kwargs)
        match key, readopts:
            case (cf, k), None:
                return self._db.get_cf(cf._handle, k)
            case (cf, k), _:
                return self._db.get_cf_opt(cf._handle, k, readopts)
            case _, None:
                return self._db.get(key)
            case _, _:
                return self._db.get_opt(key, readopts)

    def put(self, key: Key, value: bytes, *args, **kwargs) -> None:
        writeopts = _writeopts(*args, **kwargs)
        match key, writeopts:
            case (cf, k), None:
                self._db.put_cf(cf._handle, k, value)
            case (cf, k), _:
                self._db.put_cf_opt(cf._handle, k, value, writeopts)
            case _, None:
                self._db.put(key, value)
            case _, _:
                self._db.put_opt(key, value, writeopts)

    def merge(self, key: Key, value: bytes, *args, **kwargs) -> None:
        writeopts = _writeopts(*args, **kwargs)
        match key, writeopts:
            case (cf, k), None:
                self._db.merge_cf(cf._handle, k, value)
            case (cf, k), _:
                self._db.merge_cf_opt(cf._handle, k, value, writeopts)
            case _, None:
                self._db.merge(key, value)
            case _, _:
                self._db.merge_opt(key, value, writeopts)

    def delete(self, key: Key, *args, **kwargs) -> None:
        writeopts = _writeopts(*args, **kwargs)
        match key, writeopts:
            case (cf, k), None:
                self._db.delete_cf(cf._handle, k)
            case (cf, k), _:
                self._db.delete_cf_opt(cf._handle, k, writeopts)
            case _, None:
                self._db.delete(key, value)
            case _, _:
                self._db.delete_opt(key, writeopts)

    def snapshot(self) -> Snapshot:
        return self._db.snapshot()

    def get_live_files_metadata(self) -> list[dict]:
        return [{
            'name': lf.name,
            'level': lf.level,
            'size': lf.size,
            'smallestkey': lf.start_key,
            'largestkey': lf.end_key,
            # not available:
            #'smallest_seqno': 0,
            #'largest_seqno': 0,
        } for lf in self._db.live_files()]

    def get_column_family_meta_data(self, column_family: ColumnFamilyHandle | None = None) -> dict:
        cfmd: _rocksdb_pyo3.ColumnFamilyMetaData
        if column_family is not None:
            cfmd = self._db.get_column_family_metadata_cf(column_family._handle)
        else:
            cfmd = self._db.get_column_family_metadata()
        return {
            'size': cfmd.size,
            'file_count': cfmd.file_count,
        }

    def get_property(self, prop: bytes, column_family: ColumnFamilyHandle | None = None) -> bytes | None:
        sprop = prop.decode('utf-8')
        value: Option[str]
        if column_family is not None:
            value = self._db.property_value_cf(column_family._handle, sprop)
        else:
            value = self._db.property_value(sprop)
        if value is None:
            return None
        else:
            return value.encode('utf-8')

    def write(self, batch: WriteBatch, *args, **kwargs) -> None:
        writeopts = _writeopts(*args, **kwargs)
        if writeopts is not None:
            self._db.write_opt(batch._batch, writeopts)
        else:
            self._db.write(batch._batch)

    def compact_range(self, begin: bytes | None = None, end: bytes | None = None, column_family: ColumnFamilyHandle | None = None, **py_options) -> None:
        compactopts = _compactopts(**py_options)
        match column_family, compactopts:
            case None, None:
                self._db.compact_range(begin, end)
            case None, _:
                self._db.compact_range_opt(begin, end, compactopts)
            case _, None:
                self._db.compact_range_cf(column_family._handle, begin, end)
            case _, _:
                self._db.compact_range_cf_opt(column_family._handle, begin, end, compactopts)

    def _ensure_key_cf(self, key: Key) -> tuple[ColumnFamilyHandle, bytes]:
        match key:
            case cf, k:
                return cf, k
            case _:
                return self._default_column_family, key

    def key_may_exist(self, key: Key, fetch: bool = False, *args, **kwargs) -> tuple[bool, bytes | None]:
        readopts = _readopts(*args, **kwargs)
        if fetch:
            cf, k = self._ensure_key_cf(key)
            return self._db.key_may_exist_cf_opt_value(cf._handle, k, _ensure_readopts(readopts))
        else:
            match key, readopts:
                case (cf, k), None:
                    return self._db.key_may_exist_cf(cf._handle, k), None
                case (cf, k), _:
                    return self._db.key_may_exist_cf_opt(cf._handle, k, readopts), None
                case _, None:
                    return self._db.key_may_exist(key), None
                case _, _:
                    return self._db.key_may_exist_opt(key, readopts), None

    def multi_get(self, keys: list[Key], as_dict: bool = True, *args, **kwargs) -> list[bytes | None] | dict[Key, bytes | None]:
        readopts = _readopts(*args, **kwargs)
        norm_keys: list[tuple[_rocksdb_pyo3.ColumnFamilyHandle, bytes]] = []
        for key in keys:
            cf, k = _ensure_cf(norm_key)
            norm_keys.append((cf._handle, k))
        values: list[bytes | None]
        if readopts is not None:
            values = self._db.multi_get_cf_opt(norm_keys, readopts)
        else:
            values = self._db.multi_get_cf(norm_keys)
        if not as_dict:
            return values
        res: dict[Key, bytes | None] = {}
        for i, value in enumerate(values):
            key = keys[i]
            res[key] = value
        return res

    def _iterator(self, column_family: ColumnFamilyHandle | None = None, *args, **kwargs) -> _rocksdb_pyo3.DBIterator:
        readopts = _readopts(*args, **kwargs)
        match column_family, readopts:
            case None, None:
                return self._db.iterator()
            case None, _:
                return self._db.iterator_opt(readopts)
            case _, None:
                return self._db.iterator_cf(column_family._handle)
            case _, _:
                return self._db.iterator_cf_opt(column_family._handle, readopts)

    def iterkeys(self, column_family: ColumnFamilyHandle | None = None, *args, **kwargs) -> KeysIterator:
        return KeysIterator(self._iterator(column_family, *args, **kwargs) , column_family)

    def itervalues(self, column_family: ColumnFamilyHandle | None = None, *args, **kwargs) -> ValuesIterator:
        return ValuesIterator(self._iterator(column_family, *args, **kwargs) , column_family)

    def iteritems(self, column_family: ColumnFamilyHandle | None = None, *args, **kwargs) -> ItemsIterator:
        return ItemsIterator(self._iterator(column_family, *args, **kwargs) , column_family)

    # iterskeys(self, column_families: list[ColumnFamilyHandle] = None, <READOPTS>) -> ItemsIterator


def _compression_type(t: str) -> _rocksdb_pyo3.DBCompressionType:
    match t:
        case 'all':
            return _rocksdb_pyo3.ReadTier.ALL
        case 'cache':
            return _rocksdb_pyo3.ReadTier.BLOCK_CACHE
        case _:
            raise ValueError('invalid read_tier')


def _read_tier(t: str) -> _rocksdb_pyo3.ReadTier:
    match t:
        case 'all':
            return _rocksdb_pyo3.ReadTier.ALL
        case 'cache':
            return _rocksdb_pyo3.ReadTier.BLOCK_CACHE
        case _:
            raise ValueError('invalid read_tier')


def _bottommost_level_compaction(t: str) -> _rocksdb_pyo3.BottommostLevelCompaction:
    match t:
        case 'skip':
            return _rocksdb_pyo3.BottommostLevelCompaction.SKIP
        case 'if_have_compaction_filter':
            return _rocksdb_pyo3.BottommostLevelCompaction.IF_HAVE_COMPACTION_FILTER
        case 'force':
            return _rocksdb_pyo3.BottommostLevelCompaction.FORCE
        case 'force_optimized':
            return _rocksdb_pyo3.BottommostLevelCompaction.FORCE_OPTIMIZED
        case _:
            raise ValueError('invalid read_tier')


def _map_cfs(column_families: dict[bytes, ColumnFamilyOptions]) -> list[bytes, _rocksdb_pyo3.Options]:
    return [(k.decode('utf-8'), v._opts) for k, v in column_families.items()]


def _readopts(*args, **kwargs) -> _rocksdb_pyo3.ReadOptions:
    if not args and not kwargs:
        return None
    return _readopts_inner(*args, **kwargs)


def _ensure_readopts(self, readopts: _rocksdb_pyo3.ReadOptions | None) -> _rocksdb_pyo3.ReadOptions:
    if readopts is None:
        return _rocksdb_pyo3.ReadOptions()
    else:
        return readopts


def _readopts_inner(verify_checksums: bool = True, fill_cache: bool = True,
                    snapshot: Snapshot | None = None, read_tier: str = 'all'
                    ) -> _rocksdb_pyo3.WriteOptions:
    opts = _rocksdb_pyo3.ReadOptions()
    opts.set_verify_checksums(verify_checksums)
    opts.fill_cache(fill_cache)
    if snapshot is not None:
        opts.set_snapshot(snapshot)
    opts.set_read_tier(_read_tier(read_tier))
    return opts


def _writeopts(*args, **kwargs) -> _rocksdb_pyo3.WriteOptions:
    if not args and not kwargs:
        return None
    return _writeopts_inner(*args, **kwargs)


def _writeopts_inner(sync: bool = False, disable_wal: bool = False,
                     ignore_missing_column_families: bool = False,
                     no_slowdown: bool = False, low_pri: bool = False,
                     memtable_insert_hint_per_batch: bool = False,
                     ) -> _rocksdb_pyo3.WriteOptions:
    opts = _rocksdb_pyo3.WriteOptions()
    opts.set_sync(sync)
    opts.disable_wal(disable_wal)
    opts.set_ignore_missing_column_families(ignore_missing_column_families)
    opts.set_no_slowdown(no_slowdown)
    opts.set_low_pri(low_pri)
    opts.set_memtable_insert_hint_per_batch(memtable_insert_hint_per_batch)
    return opts


def _compactopts(*args, **kwargs) -> _rocksdb_pyo3.CompactOptions:
    if not args and not kwargs:
        return None
    return _compactopts_inner(*args, **kwargs)


def _compactopts_inner(
        change_level: bool = False,
        target_level: int = -1,
        bottommost_level_compaction: str = 'if_compaction_filter',
        ) -> _rocksdb_pyo3.CompactOptions:
    opts = _rocksdb_pyo3.CompactOptions()
    opts.change_level(set_sync)
    opts.target_level(change_level)
    opts.bottommost_level_compaction(_bottommost_level_compaction(bottommost_level_compaction))
    return opts


def _ensure_default_cf(cfs: dict[bytes, ColumnFamilyOptions]) -> dict[bytes, ColumnFamilyOptions]:
    if b'default' in cfs:
        return cfs
    else:
        # XXX: b'default' cf is forced to use default options
        return {
            b'default': ColumnFamilyOptions(),
            **cfs
        }
