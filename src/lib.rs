use anyhow::Result;
use log::info;
//use pyo3::create_exception;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use rocksdb;
use std::error;
use std::fmt;
use std::mem::transmute;
use std::ops::{Deref, DerefMut};
use std::path::Path;
use std::sync::{Arc, Weak};
use std::ptr;

// TODO: own error type
//type Result<T> = core::result::Result<T, Error>;
//
//create_exception!(rocksdb_pyo3, Error, pyo3::exceptions::PyException);
//impl From<rocksdb::Error> for Error {
//    fn from(value: rocksdb::Error) -> Self {
//        // TODO: improve this:
//        Self::new_err(value.into_string())
//    }
//}

#[derive(Debug)]
struct DBClosedError;

impl fmt::Display for DBClosedError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DB is closed")
    }
}

impl error::Error for DBClosedError {}

#[pyclass(module = "rocksdb_pyo3")]
#[derive(Clone)]
struct Cache(rocksdb::Cache);

impl From<rocksdb::Cache> for Cache {
    fn from(value: rocksdb::Cache) -> Self {
        Cache(value)
    }
}

impl Deref for Cache {
    type Target = rocksdb::Cache;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Cache {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[pymethods]
impl Cache {
    #[staticmethod]
    fn new_lru_cache(capacity: usize) -> Self {
        rocksdb::Cache::new_lru_cache(capacity).into()
    }

    #[staticmethod]
    fn new_hyper_clock_cache(capacity: usize, estimated_entry_charge: usize) -> Self {
        rocksdb::Cache::new_hyper_clock_cache(capacity, estimated_entry_charge).into()
    }

    fn get_usage(&self) -> usize {
        self.0.get_usage()
    }

    fn get_pinned_usage(&self) -> usize {
        self.0.get_pinned_usage()
    }

    fn set_capacity(mut slf: PyRefMut<Self>, capacity: usize) {
        slf.0.set_capacity(capacity)
    }
}

#[pyclass(module = "rocksdb_pyo3")]
struct ColumnFamilyDescriptor(rocksdb::ColumnFamilyDescriptor);

impl From<rocksdb::ColumnFamilyDescriptor> for ColumnFamilyDescriptor {
    fn from(value: rocksdb::ColumnFamilyDescriptor) -> Self {
        ColumnFamilyDescriptor(value)
    }
}

impl Deref for ColumnFamilyDescriptor {
    type Target = rocksdb::ColumnFamilyDescriptor;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for ColumnFamilyDescriptor {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[pymethods]
impl ColumnFamilyDescriptor {
    #[new]
    fn new(name: &str, options: &Options) -> Self {
        rocksdb::ColumnFamilyDescriptor::new(name, options.0.clone()).into()
    }

    fn name(&self) -> &str {
        self.0.name()
    }
}

#[pyclass(module = "rocksdb_pyo3")]
#[derive(Clone)]
struct Env(rocksdb::Env);

impl From<rocksdb::Env> for Env {
    fn from(value: rocksdb::Env) -> Self {
        Env(value)
    }
}

impl Deref for Env {
    type Target = rocksdb::Env;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Env {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[pymethods]
impl Env {
    #[new]
    fn new() -> Result<Self> {
        Ok(rocksdb::Env::new()?.into())
    }

    #[staticmethod]
    fn create_mem_env() -> Result<Self> {
        Ok(rocksdb::Env::mem_env()?.into())
    }

    fn set_background_threads(mut slf: PyRefMut<Self>, num_threads: i32) {
        slf.0.set_background_threads(num_threads)
    }

    fn set_high_priority_background_threads(mut slf: PyRefMut<Self>, n: i32) {
        slf.0.set_high_priority_background_threads(n)
    }

    fn set_low_priority_background_threads(mut slf: PyRefMut<Self>, n: i32) {
        slf.0.set_low_priority_background_threads(n)
    }

    fn set_bottom_priority_background_threads(mut slf: PyRefMut<Self>, n: i32) {
        slf.0.set_bottom_priority_background_threads(n)
    }

    fn join_all_threads(mut slf: PyRefMut<Self>) {
        slf.0.join_all_threads()
    }

    fn lower_thread_pool_io_priority(mut slf: PyRefMut<Self>) {
        slf.0.lower_thread_pool_io_priority()
    }

    fn lower_high_priority_thread_pool_io_priority(mut slf: PyRefMut<Self>) {
        slf.0.lower_high_priority_thread_pool_io_priority()
    }

    fn lower_thread_pool_cpu_priority(mut slf: PyRefMut<Self>) {
        slf.0.lower_thread_pool_cpu_priority()
    }

    fn lower_high_priority_thread_pool_cpu_priority(mut slf: PyRefMut<Self>) {
        slf.0.lower_high_priority_thread_pool_cpu_priority()
    }
}

//#[pyclass(module = "rocksdb_pyo3")]
//struct DBPath(rocksdb::DBPath);
//
//// XXX: is this fine?
//unsafe impl Send for DBPath {}
//unsafe impl Sync for DBPath {}
//
//impl From<rocksdb::DBPath> for DBPath {
//    fn from(value: rocksdb::DBPath) -> Self {
//        DBPath(value)
//    }
//}
//
//impl Deref for DBPath {
//    type Target = rocksdb::DBPath;
//
//    fn deref(&self) -> &Self::Target {
//        &self.0
//    }
//}
//
//impl DerefMut for DBPath {
//    fn deref_mut(&mut self) -> &mut Self::Target {
//        &mut self.0
//    }
//}
//
//#[pymethods]
//impl DBPath {
//    #[new]
//    fn new(path: &str, target_size: u64) -> Result<Self> {
//        Ok(rocksdb::DBPath::new(path, target_size)?.into())
//    }
//}

#[pyclass(module = "rocksdb_pyo3")]
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
#[allow(non_camel_case_types)]
enum DBCompressionType {
    NO = rocksdb::DBCompressionType::None as isize,
    SNAPPY = rocksdb::DBCompressionType::Snappy as isize,
    ZLIB = rocksdb::DBCompressionType::Zlib as isize,
    BZ2 = rocksdb::DBCompressionType::Bz2 as isize,
    LZ4 = rocksdb::DBCompressionType::Lz4 as isize,
    LZ4HC = rocksdb::DBCompressionType::Lz4hc as isize,
    ZSTD = rocksdb::DBCompressionType::Zstd as isize,
}

impl From<rocksdb::DBCompressionType> for DBCompressionType {
    fn from(value: rocksdb::DBCompressionType) -> Self {
        use rocksdb::DBCompressionType::*;
        match value {
            None => Self::NO,
            Snappy => Self::SNAPPY,
            Zlib => Self::ZLIB,
            Bz2 => Self::BZ2,
            Lz4 => Self::LZ4,
            Lz4hc => Self::LZ4HC,
            Zstd => Self::ZSTD,
        }
    }
}

impl From<DBCompressionType> for rocksdb::DBCompressionType {
    fn from(value: DBCompressionType) -> Self {
        use DBCompressionType::*;
        match value {
            NO => Self::None,
            SNAPPY => Self::Snappy,
            ZLIB => Self::Zlib,
            BZ2 => Self::Bz2,
            LZ4 => Self::Lz4,
            LZ4HC => Self::Lz4hc,
            ZSTD => Self::Zstd,
        }
    }
}

#[pyclass(module = "rocksdb_pyo3")]
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
#[allow(non_camel_case_types)]
enum DBCompactionStyle {
    LEVEL = rocksdb::DBCompactionStyle::Level as isize,
    UNIVERSAL = rocksdb::DBCompactionStyle::Universal as isize,
    FIFO = rocksdb::DBCompactionStyle::Fifo as isize,
}

impl From<rocksdb::DBCompactionStyle> for DBCompactionStyle {
    fn from(value: rocksdb::DBCompactionStyle) -> Self {
        use rocksdb::DBCompactionStyle::*;
        match value {
            Level => Self::LEVEL,
            Universal => Self::UNIVERSAL,
            Fifo => Self::FIFO,
        }
    }
}

impl From<DBCompactionStyle> for rocksdb::DBCompactionStyle {
    fn from(value: DBCompactionStyle) -> Self {
        use DBCompactionStyle::*;
        match value {
            LEVEL => Self::Level,
            UNIVERSAL => Self::Universal,
            FIFO => Self::Fifo,
        }
    }
}

#[pyclass(module = "rocksdb_pyo3")]
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
#[allow(non_camel_case_types)]
enum DBRecoveryMode {
    TOLERATE_CORRUPTED_TAIL_RECORDS =
        rocksdb::DBRecoveryMode::TolerateCorruptedTailRecords as isize,
    ABSOLUTE_CONSISTENCY = rocksdb::DBRecoveryMode::AbsoluteConsistency as isize,
    POINT_IN_TIME = rocksdb::DBRecoveryMode::PointInTime as isize,
    SKIP_ANY_CORRUPTED_RECORDS = rocksdb::DBRecoveryMode::SkipAnyCorruptedRecord as isize,
}

impl From<rocksdb::DBRecoveryMode> for DBRecoveryMode {
    fn from(value: rocksdb::DBRecoveryMode) -> Self {
        use rocksdb::DBRecoveryMode::*;
        match value {
            TolerateCorruptedTailRecords => Self::TOLERATE_CORRUPTED_TAIL_RECORDS,
            AbsoluteConsistency => Self::ABSOLUTE_CONSISTENCY,
            PointInTime => Self::POINT_IN_TIME,
            SkipAnyCorruptedRecord => Self::SKIP_ANY_CORRUPTED_RECORDS,
        }
    }
}

impl From<DBRecoveryMode> for rocksdb::DBRecoveryMode {
    fn from(value: DBRecoveryMode) -> Self {
        use DBRecoveryMode::*;
        match value {
            TOLERATE_CORRUPTED_TAIL_RECORDS => Self::TolerateCorruptedTailRecords,
            ABSOLUTE_CONSISTENCY => Self::AbsoluteConsistency,
            POINT_IN_TIME => Self::PointInTime,
            SKIP_ANY_CORRUPTED_RECORDS => Self::SkipAnyCorruptedRecord,
        }
    }
}

#[pyclass(module = "rocksdb_pyo3")]
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
#[repr(i32)]
#[allow(non_camel_case_types)]
enum ReadTier {
    ALL = rocksdb::ReadTier::All as i32,
    BLOCK_CACHE = rocksdb::ReadTier::BlockCache as i32,
}

impl From<rocksdb::ReadTier> for ReadTier {
    fn from(value: rocksdb::ReadTier) -> Self {
        use rocksdb::ReadTier::*;
        match value {
            All => Self::ALL,
            BlockCache => Self::BLOCK_CACHE,
        }
    }
}

impl From<ReadTier> for rocksdb::ReadTier {
    fn from(value: ReadTier) -> Self {
        use ReadTier::*;
        match value {
            ALL => Self::All,
            BLOCK_CACHE => Self::BlockCache,
        }
    }
}

#[pyclass(module = "rocksdb_pyo3")]
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
#[repr(i32)]
#[allow(non_camel_case_types)]
enum LogLevel {
    DEBUG = rocksdb::LogLevel::Debug as i32,
    INFO = rocksdb::LogLevel::Info as i32,
    WARN = rocksdb::LogLevel::Warn as i32,
    ERROR = rocksdb::LogLevel::Error as i32,
    FATAL = rocksdb::LogLevel::Fatal as i32,
    HEADER = rocksdb::LogLevel::Header as i32,
}

impl From<rocksdb::LogLevel> for LogLevel {
    fn from(value: rocksdb::LogLevel) -> Self {
        use rocksdb::LogLevel::*;
        match value {
            Debug => Self::DEBUG,
            Info => Self::INFO,
            Warn => Self::WARN,
            Error => Self::ERROR,
            Fatal => Self::FATAL,
            Header => Self::HEADER,
        }
    }
}

impl From<LogLevel> for rocksdb::LogLevel {
    fn from(value: LogLevel) -> Self {
        use LogLevel::*;
        match value {
            DEBUG => Self::Debug,
            INFO => Self::Info,
            WARN => Self::Warn,
            ERROR => Self::Error,
            FATAL => Self::Fatal,
            HEADER => Self::Header,
        }
    }
}

#[pyclass(module = "rocksdb_pyo3")]
#[derive(Default, Clone)]
struct Options(rocksdb::Options);

impl From<rocksdb::Options> for Options {
    fn from(value: rocksdb::Options) -> Self {
        Options(value)
    }
}

impl Deref for Options {
    type Target = rocksdb::Options;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Options {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[pymethods]
impl Options {
    #[new]
    fn new() -> Self {
        Self::default()
    }

    #[staticmethod]
    fn load_latest(
        path: &str,
        env: Env,
        ignore_unknown_options: bool,
        cache: Cache,
    ) -> Result<(Self, Vec<ColumnFamilyDescriptor>)> {
        let (options, cfds) =
            rocksdb::Options::load_latest(path, env.0, ignore_unknown_options, cache.0)?;
        Ok((
            options.into(),
            cfds.into_iter().map(|cfd| cfd.into()).collect(),
        ))
    }

    fn increase_parallelism(mut slf: PyRefMut<Self>, parallelism: i32) {
        slf.0.increase_parallelism(parallelism)
    }

    fn optimize_level_style_compaction(mut slf: PyRefMut<Self>, memtable_memory_budget: usize) {
        slf.0
            .optimize_level_style_compaction(memtable_memory_budget)
    }

    fn optimize_universal_style_compaction(mut slf: PyRefMut<Self>, memtable_memory_budget: usize) {
        slf.0
            .optimize_universal_style_compaction(memtable_memory_budget)
    }

    fn create_if_missing(mut slf: PyRefMut<Self>, create_if_missing: bool) {
        slf.0.create_if_missing(create_if_missing)
    }

    fn create_missing_column_families(mut slf: PyRefMut<Self>, create_missing_cfs: bool) {
        slf.0.create_missing_column_families(create_missing_cfs)
    }

    fn set_error_if_exists(mut slf: PyRefMut<Self>, enabled: bool) {
        slf.0.set_error_if_exists(enabled)
    }

    fn set_paranoid_checks(mut slf: PyRefMut<Self>, enabled: bool) {
        slf.0.set_paranoid_checks(enabled)
    }

    // TODO: figure out how to extract rocksdb::DBPath from whatever py-ref, since it doesn't support cloning
    //fn set_db_paths<'py>(mut slf: PyRefMut<Self>, py: Python<'py>, paths: Vec<PyRefMut<'py, DBPath>>) {
    //    let dbpaths: Vec<rocksdb::DBPath> = paths.into_iter().map(|p| Ok(p.extract::<DBPath>(py)?.0)).collect();
    //    slf.0.set_db_paths(&dbpaths)
    //}
    fn set_db_paths(mut slf: PyRefMut<Self>, paths: Vec<(&str, u64)>) -> Result<()> {
        let dbpaths: Result<Vec<_>, rocksdb::Error> = paths
            .into_iter()
            .map(|(path, target_size)| rocksdb::DBPath::new(path, target_size))
            .collect();
        Ok(slf.0.set_db_paths(dbpaths?.as_slice()))
    }

    fn set_env(mut slf: PyRefMut<Self>, env: &Env) {
        slf.0.set_env(env)
    }

    fn set_compression_type(mut slf: PyRefMut<Self>, t: DBCompressionType) {
        slf.0.set_compression_type(t.into())
    }

    fn set_bottommost_compression_type(mut slf: PyRefMut<Self>, t: DBCompressionType) {
        slf.0.set_bottommost_compression_type(t.into())
    }

    fn set_compression_per_level(mut slf: PyRefMut<Self>, level_types: Vec<DBCompressionType>) {
        let level_types: Vec<rocksdb::DBCompressionType> =
            level_types.into_iter().map(Into::into).collect();
        slf.0.set_compression_per_level(level_types.as_slice())
    }

    fn set_compression_options(
        mut slf: PyRefMut<Self>,
        w_bits: i32,
        level: i32,
        strategy: i32,
        max_dict_bytes: i32,
    ) {
        slf.0
            .set_compression_options(w_bits, level, strategy, max_dict_bytes)
    }

    fn set_bottommost_compression_options(
        mut slf: PyRefMut<Self>,
        w_bits: i32,
        level: i32,
        strategy: i32,
        max_dict_bytes: i32,
        enabled: bool,
    ) {
        slf.0
            .set_bottommost_compression_options(w_bits, level, strategy, max_dict_bytes, enabled)
    }

    fn set_zstd_max_train_bytes(mut slf: PyRefMut<Self>, value: i32) {
        slf.0.set_zstd_max_train_bytes(value)
    }

    fn set_bottommost_zstd_max_train_bytes(mut slf: PyRefMut<Self>, value: i32, enabled: bool) {
        slf.0.set_bottommost_zstd_max_train_bytes(value, enabled)
    }

    fn set_compaction_readahead_size(mut slf: PyRefMut<Self>, compaction_readahead_size: usize) {
        slf.0
            .set_compaction_readahead_size(compaction_readahead_size)
    }

    fn set_level_compaction_dynamic_level_bytes(mut slf: PyRefMut<Self>, v: bool) {
        slf.0.set_level_compaction_dynamic_level_bytes(v)
    }

    // TODO:
    //fn set_merge_operator_associative<F: MergeFn + Clone>(&mut self, name: impl CStrLike, full_merge_fn: F)
    //fn set_merge_operator<F: MergeFn, PF: MergeFn>(&mut self, name: impl CStrLike, full_merge_fn: F, partial_merge_fn: PF)
    //fn set_compaction_filter<F>(&mut self, name: impl CStrLike, filter_fn: F)where F: CompactionFilterFn + Send + 'static,
    //fn set_compaction_filter_factory<F>(&mut self, factory: F)where F: CompactionFilterFactory + 'static,
    //fn set_comparator(&mut self, name: impl CStrLike, compare_fn: Box<dyn Fn(&[u8], &[u8]) -> Ordering>)

    fn set_optimize_filters_for_hits(mut slf: PyRefMut<Self>, optimize_for_hits: bool) {
        slf.0.set_optimize_filters_for_hits(optimize_for_hits)
    }

    fn set_delete_obsolete_files_period_micros(mut slf: PyRefMut<Self>, micros: u64) {
        slf.0.set_delete_obsolete_files_period_micros(micros)
    }

    fn prepare_for_bulk_load(mut slf: PyRefMut<Self>) {
        slf.0.prepare_for_bulk_load()
    }

    fn set_max_open_files(mut slf: PyRefMut<Self>, nfiles: i32) {
        slf.0.set_max_open_files(nfiles)
    }

    fn set_max_file_opening_threads(mut slf: PyRefMut<Self>, nthreads: i32) {
        slf.0.set_max_file_opening_threads(nthreads)
    }

    fn set_use_fsync(mut slf: PyRefMut<Self>, useit: bool) {
        slf.0.set_use_fsync(useit)
    }

    fn set_db_log_dir(mut slf: PyRefMut<Self>, path: &str) {
        slf.0.set_db_log_dir(path)
    }

    fn set_log_level(mut slf: PyRefMut<Self>, level: LogLevel) {
        slf.0.set_log_level(level.into())
    }

    fn set_bytes_per_sync(mut slf: PyRefMut<Self>, nbytes: u64) {
        slf.0.set_bytes_per_sync(nbytes)
    }

    fn set_wal_bytes_per_sync(mut slf: PyRefMut<Self>, nbytes: u64) {
        slf.0.set_wal_bytes_per_sync(nbytes)
    }

    fn set_writable_file_max_buffer_size(mut slf: PyRefMut<Self>, nbytes: u64) {
        slf.0.set_writable_file_max_buffer_size(nbytes)
    }

    fn set_allow_concurrent_memtable_write(mut slf: PyRefMut<Self>, allow: bool) {
        slf.0.set_allow_concurrent_memtable_write(allow)
    }

    fn set_enable_write_thread_adaptive_yield(mut slf: PyRefMut<Self>, enabled: bool) {
        slf.0.set_enable_write_thread_adaptive_yield(enabled)
    }

    fn set_max_sequential_skip_in_iterations(mut slf: PyRefMut<Self>, num: u64) {
        slf.0.set_max_sequential_skip_in_iterations(num)
    }

    fn set_use_direct_reads(mut slf: PyRefMut<Self>, enabled: bool) {
        slf.0.set_use_direct_reads(enabled)
    }

    fn set_use_direct_io_for_flush_and_compaction(mut slf: PyRefMut<Self>, enabled: bool) {
        slf.0.set_use_direct_io_for_flush_and_compaction(enabled)
    }

    fn set_is_fd_close_on_exec(mut slf: PyRefMut<Self>, enabled: bool) {
        slf.0.set_is_fd_close_on_exec(enabled)
    }

    fn set_table_cache_num_shard_bits(mut slf: PyRefMut<Self>, nbits: i32) {
        slf.0.set_table_cache_num_shard_bits(nbits)
    }

    fn set_target_file_size_multiplier(mut slf: PyRefMut<Self>, multiplier: i32) {
        slf.0.set_target_file_size_multiplier(multiplier)
    }

    fn set_min_write_buffer_number(mut slf: PyRefMut<Self>, nbuf: i32) {
        slf.0.set_min_write_buffer_number(nbuf)
    }

    fn set_max_write_buffer_number(mut slf: PyRefMut<Self>, nbuf: i32) {
        slf.0.set_max_write_buffer_number(nbuf)
    }

    fn set_write_buffer_size(mut slf: PyRefMut<Self>, size: usize) {
        slf.0.set_write_buffer_size(size)
    }

    fn set_db_write_buffer_size(mut slf: PyRefMut<Self>, size: usize) {
        slf.0.set_db_write_buffer_size(size)
    }

    fn set_max_bytes_for_level_base(mut slf: PyRefMut<Self>, size: u64) {
        slf.0.set_max_bytes_for_level_base(size)
    }

    fn set_max_bytes_for_level_multiplier(mut slf: PyRefMut<Self>, mul: f64) {
        slf.0.set_max_bytes_for_level_multiplier(mul)
    }

    fn set_max_manifest_file_size(mut slf: PyRefMut<Self>, size: usize) {
        slf.0.set_max_manifest_file_size(size)
    }

    fn set_target_file_size_base(mut slf: PyRefMut<Self>, size: u64) {
        slf.0.set_target_file_size_base(size)
    }

    fn set_min_write_buffer_number_to_merge(mut slf: PyRefMut<Self>, to_merge: i32) {
        slf.0.set_min_write_buffer_number_to_merge(to_merge)
    }

    fn set_level_zero_file_num_compaction_trigger(mut slf: PyRefMut<Self>, n: i32) {
        slf.0.set_level_zero_file_num_compaction_trigger(n)
    }

    fn set_level_zero_slowdown_writes_trigger(mut slf: PyRefMut<Self>, n: i32) {
        slf.0.set_level_zero_slowdown_writes_trigger(n)
    }

    fn set_level_zero_stop_writes_trigger(mut slf: PyRefMut<Self>, n: i32) {
        slf.0.set_level_zero_stop_writes_trigger(n)
    }

    fn set_compaction_style(mut slf: PyRefMut<Self>, style: DBCompactionStyle) {
        slf.0.set_compaction_style(style.into())
    }

    // TODO:
    //fn set_universal_compaction_options(mut slf: PyRefMut<Self>, uco: &UniversalCompactOptions) {
    //}

    // TODO:
    //fn set_fifo_compaction_options(mut slf: PyRefMut<Self>, fco: &FifoCompactOptions) {
    //}

    fn set_unordered_write(mut slf: PyRefMut<Self>, unordered: bool) {
        slf.0.set_unordered_write(unordered)
    }

    fn set_max_subcompactions(mut slf: PyRefMut<Self>, num: u32) {
        slf.0.set_max_subcompactions(num)
    }

    fn set_max_background_jobs(mut slf: PyRefMut<Self>, jobs: i32) {
        slf.0.set_max_background_jobs(jobs)
    }

    fn set_disable_auto_compactions(mut slf: PyRefMut<Self>, disable: bool) {
        slf.0.set_disable_auto_compactions(disable)
    }

    fn set_memtable_huge_page_size(mut slf: PyRefMut<Self>, size: usize) {
        slf.0.set_memtable_huge_page_size(size)
    }

    fn set_max_successive_merges(mut slf: PyRefMut<Self>, num: usize) {
        slf.0.set_max_successive_merges(num)
    }

    fn set_bloom_locality(mut slf: PyRefMut<Self>, v: u32) {
        slf.0.set_bloom_locality(v)
    }

    fn set_inplace_update_support(mut slf: PyRefMut<Self>, enabled: bool) {
        slf.0.set_inplace_update_support(enabled)
    }

    fn set_inplace_update_locks(mut slf: PyRefMut<Self>, num: usize) {
        slf.0.set_inplace_update_locks(num)
    }

    fn set_max_bytes_for_level_multiplier_additional(
        mut slf: PyRefMut<Self>,
        level_values: Vec<i32>,
    ) {
        slf.0
            .set_max_bytes_for_level_multiplier_additional(level_values.as_slice())
    }

    fn set_skip_checking_sst_file_sizes_on_db_open(mut slf: PyRefMut<Self>, value: bool) {
        slf.0.set_skip_checking_sst_file_sizes_on_db_open(value)
    }

    fn set_max_write_buffer_size_to_maintain(mut slf: PyRefMut<Self>, size: i64) {
        slf.0.set_max_write_buffer_size_to_maintain(size)
    }

    fn set_enable_pipelined_write(mut slf: PyRefMut<Self>, value: bool) {
        slf.0.set_enable_pipelined_write(value)
    }

    // TODO:
    //fn set_memtable_factory(mut slf: PyRefMut<Self>, factory: MemtableFactory) {
    //}

    // TODO:
    //fn set_memtable_factory(mut slf: PyRefMut<Self>, factory: MemtableFactory) {
    //}
    
    // TODO:
    //fn set_block_based_table_factory(mut slf: PyRefMut<Self>, factory: &BlockBasedOptions) {
    //}
    
    // TODO:
    //fn set_cuckoo_table_factory(mut slf: PyRefMut<Self>, factory: &CuckooTableOptions) {
    //}

    // TODO:
    //fn set_plain_table_factory(mut slf: PyRefMut<Self>, options: &PlainTableFactoryOptions) {
    //}

    fn set_min_level_to_compress(mut slf: PyRefMut<Self>, lvl: i32) {
        slf.0.set_min_level_to_compress(lvl)
    }

    fn set_report_bg_io_stats(mut slf: PyRefMut<Self>, enable: bool) {
        slf.0.set_report_bg_io_stats(enable)
    }

    fn set_max_total_wal_size(mut slf: PyRefMut<Self>, size: u64) {
        slf.0.set_max_total_wal_size(size)
    }

    fn set_wal_recovery_mode(mut slf: PyRefMut<Self>, mode: DBRecoveryMode) {
        slf.0.set_wal_recovery_mode(mode.into())
    }

    fn set_stats_dump_period_sec(mut slf: PyRefMut<Self>, period: u32) {
        slf.0.set_stats_dump_period_sec(period)
    }

    fn set_stats_persist_period_sec(mut slf: PyRefMut<Self>, period: u32) {
        slf.0.set_stats_persist_period_sec(period)
    }

    fn set_advise_random_on_open(mut slf: PyRefMut<Self>, advise: bool) {
        slf.0.set_advise_random_on_open(advise)
    }

    // TODO:
    //fn set_access_hint_on_compaction_start(mut slf: PyRefMut<Self>, pattern: AccessHint) {
    //}

    fn set_use_adaptive_mutex(mut slf: PyRefMut<Self>, enabled: bool) {
        slf.0.set_use_adaptive_mutex(enabled)
    }

    fn set_num_levels(mut slf: PyRefMut<Self>, n: i32) {
        slf.0.set_num_levels(n)
    }

    fn set_memtable_prefix_bloom_ratio(mut slf: PyRefMut<Self>, ratio: f64) {
        slf.0.set_memtable_prefix_bloom_ratio(ratio)
    }

    fn set_max_compaction_bytes(mut slf: PyRefMut<Self>, nbytes: u64) {
        slf.0.set_max_compaction_bytes(nbytes)
    }

    fn set_wal_dir(mut slf: PyRefMut<Self>, path: &str) {
        slf.0.set_wal_dir(path)
    }

    fn set_wal_ttl_seconds(mut slf: PyRefMut<Self>, secs: u64) {
        slf.0.set_wal_ttl_seconds(secs)
    }

    fn set_wal_size_limit_mb(mut slf: PyRefMut<Self>, size: u64) {
        slf.0.set_wal_size_limit_mb(size)
    }

    fn set_manifest_preallocation_size(mut slf: PyRefMut<Self>, size: usize) {
        slf.0.set_manifest_preallocation_size(size)
    }

    fn set_skip_stats_update_on_db_open(mut slf: PyRefMut<Self>, skip: bool) {
        slf.0.set_skip_stats_update_on_db_open(skip)
    }

    fn set_keep_log_file_num(mut slf: PyRefMut<Self>, nfiles: usize) {
        slf.0.set_keep_log_file_num(nfiles)
    }

    fn set_allow_mmap_writes(mut slf: PyRefMut<Self>, is_enabled: bool) {
        slf.0.set_allow_mmap_writes(is_enabled)
    }

    fn set_allow_mmap_reads(mut slf: PyRefMut<Self>, is_enabled: bool) {
        slf.0.set_allow_mmap_reads(is_enabled)
    }

    fn set_manual_wal_flush(mut slf: PyRefMut<Self>, is_enabled: bool) {
        slf.0.set_manual_wal_flush(is_enabled)
    }

    fn set_atomic_flush(mut slf: PyRefMut<Self>, atomic_flush: bool) {
        slf.0.set_atomic_flush(atomic_flush)
    }

    fn set_row_cache(mut slf: PyRefMut<Self>, cache: &Cache) {
        slf.0.set_row_cache(cache)
    }

    fn set_ratelimiter(
        mut slf: PyRefMut<Self>,
        rate_bytes_per_sec: i64,
        refill_period_us: i64,
        fairness: i32,
    ) {
        slf.0
            .set_ratelimiter(rate_bytes_per_sec, refill_period_us, fairness)
    }

    fn set_max_log_file_size(mut slf: PyRefMut<Self>, size: usize) {
        slf.0.set_max_log_file_size(size)
    }

    fn set_log_file_time_to_roll(mut slf: PyRefMut<Self>, secs: usize) {
        slf.0.set_log_file_time_to_roll(secs)
    }

    fn set_recycle_log_file_num(mut slf: PyRefMut<Self>, num: usize) {
        slf.0.set_recycle_log_file_num(num)
    }

    fn set_soft_pending_compaction_bytes_limit(mut slf: PyRefMut<Self>, limit: usize) {
        slf.0.set_soft_pending_compaction_bytes_limit(limit)
    }

    fn set_hard_pending_compaction_bytes_limit(mut slf: PyRefMut<Self>, limit: usize) {
        slf.0.set_hard_pending_compaction_bytes_limit(limit)
    }

    fn set_arena_block_size(mut slf: PyRefMut<Self>, size: usize) {
        slf.0.set_arena_block_size(size)
    }

    fn set_dump_malloc_stats(mut slf: PyRefMut<Self>, enabled: bool) {
        slf.0.set_dump_malloc_stats(enabled)
    }

    fn set_memtable_whole_key_filtering(mut slf: PyRefMut<Self>, whole_key_filter: bool) {
        slf.0.set_memtable_whole_key_filtering(whole_key_filter)
    }

    fn set_enable_blob_files(mut slf: PyRefMut<Self>, val: bool) {
        slf.0.set_enable_blob_files(val)
    }

    fn set_min_blob_size(mut slf: PyRefMut<Self>, val: u64) {
        slf.0.set_min_blob_size(val)
    }

    fn set_blob_file_size(mut slf: PyRefMut<Self>, val: u64) {
        slf.0.set_blob_file_size(val)
    }

    fn set_blob_compression_type(mut slf: PyRefMut<Self>, val: DBCompressionType) {
        slf.0.set_blob_compression_type(val.into())
    }

    fn set_enable_blob_gc(mut slf: PyRefMut<Self>, val: bool) {
        slf.0.set_enable_blob_gc(val)
    }

    fn set_blob_gc_age_cutoff(mut slf: PyRefMut<Self>, val: f64) {
        slf.0.set_blob_gc_age_cutoff(val)
    }

    fn set_blob_gc_force_threshold(mut slf: PyRefMut<Self>, val: f64) {
        slf.0.set_blob_gc_force_threshold(val)
    }

    fn set_blob_compaction_readahead_size(mut slf: PyRefMut<Self>, val: u64) {
        slf.0.set_blob_compaction_readahead_size(val)
    }
}

#[pyclass(module = "rocksdb_pyo3")]
#[derive(Default)]
struct ReadOptions(rocksdb::ReadOptions);

impl From<rocksdb::ReadOptions> for ReadOptions {
    fn from(options: rocksdb::ReadOptions) -> Self {
        ReadOptions(options)
    }
}

impl From<ReadOptions> for rocksdb::ReadOptions {
    fn from(options: ReadOptions) -> Self {
        options.0
    }
}

impl Deref for ReadOptions {
    type Target = rocksdb::ReadOptions;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for ReadOptions {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[pymethods]
impl ReadOptions {
    #[new]
    fn py_new() -> Self {
        Self::default()
    }

    fn fill_cache(mut slf: PyRefMut<Self>, v: bool) {
        slf.0.fill_cache(v);
    }

    fn set_snapshot(mut slf: PyRefMut<Self>, snapshot: &Snapshot) -> Result<()> {
        Ok(slf.0.set_snapshot(snapshot.try_ref()?))
    }

    fn set_read_tier(mut slf: PyRefMut<Self>, read_tier: ReadTier) {
        slf.0.set_read_tier(read_tier.into())
    }

    fn set_prefix_same_as_start(mut slf: PyRefMut<Self>, v: bool) {
        slf.0.set_prefix_same_as_start(v)
    }

    fn set_total_order_seek(mut slf: PyRefMut<Self>, v: bool) {
        slf.0.set_total_order_seek(v)
    }

    fn set_max_skippable_internal_keys(mut slf: PyRefMut<Self>, num: u64) {
        slf.0.set_max_skippable_internal_keys(num)
    }

    fn set_background_purge_on_iterator_cleanup(mut slf: PyRefMut<Self>, v: bool) {
        slf.0.set_background_purge_on_iterator_cleanup(v)
    }

    fn set_ignore_range_deletions(mut slf: PyRefMut<Self>, v: bool) {
        slf.0.set_ignore_range_deletions(v)
    }

    fn set_verify_checksums(mut slf: PyRefMut<Self>, v: bool) {
        slf.0.set_verify_checksums(v)
    }

    fn set_readahead_size(mut slf: PyRefMut<Self>, v: usize) {
        slf.0.set_readahead_size(v)
    }

    fn set_auto_readahead_size(mut slf: PyRefMut<Self>, v: bool) {
        slf.0.set_auto_readahead_size(v)
    }

    fn set_tailing(mut slf: PyRefMut<Self>, v: bool) {
        slf.0.set_tailing(v)
    }

    fn set_pin_data(mut slf: PyRefMut<Self>, v: bool) {
        slf.0.set_pin_data(v)
    }

    fn set_async_io(mut slf: PyRefMut<Self>, v: bool) {
        slf.0.set_async_io(v)
    }
}

#[pyclass(module = "rocksdb_pyo3")]
#[derive(Default)]
struct WriteOptions(rocksdb::WriteOptions);

impl From<rocksdb::WriteOptions> for WriteOptions {
    fn from(value: rocksdb::WriteOptions) -> Self {
        WriteOptions(value)
    }
}

impl Deref for WriteOptions {
    type Target = rocksdb::WriteOptions;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for WriteOptions {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[pymethods]
impl WriteOptions {
    #[new]
    fn py_new() -> Self {
        Self::default()
    }

    fn set_sync(mut slf: PyRefMut<Self>, sync: bool) {
        slf.0.set_sync(sync);
    }

    fn disable_wal(mut slf: PyRefMut<Self>, disable: bool) {
        slf.0.disable_wal(disable);
    }

    fn set_ignore_missing_column_families(mut slf: PyRefMut<Self>, ignore: bool) {
        slf.0.set_ignore_missing_column_families(ignore);
    }

    fn set_no_slowdown(mut slf: PyRefMut<Self>, no_slowdown: bool) {
        slf.0.set_no_slowdown(no_slowdown);
    }

    fn set_low_pri(mut slf: PyRefMut<Self>, v: bool) {
        slf.0.set_low_pri(v);
    }

    fn set_memtable_insert_hint_per_batch(mut slf: PyRefMut<Self>, v: bool) {
        slf.0.set_memtable_insert_hint_per_batch(v);
    }
}

#[pyclass(module = "rocksdb_pyo3")]
#[derive(Default)]
struct FlushOptions(rocksdb::FlushOptions);

impl From<rocksdb::FlushOptions> for FlushOptions {
    fn from(value: rocksdb::FlushOptions) -> Self {
        FlushOptions(value)
    }
}

impl Deref for FlushOptions {
    type Target = rocksdb::FlushOptions;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for FlushOptions {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[pymethods]
impl FlushOptions {
    #[new]
    fn py_new() -> Self {
        Self::default()
    }

    fn wait(mut slf: PyRefMut<Self>, wait: bool) {
        slf.0.set_wait(wait);
    }
}

#[pyclass(module = "rocksdb_pyo3")]
#[derive(Default)]
struct CompactOptions(rocksdb::CompactOptions);

impl From<rocksdb::CompactOptions> for CompactOptions {
    fn from(value: rocksdb::CompactOptions) -> Self {
        CompactOptions(value)
    }
}

impl Deref for CompactOptions {
    type Target = rocksdb::CompactOptions;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for CompactOptions {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[pyclass(module = "rocksdb_pyo3")]
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
#[repr(u8)]
#[allow(non_camel_case_types)]
enum BottommostLevelCompaction {
    SKIP = rocksdb::BottommostLevelCompaction::Skip as u8,
    IF_HAVE_COMPACTION_FILTER = rocksdb::BottommostLevelCompaction::IfHaveCompactionFilter as u8,
    FORCE = rocksdb::BottommostLevelCompaction::Force as u8,
    FORCE_OPTIMIZED = rocksdb::BottommostLevelCompaction::ForceOptimized as u8,
}

impl From<rocksdb::BottommostLevelCompaction> for BottommostLevelCompaction {
    fn from(value: rocksdb::BottommostLevelCompaction) -> Self {
        use rocksdb::BottommostLevelCompaction::*;
        match value {
            Skip => Self::SKIP,
            IfHaveCompactionFilter => Self::IF_HAVE_COMPACTION_FILTER,
            Force => Self::FORCE,
            ForceOptimized => Self::FORCE_OPTIMIZED,
        }
    }
}

impl From<BottommostLevelCompaction> for rocksdb::BottommostLevelCompaction {
    fn from(value: BottommostLevelCompaction) -> Self {
        use BottommostLevelCompaction::*;
        match value {
            SKIP => Self::Skip,
            IF_HAVE_COMPACTION_FILTER => Self::IfHaveCompactionFilter,
            FORCE => Self::Force,
            FORCE_OPTIMIZED => Self::ForceOptimized,
        }
    }
}

#[pymethods]
impl CompactOptions {
    #[new]
    fn py_new() -> Self {
        Self::default()
    }

    fn exclusive_manual_compaction(mut slf: PyRefMut<Self>, v: bool) {
        slf.0.set_exclusive_manual_compaction(v)
    }

    fn bottommost_level_compaction(mut slf: PyRefMut<Self>, c: BottommostLevelCompaction) {
        slf.0.set_bottommost_level_compaction(c.into())
    }

    fn change_level(mut slf: PyRefMut<Self>, v: bool) {
        slf.0.set_change_level(v)
    }

    fn target_level(mut slf: PyRefMut<Self>, lvl: i32) {
        slf.0.set_target_level(lvl)
    }
}

#[pyclass(module = "rocksdb_pyo3")]
#[derive(Clone, Debug)]
struct LiveFile(rocksdb::LiveFile);

impl From<rocksdb::LiveFile> for LiveFile {
    fn from(value: rocksdb::LiveFile) -> Self {
        LiveFile(value)
    }
}

impl Deref for LiveFile {
    type Target = rocksdb::LiveFile;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for LiveFile {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[pymethods]
impl LiveFile {
    #[getter]
    fn column_family_name(&self) -> &str {
        self.0.column_family_name.as_str()
    }

    #[getter]
    fn name(&self) -> &str {
        self.0.name.as_str()
    }

    #[getter]
    fn size(&self) -> usize {
        self.0.size
    }

    #[getter]
    fn level(&self) -> i32 {
        self.0.level
    }

    #[getter]
    fn start_key<'py>(&self, py: Python<'py>) -> Option<&'py PyBytes> {
        match &self.0.start_key {
            None => None,
            Some(key) => Some(PyBytes::new(py, key.as_slice())),
        }
    }

    #[getter]
    fn end_key<'py>(&self, py: Python<'py>) -> Option<&'py PyBytes> {
        match &self.0.end_key {
            None => None,
            Some(key) => Some(PyBytes::new(py, key.as_slice())),
        }
    }

    #[getter]
    fn num_entries(&self) -> u64 {
        self.0.num_entries
    }

    #[getter]
    fn num_deletions(&self) -> u64 {
        self.0.num_deletions
    }
}

// XXX: cannot use rocksdb::ColumnFamilyMetaData because it is not re-exported
#[pyclass(module = "rocksdb_pyo3")]
#[derive(Clone, Debug)]
pub struct ColumnFamilyMetaData {
    #[pyo3(get)]
    size: u64,
    #[pyo3(get)]
    name: String,
    #[pyo3(get)]
    file_count: usize,
}

pub(crate) mod helper {
    // Copied from https://docs.rs/rocksdb/latest/src/rocksdb/column_family.rs.html because it's
    // not public, but it's actually very handy to use
    pub(crate) struct UnboundColumnFamily {
        pub(crate) inner: *mut librocksdb_sys::rocksdb_column_family_handle_t,
    }

    impl Drop for UnboundColumnFamily {
        fn drop(&mut self) {
            unsafe {
                librocksdb_sys::rocksdb_column_family_handle_destroy(self.inner);
            }
        }
    }

    unsafe impl Send for UnboundColumnFamily {}
    unsafe impl Sync for UnboundColumnFamily {}
}

#[pyclass(module = "rocksdb_pyo3")]
struct ColumnFamilyHandle {
    handle: Weak<helper::UnboundColumnFamily>,
    db: Weak<rocksdb::DB>,
}

impl rocksdb::AsColumnFamilyRef for ColumnFamilyHandle {
    fn inner(&self) -> *mut librocksdb_sys::rocksdb_column_family_handle_t {
        if let Some(handle) = self.handle.upgrade() {
            handle.inner
        } else {
            ptr::null_mut()
        }
    }
}

impl ColumnFamilyHandle {
    fn new(handle: Arc<rocksdb::BoundColumnFamily>, db: &Arc<rocksdb::DB>) -> Self {
        let handle = unsafe {
            transmute::<Arc<rocksdb::BoundColumnFamily<'_>>, Arc<helper::UnboundColumnFamily>>(
                handle,
            )
        };
        let handle = Arc::downgrade(&handle);
        let db = Arc::downgrade(db);
        Self { handle, db }
    }
}

#[pymethods]
impl ColumnFamilyHandle {
    fn get_id(&self) -> Result<u32> {
        let _ = self.db.upgrade().ok_or(DBClosedError)?;
        let handle = self.handle.upgrade().ok_or(DBClosedError)?;
        let id = unsafe { librocksdb_sys::rocksdb_column_family_handle_get_id(handle.inner) };
        Ok(id)
    }

    fn get_name(&self) -> Result<&str> {
        let _ = self.db.upgrade().ok_or(DBClosedError)?;
        let handle = self.handle.upgrade().ok_or(DBClosedError)?;
        let mut name_len = 0usize;
        let name_len_ptr: *mut usize = &mut name_len;
        let name_ptr = unsafe {
            librocksdb_sys::rocksdb_column_family_handle_get_name(handle.inner, name_len_ptr)
                as *mut u8
        };
        let name = unsafe {
            let slice = core::slice::from_raw_parts(name_ptr, name_len);
            std::str::from_utf8_unchecked(slice)
        };
        Ok(name)
    }
}

#[pyclass(module = "rocksdb_pyo3")]
struct DBIterator {
    iter: rocksdb::DBRawIteratorWithThreadMode<'static, rocksdb::DB>,
    // XXX: since this is a workaround, order is important, this must be the last field
    db: Weak<rocksdb::DB>,
}

impl DBIterator {
    fn new<'db>(
        iter: rocksdb::DBRawIteratorWithThreadMode<'db, rocksdb::DB>,
        db: &Arc<rocksdb::DB>,
    ) -> Self {
        Self {
            // XXX: this is required to work around PyO3's limitation with parametrized lifetimes
            //      the field `_db: Arc<Rocksdb::DB>` ensures the lifetime will live long enough
            iter: unsafe {
                transmute::<
                    rocksdb::DBRawIteratorWithThreadMode<'db, rocksdb::DB>,
                    rocksdb::DBRawIteratorWithThreadMode<'static, rocksdb::DB>,
                >(iter)
            },
            db: Arc::downgrade(db),
        }
    }
}

#[pymethods]
impl DBIterator {
    fn valid(&self) -> Result<bool> {
        let _ = self.db.upgrade().ok_or(DBClosedError)?;
        Ok(self.iter.valid())
    }

    fn status(&self) -> Result<()> {
        let _ = self.db.upgrade().ok_or(DBClosedError)?;
        Ok(self.iter.status()?)
    }

    fn seek_to_first(mut slf: PyRefMut<Self>) -> Result<()> {
        let _ = slf.db.upgrade().ok_or(DBClosedError)?;
        Ok(slf.iter.seek_to_first())
    }

    fn seek_to_last(mut slf: PyRefMut<Self>) -> Result<()> {
        let _ = slf.db.upgrade().ok_or(DBClosedError)?;
        Ok(slf.iter.seek_to_last())
    }

    fn seek(mut slf: PyRefMut<Self>, key: &PyBytes) -> Result<()> {
        let _ = slf.db.upgrade().ok_or(DBClosedError)?;
        Ok(slf.iter.seek(key.as_bytes()))
    }

    fn seek_for_prev(mut slf: PyRefMut<Self>, key: &PyBytes) -> Result<()> {
        let _ = slf.db.upgrade().ok_or(DBClosedError)?;
        Ok(slf.iter.seek_for_prev(key.as_bytes()))
    }

    fn next(mut slf: PyRefMut<Self>) -> Result<()> {
        let _ = slf.db.upgrade().ok_or(DBClosedError)?;
        Ok(slf.iter.next())
    }

    fn prev(mut slf: PyRefMut<Self>) -> Result<()> {
        let _ = slf.db.upgrade().ok_or(DBClosedError)?;
        Ok(slf.iter.prev())
    }

    fn key<'py>(&self, py: Python<'py>) -> Result<Option<&'py PyBytes>> {
        let _ = self.db.upgrade().ok_or(DBClosedError)?;
        let key = match self.iter.key() {
            Some(k) => k,
            None => return Ok(None),
        };
        Ok(Some(PyBytes::new(py, key)))
    }

    fn value<'py>(&self, py: Python<'py>) -> Result<Option<&'py PyBytes>> {
        let _ = self.db.upgrade().ok_or(DBClosedError)?;
        let value = match self.iter.value() {
            Some(k) => k,
            None => return Ok(None),
        };
        Ok(Some(PyBytes::new(py, value)))
    }

    fn item<'py>(&self, py: Python<'py>) -> Result<Option<(&'py PyBytes, &'py PyBytes)>> {
        let _ = self.db.upgrade().ok_or(DBClosedError)?;
        let (key, value) = match self.iter.item() {
            Some((k, v)) => (k, v),
            None => return Ok(None),
        };
        Ok(Some((PyBytes::new(py, key), PyBytes::new(py, value))))
    }
}

#[pyclass(module = "rocksdb_pyo3")]
struct Snapshot {
    snapshot: rocksdb::Snapshot<'static>,
    // XXX: since this is a workaround, order is important, this must be the last field
    db: Weak<rocksdb::DB>,
}

impl Deref for Snapshot {
    type Target = rocksdb::Snapshot<'static>;

    fn deref(&self) -> &Self::Target {
        &self.snapshot
    }
}

impl Snapshot {
    fn new(db: &Arc<rocksdb::DB>) -> Self {
        Self {
            // XXX: this is required to work around PyO3's limitation with parametrized lifetimes
            //      the field `_db: Arc<Rocksdb::DB>` ensures the lifetime will live long enough
            snapshot: unsafe {
                transmute::<rocksdb::Snapshot<'_>, rocksdb::Snapshot<'static>>(db.snapshot())
            },
            db: Arc::downgrade(db),
        }
    }

    fn try_ref(&self) -> Result<&rocksdb::Snapshot<'static>> {
        let _ = self.db.upgrade().ok_or(DBClosedError)?;
        Ok(&self.snapshot)
    }
}

#[pymethods]
impl Snapshot {
    fn get<'py>(&self, py: Python<'py>, key: &PyBytes) -> Result<Option<&'py PyBytes>> {
        let _ = self.db.upgrade().ok_or(DBClosedError)?;
        Ok(self
            .snapshot
            .get_pinned(key.as_bytes())?
            .map(|value| PyBytes::new(py, &value)))
    }

    fn get_opt<'py>(
        &self,
        py: Python<'py>,
        key: &PyBytes,
        readopts: &PyCell<ReadOptions>,
    ) -> Result<Option<&'py PyBytes>> {
        let _ = self.db.upgrade().ok_or(DBClosedError)?;
        let readopts = readopts.replace(Default::default());
        Ok(self
            .snapshot
            .get_pinned_opt(key.as_bytes(), readopts.into())?
            .map(|value| PyBytes::new(py, &value)))
    }

    fn get_cf<'py>(
        &self,
        py: Python<'py>,
        cf: &ColumnFamilyHandle,
        key: &PyBytes,
    ) -> Result<Option<&'py PyBytes>> {
        let _ = self.db.upgrade().ok_or(DBClosedError)?;
        Ok(self
            .snapshot
            .get_pinned_cf(cf, key.as_bytes())?
            .map(|value| PyBytes::new(py, &value)))
    }

    fn get_cf_opt<'py>(
        &self,
        py: Python<'py>,
        cf: &ColumnFamilyHandle,
        key: &PyBytes,
        readopts: &PyCell<ReadOptions>,
    ) -> Result<Option<&'py PyBytes>> {
        let _ = self.db.upgrade().ok_or(DBClosedError)?;
        let readopts = readopts.replace(Default::default());
        Ok(self
            .snapshot
            .get_pinned_cf_opt(cf, key.as_bytes(), readopts.into())?
            .map(|value| PyBytes::new(py, &value)))
    }
}

#[pyclass(module = "rocksdb_pyo3")]
#[derive(Default)]
struct WriteBatch(rocksdb::WriteBatch);

impl Deref for WriteBatch {
    type Target = rocksdb::WriteBatch;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[pymethods]
impl WriteBatch {
    #[new]
    fn new() -> Self {
        Self::default()
    }

    #[staticmethod]
    fn from_data(data: &[u8]) -> Self {
        WriteBatch(rocksdb::WriteBatch::from_data(data))
    }

    fn len(&self) -> usize {
        self.0.len()
    }

    fn size_in_bytes(&self) -> usize {
        self.0.size_in_bytes()
    }

    fn data(&self) -> &[u8] {
        self.0.data()
    }

    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    fn clear(mut slf: PyRefMut<Self>) {
        slf.0.clear()
    }

    // TODO:
    // fn iterate(&self, callbacks: &mut dyn WriteBatchIterator)

    fn put(mut slf: PyRefMut<Self>, key: &PyBytes, value: &PyBytes) {
        slf.0.put(key.as_bytes(), value.as_bytes())
    }

    fn put_cf(mut slf: PyRefMut<Self>, cf: &ColumnFamilyHandle, key: &PyBytes, value: &PyBytes) {
        slf.0.put_cf(cf, key.as_bytes(), value.as_bytes())
    }

    fn merge(mut slf: PyRefMut<Self>, key: &PyBytes, value: &PyBytes) {
        slf.0.merge(key.as_bytes(), value.as_bytes())
    }

    fn merge_cf(mut slf: PyRefMut<Self>, cf: &ColumnFamilyHandle, key: &PyBytes, value: &PyBytes) {
        slf.0.merge_cf(cf, key.as_bytes(), value.as_bytes())
    }

    fn delete(mut slf: PyRefMut<Self>, key: &PyBytes) {
        slf.0.delete(key.as_bytes())
    }

    fn delete_cf(mut slf: PyRefMut<Self>, cf: &ColumnFamilyHandle, key: &PyBytes) {
        slf.0.delete_cf(cf, key.as_bytes())
    }

    fn delete_range(mut slf: PyRefMut<Self>, from: &PyBytes, to: &PyBytes) {
        slf.0.delete_range(from.as_bytes(), to.as_bytes())
    }

    fn delete_range_cf(
        mut slf: PyRefMut<Self>,
        cf: &ColumnFamilyHandle,
        from: &PyBytes,
        to: &PyBytes,
    ) {
        slf.0.delete_range_cf(cf, from.as_bytes(), to.as_bytes())
    }
}

#[pyclass(module = "rocksdb_pyo3")]
struct DB {
    db: Option<Arc<rocksdb::DB>>,
}

impl DB {
    fn new(db: rocksdb::DB) -> Self{
        Self {
            db: Some(Arc::new(db))
        }
    }
}

//impl Default for DB {
//    fn default() -> Self {
//    }
//}

#[pymethods]
impl DB {
    #[staticmethod]
    fn list_cf(opts: PyRef<Options>, path: &str) -> Result<Vec<String>> {
        Ok(rocksdb::DB::list_cf(&opts, path)?.into())
    }

    #[staticmethod]
    fn repair(opts: PyRef<Options>, path: &str) -> Result<()> {
        Ok(rocksdb::DB::repair(&opts, path)?)
    }

    #[staticmethod]
    fn destroy(opts: PyRef<Options>, path: &str) -> Result<()> {
        Ok(rocksdb::DB::destroy(&opts, path)?)
    }

    #[staticmethod]
    fn open(path: &str) -> Result<Self> {
        info!("open default");
        Ok(Self::new(rocksdb::DB::open_default(path)?))
    }

    #[staticmethod]
    fn open_with_opts(path: &str, opts: PyRef<Options>) -> Result<Self> {
        info!("open");
        Ok(Self::new(rocksdb::DB::open(&opts, path)?))
    }

    #[staticmethod]
    fn open_for_read_only(
        path: &str,
        opts: PyRef<Options>,
        error_if_log_file_exist: bool,
        //
    ) -> Result<Self> {
        info!("open for read-only");
        Ok(Self::new(rocksdb::DB::open_for_read_only(&opts, path, error_if_log_file_exist)?))
    }

    #[staticmethod]
    fn open_cf_with_opts(
        path: &str,
        opts: PyRef<Options>,
        cfs: Vec<(&str, PyRef<Options>)>,
    ) -> Result<Self> {
        info!("open cfs with opts");
        let cfs = cfs
            .into_iter()
            .map(|(cf_name, opts)| (cf_name, opts.0.clone()));
        Ok(Self::new(rocksdb::DB::open_cf_with_opts(&opts, path, cfs)?))
    }

    #[staticmethod]
    fn open_cf_with_opts_for_read_only(
        path: &str,
        opts: PyRef<Options>,
        cfs: Vec<(&str, PyRef<Options>)>,
        error_if_log_file_exist: bool,
    ) -> Result<Self> {
        info!("open cfs with opts for read-only");
        let cfs = cfs
            .into_iter()
            .map(|(cf_name, opts)| (cf_name, opts.0.clone()));
        Ok(Self::new(rocksdb::DB::open_cf_with_opts_for_read_only(
            &opts,
            path,
            cfs,
            error_if_log_file_exist,
        )?))
    }

    fn create_cf(slf: PyRefMut<Self>, name: &str, opts: &Options) -> Result<()> {
        Ok(slf.db.as_ref().ok_or(DBClosedError)?.create_cf(name, opts)?)
    }

    fn drop_cf(slf: PyRefMut<Self>, name: &str) -> Result<()> {
        Ok(slf.db.as_ref().ok_or(DBClosedError)?.drop_cf(name)?)
    }

    fn cf_handle(&self, name: &str) -> Result<Option<ColumnFamilyHandle>> {
        let db = self.db.as_ref().ok_or(DBClosedError)?;
        let maybe_handle = db.cf_handle(name);
        info!("cf_hadle: {}, is_none: {}", name, maybe_handle.is_none());
        let handle = match maybe_handle {
            Some(h) => h,
            None => return Ok(None),
        };
        Ok(Some(ColumnFamilyHandle::new(handle, &db)))
    }

    fn cf_handles(&self) -> Result<Vec<ColumnFamilyHandle>> {
        let db = self.db.as_ref().ok_or(DBClosedError)?;
        Ok(db.cf_handles()
            .into_iter()
            .map(|handle| ColumnFamilyHandle::new(handle, &db))
            .collect())
    }

    fn path(&self) -> Result<&Path> {
        Ok(self.db.as_ref().ok_or(DBClosedError)?.path())
    }

    fn snapshot(&self) -> Result<Snapshot> {
        Ok(Snapshot::new(self.db.as_ref().ok_or(DBClosedError)?))
    }

    fn latest_sequence_number(&self) -> Result<u64> {
        Ok(self.db.as_ref().ok_or(DBClosedError)?.latest_sequence_number())
    }

    fn cancel_all_background_work(mut slf: PyRefMut<Self>, wait: bool) {
        if let Some(ref mut db) = slf.db {
            db.cancel_all_background_work(wait)
        }
    }

    fn close(mut slf: PyRefMut<Self>) {
        if let Some(arc_db) = slf.db.take() {
            let _ = Arc::into_inner(arc_db).unwrap();
            info!("close called");
        }
    }

    fn arc_count(&self) -> Result<(usize, usize)> {
        let db = self.db.as_ref().ok_or(DBClosedError)?;
        let strong_count = Arc::strong_count(db);
        let weak_count = Arc::weak_count(db);
        Ok((strong_count, weak_count))
    }

    fn live_files(&self) -> Result<Vec<LiveFile>> {
        Ok(self.db.as_ref().ok_or(DBClosedError)?.live_files()?.into_iter().map(Into::into).collect())
    }

    fn get_column_family_metadata(&self) -> Result<ColumnFamilyMetaData> {
        let cfmd = self.db.as_ref().ok_or(DBClosedError)?.get_column_family_metadata();
        Ok(ColumnFamilyMetaData {
            size: cfmd.size,
            name: cfmd.name,
            file_count: cfmd.file_count,
        })
    }

    fn get_column_family_metadata_cf(&self, cf: &ColumnFamilyHandle) -> Result<ColumnFamilyMetaData> {
        let cfmd = self.db.as_ref().ok_or(DBClosedError)?.get_column_family_metadata_cf(cf);
        Ok(ColumnFamilyMetaData {
            size: cfmd.size,
            name: cfmd.name,
            file_count: cfmd.file_count,
        })
    }

    fn property_value(&self, key: &str) -> Result<Option<String>> {
        Ok(self.db.as_ref().ok_or(DBClosedError)?.property_value(key)?)
    }

    fn property_value_cf(&self, cf: &ColumnFamilyHandle, key: &str) -> Result<Option<String>> {
        Ok(self.db.as_ref().ok_or(DBClosedError)?.property_value_cf(cf, key)?)
    }

    fn get<'py>(&self, py: Python<'py>, key: &PyBytes) -> Result<Option<&'py PyBytes>> {
        Ok(self.db.as_ref().ok_or(DBClosedError)?
            .get_pinned(key.as_bytes())?
            .map(|value| PyBytes::new(py, &value)))
    }

    fn get_opt<'py>(
        &self,
        py: Python<'py>,
        key: &PyBytes,
        readopts: &ReadOptions,
    ) -> Result<Option<&'py PyBytes>> {
        Ok(self.db.as_ref().ok_or(DBClosedError)?
            .get_pinned_opt(key.as_bytes(), readopts)?
            .map(|value| PyBytes::new(py, &value)))
    }

    fn get_cf<'py>(
        &self,
        py: Python<'py>,
        cf: &ColumnFamilyHandle,
        key: &PyBytes,
    ) -> Result<Option<&'py PyBytes>> {
        Ok(self.db.as_ref().ok_or(DBClosedError)?
            .get_pinned_cf(cf, key.as_bytes())?
            .map(|value| PyBytes::new(py, &value)))
    }

    fn get_cf_opt<'py>(
        &self,
        py: Python<'py>,
        cf: &ColumnFamilyHandle,
        key: &PyBytes,
        readopts: &ReadOptions,
    ) -> Result<Option<&'py PyBytes>> {
        Ok(self.db.as_ref().ok_or(DBClosedError)?
            .get_pinned_cf_opt(cf, key.as_bytes(), readopts)?
            .map(|value| PyBytes::new(py, &value)))
    }

    fn multi_get<'py>(
        &self,
        py: Python<'py>,
        keys: Vec<&PyBytes>,
    ) -> Result<Vec<Option<&'py PyBytes>>> {
        Ok(self.db.as_ref().ok_or(DBClosedError)?
            .multi_get(keys.iter().map(|k| k.as_bytes()))
            .into_iter()
            .collect::<core::result::Result<Vec<_>, _>>()?
            .into_iter()
            .map(|opt_value| opt_value.map(|value| PyBytes::new(py, &value)))
            .collect())
    }

    fn multi_get_opt<'py>(
        &self,
        py: Python<'py>,
        keys: Vec<&PyBytes>,
        readopts: &ReadOptions,
    ) -> Result<Vec<Option<&'py PyBytes>>> {
        Ok(self.db.as_ref().ok_or(DBClosedError)?
            .multi_get_opt(keys.iter().map(|k| k.as_bytes()), readopts)
            .into_iter()
            .collect::<core::result::Result<Vec<_>, _>>()?
            .into_iter()
            .map(|opt_value| opt_value.map(|value| PyBytes::new(py, &value)))
            .collect())
    }

    fn multi_get_cf<'py>(
        &self,
        py: Python<'py>,
        keys: Vec<(PyRef<ColumnFamilyHandle>, &PyBytes)>,
    ) -> Result<Vec<Option<&'py PyBytes>>> {
        Ok(self.db.as_ref().ok_or(DBClosedError)?
            .multi_get_cf(keys.iter().map(|(cf, k)| (cf.deref(), k.as_bytes())))
            .into_iter()
            .collect::<core::result::Result<Vec<_>, _>>()?
            .into_iter()
            .map(|opt_value| opt_value.map(|value| PyBytes::new(py, &value)))
            .collect())
    }

    fn multi_get_cf_opt<'py>(
        &self,
        py: Python<'py>,
        keys: Vec<(PyRef<ColumnFamilyHandle>, &PyBytes)>,
        readopts: &ReadOptions,
    ) -> Result<Vec<Option<&'py PyBytes>>> {
        Ok(self.db.as_ref().ok_or(DBClosedError)?
            .multi_get_cf_opt(
                keys.iter().map(|(cf, k)| (cf.deref(), k.as_bytes())),
                readopts,
            )
            .into_iter()
            .collect::<core::result::Result<Vec<_>, _>>()?
            .into_iter()
            .map(|opt_value| opt_value.map(|value| PyBytes::new(py, &value)))
            .collect())
    }

    fn key_may_exist(&self, key: &PyBytes) -> Result<bool> {
        Ok(self.db.as_ref().ok_or(DBClosedError)?.key_may_exist(key.as_bytes()))
    }

    fn key_may_exist_opt(&self, key: &PyBytes, readopts: &ReadOptions) -> Result<bool> {
        Ok(self.db.as_ref().ok_or(DBClosedError)?.key_may_exist_opt(key.as_bytes(), readopts))
    }

    fn key_may_exist_cf(&self, cf: &ColumnFamilyHandle, key: &PyBytes) -> Result<bool> {
        Ok(self.db.as_ref().ok_or(DBClosedError)?.key_may_exist_cf(cf, key.as_bytes()))
    }

    fn key_may_exist_cf_opt(
        &self,
        cf: &ColumnFamilyHandle,
        key: &PyBytes,
        readopts: &ReadOptions,
    ) -> Result<bool> {
        Ok(self.db.as_ref().ok_or(DBClosedError)?.key_may_exist_cf_opt(cf, key.as_bytes(), readopts))
    }

    fn key_may_exist_cf_opt_value<'py>(
        &self,
        py: Python<'py>,
        cf: &ColumnFamilyHandle,
        key: &PyBytes,
        readopts: &ReadOptions,
    ) -> Result<(bool, Option<&'py PyBytes>)> {
        let (may_exist, value) = self.db.as_ref().ok_or(DBClosedError)?.key_may_exist_cf_opt_value(cf, key.as_bytes(), readopts);
        Ok((may_exist, value.map(|v| PyBytes::new(py, v.as_ref()))))
    }

    fn put(&self, key: &PyBytes, value: &PyBytes) -> Result<()> {
        Ok(self.db.as_ref().ok_or(DBClosedError)?.put(key.as_bytes(), value.as_bytes())?)
    }

    fn put_opt(&self, key: &PyBytes, value: &PyBytes, writeopts: &WriteOptions) -> Result<()> {
        Ok(self.db.as_ref().ok_or(DBClosedError)?.put_opt(key.as_bytes(), value.as_bytes(), writeopts)?)
    }

    fn put_cf(&self, cf: &ColumnFamilyHandle, key: &PyBytes, value: &PyBytes) -> Result<()> {
        Ok(self.db.as_ref().ok_or(DBClosedError)?.put_cf(cf, key.as_bytes(), value.as_bytes())?)
    }

    fn put_cf_opt(
        &self,
        cf: &ColumnFamilyHandle,
        key: &PyBytes,
        value: &PyBytes,
        writeopts: &WriteOptions,
    ) -> Result<()> {
        Ok(self.db.as_ref().ok_or(DBClosedError)?.put_cf_opt(cf, key.as_bytes(), value.as_bytes(), writeopts)?)
    }

    fn merge(&self, key: &PyBytes, value: &PyBytes) -> Result<()> {
        Ok(self.db.as_ref().ok_or(DBClosedError)?.merge(key.as_bytes(), value.as_bytes())?)
    }

    fn merge_opt(&self, key: &PyBytes, value: &PyBytes, writeopts: &WriteOptions) -> Result<()> {
        Ok(self.db.as_ref().ok_or(DBClosedError)?.merge_opt(key.as_bytes(), value.as_bytes(), writeopts)?)
    }

    fn merge_cf(&self, cf: &ColumnFamilyHandle, key: &PyBytes, value: &PyBytes) -> Result<()> {
        Ok(self.db.as_ref().ok_or(DBClosedError)?.merge_cf(cf, key.as_bytes(), value.as_bytes())?)
    }

    fn merge_cf_opt(
        &self,
        cf: &ColumnFamilyHandle,
        key: &PyBytes,
        value: &PyBytes,
        writeopts: &WriteOptions,
    ) -> Result<()> {
        Ok(self.db.as_ref().ok_or(DBClosedError)?.merge_cf_opt(cf, key.as_bytes(), value.as_bytes(), writeopts)?)
    }

    fn delete(&self, key: &PyBytes) -> Result<()> {
        Ok(self.db.as_ref().ok_or(DBClosedError)?.delete(key.as_bytes())?)
    }

    fn delete_opt(&self, key: &PyBytes, writeopts: &WriteOptions) -> Result<()> {
        Ok(self.db.as_ref().ok_or(DBClosedError)?.delete_opt(key.as_bytes(), writeopts)?)
    }

    fn delete_cf(&self, cf: &ColumnFamilyHandle, key: &PyBytes) -> Result<()> {
        Ok(self.db.as_ref().ok_or(DBClosedError)?.delete_cf(cf, key.as_bytes())?)
    }

    fn delete_cf_opt(
        &self,
        cf: &ColumnFamilyHandle,
        key: &PyBytes,
        writeopts: &WriteOptions,
    ) -> Result<()> {
        Ok(self.db.as_ref().ok_or(DBClosedError)?.delete_cf_opt(cf, key.as_bytes(), writeopts)?)
    }

    fn flush_wal(&self, sync: bool) -> Result<()> {
        Ok(self.db.as_ref().ok_or(DBClosedError)?.flush_wal(sync)?)
    }

    fn flush(&self) -> Result<()> {
        Ok(self.db.as_ref().ok_or(DBClosedError)?.flush()?)
    }

    fn flush_opt(&self, flushopts: &FlushOptions) -> Result<()> {
        Ok(self.db.as_ref().ok_or(DBClosedError)?.flush_opt(flushopts)?)
    }

    fn flush_cf(&self, cf: &ColumnFamilyHandle) -> Result<()> {
        Ok(self.db.as_ref().ok_or(DBClosedError)?.flush_cf(cf)?)
    }

    fn flush_cf_opt(&self, cf: &ColumnFamilyHandle, flushopts: &FlushOptions) -> Result<()> {
        Ok(self.db.as_ref().ok_or(DBClosedError)?.flush_cf_opt(cf, flushopts)?)
    }

    #[pyo3(signature = (start, end))]
    fn compact_range(&self, start: Option<&PyBytes>, end: Option<&PyBytes>) -> Result<()> {
        Ok(self.db.as_ref().ok_or(DBClosedError)?.compact_range(start.map(|b| b.as_bytes()), end.map(|b| b.as_bytes())))
    }

    #[pyo3(signature = (start, end, compactopts))]
    fn compact_range_opt(
        &self,
        start: Option<&PyBytes>,
        end: Option<&PyBytes>,
        compactopts: &CompactOptions,
    ) -> Result<()> {
        Ok(self.db.as_ref().ok_or(DBClosedError)?.compact_range_opt(
            start.map(|b| b.as_bytes()),
            end.map(|b| b.as_bytes()),
            compactopts,
        ))
    }

    #[pyo3(signature = (cf, start, end))]
    fn compact_range_cf(
        &self,
        cf: &ColumnFamilyHandle,
        start: Option<&PyBytes>,
        end: Option<&PyBytes>,
    ) -> Result<()> {
        Ok(self.db.as_ref().ok_or(DBClosedError)?.compact_range_cf(cf, start.map(|b| b.as_bytes()), end.map(|b| b.as_bytes())))
    }

    #[pyo3(signature = (cf, start, end, compactopts))]
    fn compact_range_cf_opt(
        &self,
        cf: &ColumnFamilyHandle,
        start: Option<&PyBytes>,
        end: Option<&PyBytes>,
        compactopts: &CompactOptions,
    ) -> Result<()> {
        Ok(self.db.as_ref().ok_or(DBClosedError)?.compact_range_cf_opt(
            cf,
            start.map(|b| b.as_bytes()),
            end.map(|b| b.as_bytes()),
            compactopts,
        ))
    }

    fn iterator(&self) -> Result<DBIterator> {
        let db = self.db.as_ref().ok_or(DBClosedError)?;
        let iter = db.raw_iterator();
        Ok(DBIterator::new(iter, &db))
    }

    fn iterator_opt(&self, readopts: &PyCell<ReadOptions>) -> Result<DBIterator> {
        let db = self.db.as_ref().ok_or(DBClosedError)?;
        let readopts = readopts.replace(Default::default());
        let iter = db.raw_iterator_opt(readopts.into());
        Ok(DBIterator::new(iter, &db))
    }

    fn iterator_cf(&self, cf: &ColumnFamilyHandle) -> Result<DBIterator> {
        let db = self.db.as_ref().ok_or(DBClosedError)?;
        let iter = db.raw_iterator_cf(cf);
        Ok(DBIterator::new(iter, &db))
    }

    fn iterator_cf_opt(
        &self,
        cf: &ColumnFamilyHandle,
        readopts: &PyCell<ReadOptions>,
    ) -> Result<DBIterator> {
        let readopts = readopts.replace(Default::default());
        let db = self.db.as_ref().ok_or(DBClosedError)?;
        let iter = db.raw_iterator_cf_opt(cf, readopts.into());
        Ok(DBIterator::new(iter, &db))
    }

    fn write(&self, batch: &PyCell<WriteBatch>) -> Result<()> {
        let batch = batch.replace(Default::default());
        Ok(self.db.as_ref().ok_or(DBClosedError)?.write(batch.0)?)
    }

    fn write_opt(&self, batch: &PyCell<WriteBatch>, writeopts: &WriteOptions) -> Result<()> {
        let batch = batch.replace(Default::default());
        Ok(self.db.as_ref().ok_or(DBClosedError)?.write_opt(batch.0, writeopts)?)
    }

    fn write_without_wal(&self, batch: &PyCell<WriteBatch>) -> Result<()> {
        let batch = batch.replace(Default::default());
        Ok(self.db.as_ref().ok_or(DBClosedError)?.write_without_wal(batch.0)?)
    }
}

impl Drop for DB {
    fn drop(&mut self) {
        info!("drop DB");
    }
}

#[pymodule]
fn rocksdb_pyo3(_py: Python, m: &PyModule) -> PyResult<()> {
    // A good place to install the Rust -> Python logger.
    pyo3_log::init();
    m.add_class::<Cache>()?;
    m.add_class::<ColumnFamilyDescriptor>()?;
    m.add_class::<Env>()?;
    //m.add_class::<DBPath>()?;
    m.add_class::<DBCompressionType>()?;
    m.add_class::<DBRecoveryMode>()?;
    m.add_class::<DBCompactionStyle>()?;
    m.add_class::<ReadTier>()?;
    m.add_class::<LogLevel>()?;
    m.add_class::<Options>()?;
    m.add_class::<ReadOptions>()?;
    m.add_class::<WriteOptions>()?;
    m.add_class::<FlushOptions>()?;
    m.add_class::<BottommostLevelCompaction>()?;
    m.add_class::<CompactOptions>()?;
    m.add_class::<Snapshot>()?;
    m.add_class::<ColumnFamilyHandle>()?;
    m.add_class::<DBIterator>()?;
    m.add_class::<WriteBatch>()?;
    m.add_class::<DB>()?;
    info!("loaded");
    Ok(())
}
