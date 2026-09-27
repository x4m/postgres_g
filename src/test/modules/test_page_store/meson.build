# Copyright (c) 2026, PostgreSQL Global Development Group

test_page_store_sources = files(
  'test_page_store.c', 'test_page_history.c', 'test_page_protocol.c',
  'test_page_worker.c', 'test_page_overlay.c', 'test_wal_durability.c',
  'test_wal_store.c',
  'test_wal_sender.c',
  'test_remote_smgr.c')

if host_system == 'windows'
  test_page_store_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'test_page_store',
    '--FILEDESC', 'test_page_store - replay page service prototype',])
endif

test_page_store = shared_module('test_page_store',
  test_page_store_sources,
  kwargs: pg_test_mod_args + {
    'dependencies': pg_test_mod_args['dependencies'] + [libpq],
  },
)
test_install_libs += test_page_store

test_wal_fetch = executable('test_wal_fetch',
  files('test_wal_fetch.c') + xlogreader_sources,
  dependencies: [frontend_code, libpq, lz4, zstd],
  c_args: ['-DFRONTEND'],
  kwargs: default_bin_args + {'install': false},
)
testprep_targets += test_wal_fetch

test_install_data += files(
  'test_page_store.control',
  'test_page_store--1.0.sql',
)

tests += {
  'name': 'test_page_store',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'tap': {
    'deps': [test_wal_fetch],
    'env': {
      'enable_injection_points': get_option('injection_points') ? 'yes' : 'no',
    },
    'tests': [
      't/001_page_service.pl',
      't/002_remote_smgr.pl',
      't/003_page_history.pl',
      't/004_following_compute.pl',
      't/005_history_relations.pl',
      't/006_following_relations.pl',
      't/007_durable_history.pl',
      't/008_following_btree.pl',
      't/009_history_forks.pl',
      't/010_following_vm.pl',
      't/011_compute_restart.pl',
      't/012_compute_seed.pl',
      't/013_physical_service.pl',
      't/014_transport_worker.pl',
      't/015_disposable_fsm.pl',
      't/016_writer_overlay.pl',
      't/017_wal_durability.pl',
      't/018_wal_store.pl',
      't/019_early_wal_sender.pl',
      't/020_inbox_recovery.pl',
      't/021_wal_record_frontier.pl',
      't/022_child_wal_epoch.pl',
      't/023_writer_activation.pl',
    ],
  },
}
