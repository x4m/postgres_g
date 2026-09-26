# Copyright (c) 2026, PostgreSQL Global Development Group

test_page_store_sources = files(
  'test_page_store.c', 'test_page_history.c', 'test_remote_smgr.c')

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

test_install_data += files(
  'test_page_store.control',
  'test_page_store--1.0.sql',
)

tests += {
  'name': 'test_page_store',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'tap': {
    'tests': [
      't/001_page_service.pl',
      't/002_remote_smgr.pl',
      't/003_page_history.pl',
      't/004_following_compute.pl',
      't/005_history_relations.pl',
    ],
  },
}
