# contrib/pg_btree_compact/meson.build

pg_btree_compact_sources = files(
  'pg_btree_compact.c',
)

if host_system == 'windows'
  pg_btree_compact_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'pg_btree_compact',
    '--FILEDESC', 'pg_btree_compact - compact bloated B-tree indexes',])
endif

pg_btree_compact = shared_module('pg_btree_compact',
  pg_btree_compact_sources,
  kwargs: contrib_mod_args,
)
contrib_targets += pg_btree_compact

install_data(
  'pg_btree_compact.control',
  'pg_btree_compact--1.0.sql',
  kwargs: contrib_data_args,
)

tests += {
  'name': 'pg_btree_compact',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'regress': {
    'sql': [
      'btree_compact',
    ],
  },
}

