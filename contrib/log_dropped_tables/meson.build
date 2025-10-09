log_dropped_tables_sources = files(
  'log_dropped_tables.c',
)

if host_system == 'windows'
  log_dropped_tables_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'log_dropped_tables',
    '--FILEDESC', 'log_dropped_tables - log dropped tables with commit LSN',])
endif

log_dropped_tables = shared_module('log_dropped_tables',
  log_dropped_tables_sources,
  kwargs: contrib_mod_args,
)
contrib_targets += log_dropped_tables

install_data(
  'log_dropped_tables.control',
  'log_dropped_tables--1.0.sql',
  kwargs: contrib_data_args,
)

