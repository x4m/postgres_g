#!/usr/bin/env ruby

lookup = []
pgbench = []
insert = []
current = nil

ARGF.each_line do |line|
  case line
  when /^LOOKUP workload=(\S+) round=(\d+) variant=(\S+)/
    current = {kind: :lookup, workload: $1, round: $2.to_i, variant: $3}
    lookup << current
  when /^PGBENCH workload=(\S+) clients=(\d+) round=(\d+) variant=(\S+)/
    current = {kind: :pgbench, workload: $1, clients: $2.to_i,
               round: $3.to_i, variant: $4}
    pgbench << current
  when /^INSERT round=(\d+) variant=(\S+)/
    current = {kind: :insert, round: $1.to_i, variant: $2}
    insert << current
  when /^elapsed=([0-9.]+)/
    current[:value] = $1.to_f if current && current[:kind] != :pgbench
  when /^tps = ([0-9.]+)/
    current[:value] = $1.to_f if current && current[:kind] == :pgbench
  end
end

def median(values)
  sorted = values.sort
  n = sorted.length
  n.odd? ? sorted[n / 2] : (sorted[n / 2 - 1] + sorted[n / 2]) / 2.0
end

def summarize(groups, lower_is_better)
  groups.sort.each do |key, runs|
    pairs = runs.group_by { |run| run[:round] }.sort.map do |round, group|
      variants = group.map { |run| [run[:variant], run[:value]] }.to_h
      abort "incomplete #{key.inspect} round #{round}" unless
        variants.keys.sort == %w[interp master special] && variants.values.none?(&:nil?)
      special = lower_is_better ?
        100.0 * (variants["master"] / variants["special"] - 1.0) :
        100.0 * (variants["special"] / variants["master"] - 1.0)
      interp = lower_is_better ?
        100.0 * (variants["special"] / variants["interp"] - 1.0) :
        100.0 * (variants["interp"] / variants["special"] - 1.0)
      total = lower_is_better ?
        100.0 * (variants["master"] / variants["interp"] - 1.0) :
        100.0 * (variants["interp"] / variants["master"] - 1.0)
      [special, interp, total]
    end
    medians = 3.times.map { |i| median(pairs.map { |pair| pair[i] }) }
    ranges = 3.times.map do |i|
      values = pairs.map { |pair| pair[i] }
      [values.min, values.max]
    end
    values = %w[master special interp].map do |variant|
      median(runs.select { |run| run[:variant] == variant }.map { |run| run[:value] })
    end
    puts ([*key, pairs.length, *values, *medians,
           *ranges.flatten].map { |v| v.is_a?(Float) ? format("%.3f", v) : v }.join("\t"))
  end
end

abort "lookup count #{lookup.length}" unless lookup.length == 231
abort "pgbench count #{pgbench.length}" unless pgbench.length == 90
abort "insert count #{insert.length}" unless insert.length == 15

puts "LOOKUP"
puts "workload\truns\tmaster_s\tspecial_s\tinterp_s\tspecial_pct\tinterp_pct\ttotal_pct\tspecial_min\tspecial_max\tinterp_min\tinterp_max\ttotal_min\ttotal_max"
summarize(lookup.group_by { |run| [run[:workload]] }, true)

puts "\nPGBENCH"
puts "workload\tclients\truns\tmaster_tps\tspecial_tps\tinterp_tps\tspecial_pct\tinterp_pct\ttotal_pct\tspecial_min\tspecial_max\tinterp_min\tinterp_max\ttotal_min\ttotal_max"
summarize(pgbench.group_by { |run| [run[:workload], run[:clients]] }, false)

puts "\nINSERT"
puts "runs\tmaster_s\tspecial_s\tinterp_s\tspecial_pct\tinterp_pct\ttotal_pct\tspecial_min\tspecial_max\tinterp_min\tinterp_max\ttotal_min\ttotal_max"
summarize({[] => insert}, true)
