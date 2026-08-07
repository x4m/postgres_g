#!/usr/bin/env ruby

runs = []
current = nil

ARGF.each_line do |line|
  if line.start_with?("RESULT ")
    fields = line.split.drop(1).map { |field| field.split("=", 2) }.to_h
    current = {
      workload: fields.fetch("workload"),
      clients: fields.fetch("clients").to_i,
      round: fields.fetch("round").to_i,
      variant: fields.fetch("variant")
    }
    runs << current
  elsif current && line.start_with?("tps =")
    current[:tps] = line.split[2].to_f
  elsif current && line.start_with?("client_user=")
    line.scan(/(client_user|client_system|elapsed)=([0-9.]+)/).each do |key, value|
      current[key.to_sym] = value.to_f
    end
  elsif current && line.start_with?("cpu_user=")
    line.scan(/(cpu_user|cpu_system|cpu_idle|samples)=([0-9.]+)/).each do |key, value|
      current[key.to_sym] = value.to_f
    end
  end
end

def median(values)
  sorted = values.sort
  n = sorted.length
  return sorted[n / 2] if n.odd?
  (sorted[n / 2 - 1] + sorted[n / 2]) / 2.0
end

abort "incomplete runs" unless runs.length == 120 && runs.all? { |run| run[:tps] }

puts "workload\tclients\tpairs\tbase_median\tpatch_median\tpaired_median_pct\tpaired_min_pct\tpaired_max_pct"
runs.group_by { |run| [run[:workload], run[:clients]] }.sort.each do |key, group|
  pairs = group.group_by { |run| run[:round] }.map do |round, pair|
    by_variant = pair.map { |run| [run[:variant], run] }.to_h
    abort "incomplete pair #{key.inspect} round #{round}" unless by_variant.keys.sort == %w[base patch]
    100.0 * (by_variant["patch"][:tps] / by_variant["base"][:tps] - 1.0)
  end
  base = group.select { |run| run[:variant] == "base" }.map { |run| run[:tps] }
  patch = group.select { |run| run[:variant] == "patch" }.map { |run| run[:tps] }
  puts ([*key, pairs.length, median(base), median(patch), median(pairs), pairs.min, pairs.max].map do |value|
    value.is_a?(Float) ? format("%.4f", value) : value
  end).join("\t")
end

puts "\npaired results"
puts "workload\tclients\tround\tfirst\tbase_tps\tpatch_tps\tdelta_pct"
runs.group_by { |run| [run[:workload], run[:clients], run[:round]] }.sort.each do |key, pair|
  by_variant = pair.map { |run| [run[:variant], run] }.to_h
  delta = 100.0 * (by_variant["patch"][:tps] / by_variant["base"][:tps] - 1.0)
  puts ([*key, pair.first[:variant], by_variant["base"][:tps], by_variant["patch"][:tps], delta].map do |value|
    value.is_a?(Float) ? format("%.4f", value) : value
  end).join("\t")
end
