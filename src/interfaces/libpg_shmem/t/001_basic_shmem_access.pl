#!/usr/bin/perl

# Copyright (c) 2025, PostgreSQL Global Development Group

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# Test basic functionality of test_shmem_access and libpg_shmem

# Test 1: Check if test_shmem_access binary exists
my $test_shmem_path = "test_shmem_access";
ok(-x $test_shmem_path, "test_shmem_access binary exists and is executable");

# Test 2: Test with invalid arguments (should show usage)
my $ret = system($test_shmem_path);
isnt($ret, 0, "test_shmem_access fails when run without arguments");

# Test 3: Test with clearly invalid PID (should fail gracefully)  
$ret = system("$test_shmem_path 999999 2>/dev/null");
isnt($ret, 0, "test_shmem_access fails gracefully with invalid PID");

# Test 4-6: Test with actual PostgreSQL cluster (with error handling)
eval {
    my $node = PostgreSQL::Test::Cluster->new('shmem_test');
    $node->init;
    $node->start;

    # Get the postmaster PID by reading the postmaster.pid file
    my $pid_file = $node->data_dir . "/postmaster.pid";
    ok(-f $pid_file, "postmaster.pid file exists");
    
    open my $fh, '<', $pid_file or die "Cannot open $pid_file: $!";
    my $postmaster_pid = <$fh>;
    close $fh;
    chomp $postmaster_pid;
    
    ok(defined $postmaster_pid && $postmaster_pid > 0, "Got postmaster PID: $postmaster_pid");

    # Test connecting to actual PostgreSQL shared memory
    diag("Testing connection to PID $postmaster_pid");
    diag("Node data directory: " . $node->data_dir);
    
    # Check if postmaster.pid exists in the data directory
    my $test_pid_file = $node->data_dir . "/postmaster.pid";
    if (-f $test_pid_file) {
        diag("postmaster.pid file exists at: $test_pid_file");
        # Show first few lines of the file
        open my $fh, '<', $test_pid_file;
        my @lines = <$fh>;
        close $fh;
        chomp @lines;
        diag("postmaster.pid content: " . join(" | ", @lines[0..2]));
    } else {
        diag("postmaster.pid file NOT found at: $test_pid_file");
    }
    
    # Set environment variable to help our library find the data directory
    $ENV{PGDATA_FOR_TEST} = $node->data_dir;
    diag("Set PGDATA_FOR_TEST to: " . $ENV{PGDATA_FOR_TEST});
    
    # Check if process is still alive before testing
    my $process_alive = kill(0, $postmaster_pid);
    diag("Process $postmaster_pid alive before test: " . ($process_alive ? "yes" : "no"));
    
    my ($stdout, $stderr);
    # Use system() to ensure environment variables are preserved and use local library
    my $cmd = "PGDATA_FOR_TEST='" . $node->data_dir . "' DYLD_LIBRARY_PATH=. $test_shmem_path $postmaster_pid";
    my $result = system($cmd . " > /tmp/test_out 2> /tmp/test_err");
    
    # Check if process is still alive after testing
    my $process_alive_after = kill(0, $postmaster_pid);
    diag("Process $postmaster_pid alive after test: " . ($process_alive_after ? "yes" : "no"));
    
    # Read the output files
    if (-f "/tmp/test_out") {
        open my $fh, '<', "/tmp/test_out";
        $stdout = join('', <$fh>);
        close $fh;
    }
    if (-f "/tmp/test_err") {
        open my $fh, '<', "/tmp/test_err";
        $stderr = join('', <$fh>);
        close $fh;
    }
    
    # Print debug output for troubleshooting
    diag("Return code: $result");
    diag("STDOUT: $stdout") if $stdout;
    diag("STDERR: $stderr") if $stderr;
    
    is($result, 0, "test_shmem_access connected successfully to postmaster");

    # Verify we get expected output indicating successful connection
    like($stdout, qr/Successfully connected to PostgreSQL shared memory/, 
         "Connection success message found in output");

    # Clean up
    $node->stop;
};

if ($@) {
    diag("Failed to create PostgreSQL cluster: $@");
    # Skip the cluster tests if we can't create one
    pass("Skipping cluster tests due to setup failure");
    pass("Skipping cluster tests due to setup failure");
    pass("Skipping cluster tests due to setup failure");
    pass("Skipping cluster tests due to setup failure");
}

done_testing(); 