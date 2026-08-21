#!/usr/bin/env perl
use strict;
use warnings;
use Socket qw(AF_UNIX SOCK_STREAM sockaddr_un);

if (@ARGV != 3) {
    die "usage: probe-postgresql.pl <unix-socket> <user> <database>\n";
}

my ($socket_path, $user, $database) = @ARGV;
socket(my $socket, AF_UNIX, SOCK_STREAM, 0)
    or die "socket failed: $!\n";
connect($socket, sockaddr_un($socket_path))
    or die "connect failed: $!\n";

my $parameters = "user\0$user\0database\0$database\0client_encoding\0UTF8\0\0";
my $message = pack("N", 8 + length($parameters)) . pack("N", 196608) . $parameters;
my $offset = 0;
while ($offset < length($message)) {
    my $written = syswrite($socket, $message, length($message) - $offset, $offset);
    defined($written) && $written > 0
        or die "write failed: $!\n";
    $offset += $written;
}

my $header = "";
while (length($header) < 5) {
    my $read = sysread($socket, $header, 5 - length($header), length($header));
    defined($read) && $read > 0
        or die "server closed before a PostgreSQL response\n";
}

my ($message_type, $message_length) = unpack("aN", $header);
$message_type eq "R" && $message_length >= 8
    or die "unexpected PostgreSQL response\n";

print "PostgreSQL authentication response received\n";
