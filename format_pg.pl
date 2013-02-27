#!/usr/bin/perl

use feature qw(switch);

while ($data = <STDIN>) {
    $indent = 0;
    foreach $c (split //, $data) {
        given($c) {
            when(":") { print "\n", "  " x $indent, ":" ; }
            when("(") { $indent++; print $c; }
            when("{") { $indent++; print $c; }
            when(")") { $indent--; print $c; }
            when("}") { $indent--; print "\n", "  " x $indent, $c }
            default { print $c; }
        }
    }    
}


