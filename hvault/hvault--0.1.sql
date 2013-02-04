create or replace function hvault_fdw_validator (text[], oid) 
    returns bool 
    as 'MODULE_PATHNAME' 
    language C strict;

create or replace function hvault_fdw_handler() 
    returns fdw_handler
    as 'MODULE_PATHNAME' 
    language C strict;

create foreign data wrapper hvault_fdw
    validator hvault_fdw_validator
	handler   hvault_fdw_handler;

create server hvault_service foreign data wrapper hvault_fdw;

create foreign table test_foreign (
    num int4
) server hvault_service;
