duckdb.duckdb.BinderException: Binder Error: No function matches the given name and argument types '*(VARCHAR, INTEGER_LITERAL)'. You might need to add explicit type casts.
        Candidate functions:
        *(TINYINT, TINYINT) -> TINYINT
        *(SMALLINT, SMALLINT) -> SMALLINT
        *(INTEGER, INTEGER) -> INTEGER
        *(BIGINT, BIGINT) -> BIGINT
        *(HUGEINT, HUGEINT) -> HUGEINT
        *(FLOAT, FLOAT) -> FLOAT
        *(DOUBLE, DOUBLE) -> DOUBLE
        *(DECIMAL, DECIMAL) -> DECIMAL
        *(UTINYINT, UTINYINT) -> UTINYINT
        *(USMALLINT, USMALLINT) -> USMALLINT
        *(UINTEGER, UINTEGER) -> UINTEGER
        *(UBIGINT, UBIGINT) -> UBIGINT
        *(UHUGEINT, UHUGEINT) -> UHUGEINT
        *(INTERVAL, DOUBLE) -> INTERVAL
        *(DOUBLE, INTERVAL) -> INTERVAL
        *(BIGINT, INTERVAL) -> INTERVAL
        *(INTERVAL, BIGINT) -> INTERVAL


LINE 10:         CASE WHEN SIGN1 = '-' THEN OUTSTNDAMT * -1
                                                       ^
