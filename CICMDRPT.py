df = df.with_columns([
    # Pad COSTCENTER to 7 characters (like Z7.)
    pl.col("COSTCENTER").cast(pl.Utf8).str.zfill(7).alias("COSTCENTERX"),

    # Extract substring (chars 5-7, SAS is 1-based so adjust → slice(4,3))
    pl.col("COSTCENTER").cast(pl.Utf8).str.zfill(7).str.slice(4, 3).cast(pl.Int32).alias("COSTCTR"),

    # Pad COSTCTR to 3 characters (like Z3.)
    pl.col("COSTCENTER").cast(pl.Utf8).str.zfill(7).str.slice(4, 3).alias("COSTCTR1"),

    # Account Open Date: take first 8 chars from 11-char padded string
    pl.col("ACCTOPENDATE").cast(pl.Utf8).str.zfill(11).str.slice(0, 8).alias("ACCTOPNDT"),

    # Extract MM/DD/YYYY from ACCTOPNDT
    pl.col("ACCTOPENDATE").cast(pl.Utf8).str.zfill(11).str.slice(0, 8).str.slice(0, 2).alias("OPENMM"),
    pl.col("ACCTOPENDATE").cast(pl.Utf8).str.zfill(11).str.slice(0, 8).str.slice(2, 2).alias("OPENDD"),
    pl.col("ACCTOPENDATE").cast(pl.Utf8).str.zfill(11).str.slice(0, 8).str.slice(4, 4).alias("OPENYY"),

    # Build DATEOPEN = YYYY||MM||DD
    (pl.col("ACCTOPENDATE").cast(pl.Utf8).str.zfill(11).str.slice(0, 8).str.slice(4, 4) +
     pl.col("ACCTOPENDATE").cast(pl.Utf8).str.zfill(11).str.slice(0, 8).str.slice(0, 2) +
     pl.col("ACCTOPENDATE").cast(pl.Utf8).str.zfill(11).str.slice(0, 8).str.slice(2, 2)
    ).alias("DATEOPEN"),

    # Last Transaction Date: same logic as ACCTOPENDATE
    pl.col("LASTTRANDATE").cast(pl.Utf8).str.zfill(11).str.slice(0, 8).alias("LASTTRNDT"),
    pl.col("LASTTRANDATE").cast(pl.Utf8).str.zfill(11).str.slice(0, 8).str.slice(0, 2).alias("LTRNMM"),
    pl.col("LASTTRANDATE").cast(pl.Utf8).str.zfill(11).str.slice(0, 8).str.slice(2, 2).alias("LTRNDD"),
    pl.col("LASTTRANDATE").cast(pl.Utf8).str.zfill(11).str.slice(0, 8).str.slice(4, 4).alias("LTRNYY"),

    # DATECLSE = YYYY||MM||DD
    (pl.col("LASTTRANDATE").cast(pl.Utf8).str.zfill(11).str.slice(0, 8).str.slice(4, 4) +
     pl.col("LASTTRANDATE").cast(pl.Utf8).str.zfill(11).str.slice(0, 8).str.slice(0, 2) +
     pl.col("LASTTRANDATE").cast(pl.Utf8).str.zfill(11).str.slice(0, 8).str.slice(2, 2)
    ).alias("DATECLSE"),

    # Ledger balance
    (pl.col("NOTECURBAL") / 100).alias("LEDGERBAL"),

    # ACCTBRCH = COSTCTR1
    pl.col("COSTCENTER").cast(pl.Utf8).str.zfill(7).str.slice(4, 3).alias("ACCTBRCH"),
])
