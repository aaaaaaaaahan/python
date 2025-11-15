convert program to python with duckdb and pyarrow
duckdb for process input file and output parquet&txt
assumed all the input file ady convert to parquet can directly use it

//CIULHRCR JOB MSGCLASS=X,MSGLEVEL=(1,1),REGION=8M,NOTIFY=&SYSUID       JOB47799
//JOBLIB   DD  DSN=DSNR.SDSNEXIT,DISP=SHR
//         DD  DSN=DSN.SDSNLOAD,DISP=SHR
//*--------------------------------------------------------------------
//XCELFILE EXEC SAS609
//SORTWK01 DD UNIT=SYSDA,SPACE=(CYL,(1000,500))
//SORTWK02 DD UNIT=SYSDA,SPACE=(CYL,(1000,500))
//SORTWK03 DD UNIT=SYSDA,SPACE=(CYL,(1000,500))
//CIHRCRVT DD DISP=SHR,DSN=UNLOAD_CIHRCRVT_FB
//OUTFILE  DD DSN=UNLOAD_CIHRCRVT_EXCEL,
//            DISP=(NEW,CATLG,DELETE),
//            UNIT=SYSDA,SPACE=(CYL,(300,300),RLSE),
//            DCB=(LRECL=1500,BLKSIZE=0,RECFM=FB)
//SASLIST  DD SYSOUT=X
//SYSIN    DD *
     DATA CIHRCRVT;
     INFILE CIHRCRVT;
        INPUT  @  1  HRV_MONTH               $6.
               @  7  HRV_BRCH_CODE           $7.
               @ 14  HRV_ACCT_TYPE           $5.
               @ 19  HRV_ACCT_NO             $20.
               @ 39  HRV_CUSTNO              $20.
               @ 59  HRV_CUSTID              $40.
               @ 99  HRV_CUST_NAME           $120.
               @219  HRV_NATIONALITY         $2.
               @221  HRV_ACCT_OPENDATE       $10.
               @231  HRV_OVERRIDING_INDC     $1.
               @232  HRV_OVERRIDING_OFFCR    $10.
               @242  HRV_OVERRIDING_REASON   $100.
               @342  HRV_DOWJONES_INDC       $1.
               @343  HRV_FUZZY_INDC          $1.
               @344  HRV_FUZZY_SCORE         3.2
               @347  HRV_NOTED_BY            $10.
               @357  HRV_RETURNED_BY         $10.
               @367  HRV_ASSIGNED_TO         $10.
               @377  HRV_NOTED_DATE          $26.
               @403  HRV_RETURNED_DATE       $26.
               @429  HRV_ASSIGNED_DATE       $26.
               @455  HRV_COMMENT_BY          $10.
               @465  HRV_COMMENT_DATE        $26.
               @491  HRV_SAMPLING_INDC       $1.
               @492  HRV_RETURN_STATUS       $1.
               @493  HRV_RECORD_STATUS       $1.
               @494  HRV_FUZZY_SCREEN_DATE   $10.
               ;
     RUN;
     PROC SORT  DATA=CIHRCRVT; BY HRV_MONTH
                                  HRV_BRCH_CODE
                                  HRV_ACCT_TYPE
                                  HRV_ACCT_NO
                                  HRV_CUSTNO ;  RUN;

  DATA OUT1 ;
   FILE OUTFILE;
   SET CIHRCRVT;
      DELIM = '|';
      IF _N_ = 1 THEN DO;
        PUT      'DETAIL LISTING FOR CIHRCRVT          ';
        PUT      'MONTH '                 +(-1)DELIM+(-1)
                 'BRCH_CODE '             +(-1)DELIM+(-1)
                 'ACCT_TYPE '             +(-1)DELIM+(-1)
                 'ACCT_NO '               +(-1)DELIM+(-1)
                 'CUSTNO '                +(-1)DELIM+(-1)
                 'CUSTID '                +(-1)DELIM+(-1)
                 'CUST_NAME '             +(-1)DELIM+(-1)
                 'NATIONALITY '           +(-1)DELIM+(-1)
                 'ACCT_OPENDATE '         +(-1)DELIM+(-1)
                 'OVERRIDING_INDC '       +(-1)DELIM+(-1)
                 'OVERRIDING_OFFCR '      +(-1)DELIM+(-1)
                 'OVERRIDING_REASON '     +(-1)DELIM+(-1)
                 'DOWJONES_INDC '         +(-1)DELIM+(-1)
                 'FUZZY_INDC '            +(-1)DELIM+(-1)
                 'FUZZY_SCORE '           +(-1)DELIM+(-1)
                 'NOTED_BY '              +(-1)DELIM+(-1)
                 'RETURNED_BY '           +(-1)DELIM+(-1)
                 'ASSIGNED_TO '           +(-1)DELIM+(-1)
                 'NOTED_DATE '            +(-1)DELIM+(-1)
                 'RETURNED_DATE '         +(-1)DELIM+(-1)
                 'ASSIGNED_DATE '         +(-1)DELIM+(-1)
                 'SAMPLING_INDC '         +(-1)DELIM+(-1)
                 'RETURN_STATUS '         +(-1)DELIM+(-1)
                 'RECORD_STATUS '         +(-1)DELIM+(-1)
                 'FUZZY_SCREEN_DATE '     +(-1)DELIM+(-1)
            ;
       END;
         PUT   HRV_MONTH                   +(-1)DELIM+(-1)
               HRV_BRCH_CODE               +(-1)DELIM+(-1)
               HRV_ACCT_TYPE               +(-1)DELIM+(-1)
               HRV_ACCT_NO                 +(-1)DELIM+(-1)
               HRV_CUSTNO                  +(-1)DELIM+(-1)
               HRV_CUSTID                  +(-1)DELIM+(-1)
               HRV_CUST_NAME               +(-1)DELIM+(-1)
               HRV_NATIONALITY             +(-1)DELIM+(-1)
               HRV_ACCT_OPENDATE           +(-1)DELIM+(-1)
               HRV_OVERRIDING_INDC         +(-1)DELIM+(-1)
               HRV_OVERRIDING_OFFCR        +(-1)DELIM+(-1)
               HRV_OVERRIDING_REASON       +(-1)DELIM+(-1)
               HRV_DOWJONES_INDC           +(-1)DELIM+(-1)
               HRV_FUZZY_SCORE             +(-1)DELIM+(-1)
               HRV_NOTED_BY                +(-1)DELIM+(-1)
               HRV_RETURNED_BY             +(-1)DELIM+(-1)
               HRV_ASSIGNED_TO             +(-1)DELIM+(-1)
               HRV_NOTED_DATE              +(-1)DELIM+(-1)
               HRV_RETURNED_DATE           +(-1)DELIM+(-1)
               HRV_ASSIGNED_DATE           +(-1)DELIM+(-1)
               HRV_SAMPLING_INDC           +(-1)DELIM+(-1)
               HRV_RETURN_STATUS           +(-1)DELIM+(-1)
               HRV_RECORD_STATUS           +(-1)DELIM+(-1)
               HRV_FUZZY_SCREEN_DATE       +(-1)DELIM+(-1)
               ;
  RUN;
