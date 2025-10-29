convert program to python with duckdb and pyarrow
duckdb for process input file and output parquet&csv
assumed all the input file ady convert to parquet can directly use it

//CIEBKALS JOB MSGCLASS=X,MSGLEVEL=(1,1),REGION=64M,NOTIFY=&SYSUID      J0143343
//*---------------------------------------------------------------------
//DELETE   EXEC PGM=IEFBR14
//DLT1     DD DSN=CIS.EBANKING.ALIAS,
//            DISP=(MOD,DELETE,DELETE),UNIT=SYSDA,SPACE=(TRK,(0))
//*---------------------------------------------------------------------
//SASLST    EXEC SAS609
//IEFRDER   DD DUMMY
//SORTWK01  DD UNIT=SYSDA,SPACE=(CYL,(200,150))
//SORTWK02  DD UNIT=SYSDA,SPACE=(CYL,(200,150))
//CUSTFILE  DD DISP=SHR,DSN=CIS.CUST.DAILY
//INPFILE   DD DISP=SHR,DSN=UNLOAD.ALLALIAS.FB
//OUTFILE   DD DSN=CIS.EBANKING.ALIAS,
//             DISP=(NEW,CATLG,DELETE),
//             UNIT=SYSDA,SPACE=(CYL,(5,20),RLSE),
//             DCB=(LRECL=150,BLKSIZE=0,RECFM=FB)
//SASLIST   DD SYSOUT=X
//SYSIN     DD *
OPTIONS IMSDEBUG=N YEARCUTOFF=1950 SORTDEV=3390 ERRORS=20;
OPTIONS NODATE NONUMBER NOCENTER;
TITLE;
 DATA CUS;
    KEEP CUSTNO ACCTNOC CUSTNAME ACCTCODE DOBDOR;
    SET CUSTFILE.CUSTDLY;
    IF CUSTNAME EQ '' THEN DELETE;
 RUN;
 PROC SORT  DATA=CUS NODUPKEY; BY CUSTNO;RUN;
 PROC PRINT DATA=CUS(OBS=10);TITLE 'CUSTOMER DATA';RUN;

 DATA ALIAS;
      INFILE INPFILE;
             INPUT  @001  HOLD_CO_NO       PD2.
                    @003  BANK_NO          PD2.
                    @005  CUSTNO           $20.
                    @034  PROCESS_TIME     $8.
                    @053  KEY_FIELD_1      $15.
                    @089  NAME_LINE        $40.
                    @343  LAST_CHANGE      $10.;
      IF KEY_FIELD_1 = 'PP';
 RUN;
 PROC SORT  DATA=ALIAS;
   BY CUSTNO
      DESCENDING LAST_CHANGE
      DESCENDING PROCESS_TIME ; RUN;
 PROC PRINT DATA=ALIAS; TITLE 'ALIAS FILE' ; RUN;
 /*-----------------------------------------------------------*/
 /*  MATCH EMAIL DATASET AND CUST DAILY                       */
 /*-----------------------------------------------------------*/
 DATA MATCH;
 MERGE ALIAS(IN=A)  CUS(IN=B);
       BY CUSTNO;
       IF B AND A;
 RUN;
 PROC SORT  DATA=MATCH; BY CUSTNO ;RUN;
 PROC PRINT DATA=MATCH(OBS=10) ;TITLE 'MATCH DATASET';RUN;

 DATA OUT;
    SET MATCH;BY CUSTNO;
    FILE OUTFILE;
    IF FIRST.CUSTNO THEN DO;
         PUT @001     BANK_NO             $Z3.
             @005     CUSTNO              $20.
             @017     NAME_LINE           $48.
             @066     DOBDOR              $10.
             ;
    END;
 RUN;
