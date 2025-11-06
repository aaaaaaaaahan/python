convert program to python with duckdb and pyarrow
duckdb for process input file and output parquet&csv
assumed all the input file ady convert to parquet can directly use it

//CIHRCYRP JOB MSGCLASS=X,MSGLEVEL=(1,1),REGION=64M,NOTIFY=&SYSUID      JOB12401
//*--------------------------------------------------------------------
//*ESMR 2016-3254
//*-GENERATE YEARLY HRC SUMMARY REPORT BY STATUS (NOTED & PENDING NOTE)
//*--------------------------------------------------------------------
//YLY#REPT EXEC SAS609
//* UNLOAD JOB FROM CIULHRCA
//HRCUNLD  DD DISP=SHR,DSN=UNLOAD.CIHRCAPT.FB
//CTRLDATE DD DISP=SHR,DSN=SRSCTRL1(0)
//OUTFILE  DD DSN=CISHRC.STATUS.YEARLY(+1),
//            DISP=(NEW,CATLG,DELETE),SPACE=(CYL,(1,5),RLSE),
//            UNIT=SYSDA,DCB=(LRECL=350,BLKSIZE=0,RECFM=FB)
//SASLIST  DD SYSOUT=X
//SORTWK01 DD UNIT=SYSDA,SPACE=(CYL,(200,100))
//SORTWK02 DD UNIT=SYSDA,SPACE=(CYL,(200,100))
//SORTWK03 DD UNIT=SYSDA,SPACE=(CYL,(200,100))
//SORTWK04 DD UNIT=SYSDA,SPACE=(CYL,(200,100))
//SORTWK05 DD UNIT=SYSDA,SPACE=(CYL,(200,100))
//SORTWK06 DD UNIT=SYSDA,SPACE=(CYL,(200,100))
//SYSIN    DD *
OPTIONS IMSDEBUG=N YEARCUTOFF=1950 SORTDEV=3390 ERRORS=0;
OPTIONS NODATE NONUMBER NOCENTER;
TITLE;
 /*----------------------------------------------------------------*/
 /*    GET SAS DATE AND TODAY REPORTING DATE                       */
 /*----------------------------------------------------------------*/
 DATA SRSDATE;
    INFILE CTRLDATE;
    INPUT @001  SRSYY    4.
          @005  SRSMM    2.
          @007  SRSDD    2.;
          /* DISPLAY TODAY REPORTING DATE*/
          TODAYSAS=MDY(SRSMM,SRSDD,SRSYY);
          TODAY=PUT(TODAYSAS,YYMMDD10.);
          CALL SYMPUT('TODAYDATE',TODAY);
          CALL SYMPUT('YYYYMM',PUT(TODAYSAS,YYMMD7.));
          CALL SYMPUT('YYYY',PUT(TODAYSAS,YEAR4.));
 RUN;
 PROC PRINT DATA=SRSDATE(OBS=5);TITLE 'DATE';RUN;
 /*----------------------------------------------------------------*/
 /*    INPUT FILE DATA DECLARATION                                 */
 /*----------------------------------------------------------------*/
 DATA HRCRECS;
    INFILE HRCUNLD;
    INPUT  @001  ALIAS             $40.
           @041  BRCHCODE           $7.
           @048  ACCTTYPE           $5.
           @053  APPROVALSTATUS     $2.
           @055  ACCTNO            $20.
           @075  CISNO             $20.
           @095  CREATIONDATE      $10.
           @105  PRIMARYJOINT      $40.
           @145  CISJOINTID1       $40.
           @185  CISJOINTID2       $40.
           @225  CISJOINTID3       $40.
           @265  CISJOINTID4       $40.
           @305  CISJOINTID5       $40.
           @345  CUSTTYPE           $1.
           @346  CUSTNAME         $120.
           @466  CUSTGENDER        $10.
           @476  CUSTDOBDOR        $10.
           @486  CUSTEMPLOYER     $120.
           @606  CUSTADDR1         $40.
           @646  CUSTADDR2         $40.
           @686  CUSTADDR3         $40.
           @726  CUSTADDR4         $40.
           @766  CUSTADDR5         $40.
           @806  CUSTPHONE         $15.
           @821  CUSTPEP            $1.
           @822  DTCORGUNIT        $10.
           @832  DTCINDUSTRY       $10.
           @842  DTCNATION         $10.
           @852  DTCOCCUP          $10.
           @862  DTCACCTTYPE       $10.
           @872  DTCCOMPFORM       $10.
           @882  DTCWEIGHTAGE       $1.
           @883  DTCTOTAL           $5.
           @888  DTCSCORE1          $5.
           @893  DTCSCORE2          $5.
           @898  DTCSCORE3          $5.
           @903  DTCSCORE4          $5.
           @908  DTCSCORE5          $5.
           @913  DTCSCORE6          $5.
           @918  ACCTPURPOSE        $5.
           @923  ACCTREMARKS       $60.
           @983  SOURCEFUND         $5.
           @988  SOURCEDETAILS     $60.
           @1048 PEPINFO          $150.
           @1198 PEPWEALTH         $60.
           @1258 PEPFUNDS          $60.
           @1320 BRCHRECOMDETAILS $900.
           @2220 BRCHEDITOPER       $8.
           @2228 BRCHAPPROVEOPER    $8.
           @2236 BRCHCOMMENTS     $150.
           @2386 BRCHREWORK       $150.
           @2536 HOVERIFYOPER       $8.
           @2544 HOVERIFYDATE      $10.
           @2554 HOVERIFYCOMMENTS $150.
           @2704 HOVERIFYREMARKS  $150.
           @2854 HOVERIFYREWORK   $150.
           @3004 HOAPPROVEOPER      $8.
           @3012 HOAPPROVEDATE     $10.
           @3022 HOAPPROVEREMARKS $150.
           @3172 HOCOMPLYREWORK   $150.
           @3322 UPDATEDATE        $10.
           @3332 UPDATETIME         $8.;

           UPDDATE = SUBSTR(UPDATEDATE,1,4);
           UPDDATE = INPUT(UPDATEDATE,$4.);
           IF "&YYYY" NE UPDDATE THEN DELETE;
           IF ACCTTYPE NOT IN('CA','SA','SDB','FD','FC','FCI','O','FDF')
              THEN DELETE;
        /* IF "&YYYYMM" EQ UPDDATE THEN OUTPUT; */

           HOEPDNOTE = 0;
           HOENOTED  = 0;
           TOTAL     = 0;

           IF APPROVALSTATUS NE '08' THEN DELETE;
           IF ACCTNO NE ' ' AND
              INDEX(HOVERIFYREMARKS,'Noted by')<= 0
              THEN HOEPDNOTE = 1;
           IF ACCTNO NE ' ' AND
              INDEX(HOVERIFYREMARKS,'Noted by')> 0
              THEN HOENOTED = 1;
 RUN;
 PROC SORT  DATA=HRCRECS; BY BRCHCODE ;RUN;
 PROC PRINT DATA=HRCRECS(OBS=5);TITLE 'MASTER HRC RECORDS';RUN;

 /*----------------------------------------------------------------*/
 /*  PROC SUMMARY FOR ALL APPROVAL COUNTS                          */
 /*----------------------------------------------------------------*/
 PROC SUMMARY DATA=HRCRECS;
 BY BRCHCODE;
 VAR HOEPDNOTE HOENOTED;
 OUTPUT OUT=TEMP (DROP=_TYPE_ RENAME=(_FREQ_=TOTAL))
        SUM=HOEPDNOTE HOENOTED;
        RUN;

 DATA TEMP;
    SET TEMP;
    TOTALX = SUM(HOEPDNOTE,HOENOTED);
 RUN;
 PROC SORT  DATA=TEMP; BY BRCHCODE ;RUN;
 PROC PRINT DATA=TEMP(OBS=5);TITLE 'REPORT SUMMARY';RUN;

 /*----------------------------------------------------------------*/
 /* OUTPUT YEARLY SUMMARY REPORT DATA                              */
 /*----------------------------------------------------------------*/
 DATA OUTRECS;
   SET TEMP;
   FILE OUTFILE;
   IF _N_ = 1 THEN
   PUT  @001  'BRANCH'
        @010  'HOE PEND NOTE'
        @030  'HOE NOTED'
        @050  'TOTAL';
   PUT  @001  BRCHCODE           $7.
        @008  ', '
        @010  HOEPDNOTE          Z8.
        @028  ', '
        @030  HOENOTED           Z8.
        @048  ', '
        @051  TOTALX             Z8.;
   RUN;
