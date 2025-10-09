convert program to python with duckdb and pyarrow
duckdb for process input file
pyarrow use for output
assumed all the input file ady convert to parquet can directly use it

//CISUMREP JOB MSGCLASS=X,MSGLEVEL=(1,1),REGION=64M,NOTIFY=&SYSUID      JOB24841
//*--------------------------------------------------------------------
//INITDS   EXEC PGM=IEFBR14
//DEL1     DD DSN=CIS.CIREPTTT.SUMMARY,
//            DISP=(MOD,DELETE,DELETE),SPACE=(TRK,(0))
//*--------------------------------------------------------------------
//*--- PROCESSING
//*--------------------------------------------------------------------
//GET#RECS EXEC SAS609
//* UNLOAD JOB FROM CIULREPT
//REPTFILE DD DISP=SHR,DSN=UNLOAD.CIREPTTT.FB
//*CTRLDATE DD DISP=SHR,DSN=SRSCTRL1(0)
//OUTFILE  DD DSN=CIS.CIREPTTT.SUMMARY,
//            DISP=(NEW,CATLG,DELETE),SPACE=(CYL,(10,10),RLSE),
//            UNIT=SYSDA,DCB=(LRECL=200,BLKSIZE=0,RECFM=FB)
//SASLIST  DD SYSOUT=X
//SORTWK01 DD UNIT=SYSDA,SPACE=(CYL,(200,100))
//SORTWK02 DD UNIT=SYSDA,SPACE=(CYL,(200,100))
//SORTWK03 DD UNIT=SYSDA,SPACE=(CYL,(200,100))
//SORTWK04 DD UNIT=SYSDA,SPACE=(CYL,(200,100))
//SORTWK05 DD UNIT=SYSDA,SPACE=(CYL,(200,100))
//SORTWK06 DD UNIT=SYSDA,SPACE=(CYL,(200,100))
//SYSIN    DD *
OPTIONS IMSDEBUG=N YEARCUTOFF=1950 SORTDEV=3390 ERRORS=5;
OPTIONS NODATE NONUMBER NOCENTER;
TITLE;
 /*----------------------------------------------------------------*/
 /*    INPUT FILE DATA DECLARATION                                 */
 /*----------------------------------------------------------------*/
DATA HRCDATA XHRCDATA;
  INFILE REPTFILE; FORMAT CNTVIEW 8.;
  INPUT  @001  BANKNO            PD2.
         @003  RECTYPE            $5.
         @008  APPLCODE           $5.
         @013  APPLNO            $20.
         @033  NOTENO            $10.
         @043  REPORTDATE        $10.
         @053  REPORTNO          $20.
         @073  BRANCHNO           $7.
         @080  NAME              $60.
         @140  CODE1              $5.
         @145  CODE2              $5.
         @150  CODE3              $5.
         @155  CODE4              $5.
         @160  CODE5              $5.
         @165  AMOUNT1           $20.
         @185  AMOUNT2           $20.
         @205  AMOUNT3           $20.
         @225  AMOUNT4           $20.
         @245  AMOUNT5           $20.
         @265  DATE1             $10.
         @275  DATE2             $10.
         @285  DATE3             $10.
         @295  DATE4             $10.
         @305  DATE5             $10.
         @315  REMARK1           $25.
         @340  REMARK2           $25.
         @365  REMARK3           $25.
         @390  REMARK4           $25.
         @415  REMARK5           $25.
         @440  VIEWED             $1.
         @441  CUSTASSESS         $1.
         @442  BRCHCOMMENTS      $40.
         @482  BRCHREVIEW        $30.
         @512  BRCHCHECK         $30.
         @542  HOCOMMENTS        $40.
         @582  HOREVIEW          $30.
         @612  HOCHECK           $30.
         @642  CUSTOCCUP          $5.
         @647  CUSTNATURE         $5.
         @652  CUSTEMPLOYER      $40.
         @692  INDORG             $1.
         @693  OCCUPDESC         $40.
         @733  NATUREDESC        $40.
         @773  VIEWOFFICER        $1.
         @774  REVIEWED           $1.;

         IF RECTYPE EQ 'DPST' AND APPLCODE EQ 'DP' AND
            REMARK3 IN ('126','127','128','129','140','141','142',
                        '143','144','145','146','147','148','149',
                        '171','172','173')
         THEN DELETE;

         IF (REVIEWED EQ 'Y' OR VIEWED EQ 'Y')
         THEN CNTVIEW = 1;
         ELSE CNTVIEW = 0;

         IF RECTYPE EQ 'DPST' AND REMARK1 NE ' '
            AND REMARK2 NE ' ' THEN OUTPUT HRCDATA;
         ELSE OUTPUT XHRCDATA;

 PROC SORT  DATA=HRCDATA;
 BY BANKNO RECTYPE REPORTDATE REPORTNO BRANCHNO; RUN;
 PROC SORT  DATA=XHRCDATA;
 BY BANKNO RECTYPE REPORTDATE REPORTNO BRANCHNO; RUN;
 PROC PRINT DATA=HRCDATA(OBS=5);TITLE 'HRC REPORT DATA';RUN;
 PROC PRINT DATA=XHRCDATA(OBS=5);TITLE 'NOT HRC REPORT DATA';RUN;

 /*----------------------------------------------------------------*/
 /*  PROC SUMMARY FOR HRC DATA                                     */
 /*----------------------------------------------------------------*/
 PROC SUMMARY DATA=HRCDATA;
 BY BANKNO RECTYPE REPORTDATE REPORTNO BRANCHNO;
 VAR CNTVIEW;
 OUTPUT OUT=TEMP (DROP=_TYPE_ RENAME=(_FREQ_=TOTAL))
                  SUM=CNTVIEW; RUN;
 PROC SORT  DATA=TEMP; BY BRANCHNO ;RUN;
 PROC PRINT DATA=TEMP(OBS=5);TITLE 'HRC SUMMARY';RUN;

 /*----------------------------------------------------------------*/
 /*  PROC SUMMARY FOR NON HRC REPORT DATA                          */
 /*----------------------------------------------------------------*/
 PROC SUMMARY DATA=XHRCDATA;
 BY BANKNO RECTYPE REPORTDATE REPORTNO BRANCHNO;
 VAR CNTVIEW;
 OUTPUT OUT=TEMP1(DROP=_TYPE_ RENAME=(_FREQ_=TOTAL)) SUM=CNTVIEW; RUN;
 PROC SORT  DATA=TEMP1; BY BRANCHNO ;RUN;
 PROC PRINT DATA=TEMP1(OBS=5);TITLE 'NON-HRC SUMMARY';RUN;

 /*----------------------------------------------------------------*/
 /* SELECT ONLY VIEWED RECORDS < 100% FOR ALL HRC RECORDS          */
 /*----------------------------------------------------------------*/
 DATA HRCRECS;
     SET TEMP; FORMAT PTAGE 8.2 ISHRC $1.;
     ISHRC = 'Y';
     PTAGE = (CNTVIEW * 100) / TOTAL;
     IF PTAGE < 100 THEN OUTPUT;
 PROC SORT  DATA=HRCRECS;
 BY BANKNO RECTYPE REPORTDATE REPORTNO BRANCHNO; RUN;
 PROC PRINT DATA=HRCRECS(OBS=5);TITLE 'HRC RECORDS';RUN;

 /*----------------------------------------------------------------*/
 /* SELECT ONLY VIEWED RECORDS < 10% FOR ALL NON-HRC RECORDS        */
 /*----------------------------------------------------------------*/
 DATA NONHRCRECS;
     SET TEMP1; FORMAT PTAGE 8.2 ISHRC $1.;
     ISHRC = 'N';
     PTAGE = (CNTVIEW * 100) / TOTAL;
     IF PTAGE < 10 THEN OUTPUT;
 PROC SORT  DATA=NONHRCRECS;
 BY BANKNO RECTYPE REPORTDATE REPORTNO BRANCHNO; RUN;
 PROC PRINT DATA=NONHRCRECS(OBS=5);TITLE 'NON-HRC RECORDS ';RUN;

 /*----------------------------------------------------------------*/
 /* MERGED ALL RECORDS                                              */
 /*----------------------------------------------------------------*/
 DATA MRGRECORDS; FORMAT YYYY $4. MM $2. DD $2.;
     SET HRCRECS NONHRCRECS;
     YYYY = SUBSTR(REPORTDATE,7,4);
     MM   = SUBSTR(REPORTDATE,4,2);
     DD   = SUBSTR(REPORTDATE,1,2);
 PROC SORT  DATA=MRGRECORDS;
 BY BRANCHNO YYYY MM DD BANKNO REPORTNO RECTYPE; RUN;
 PROC PRINT DATA=MRGRECORDS(OBS=5);TITLE 'MERGED RECORDS';RUN;

 /*----------------------------------------------------------------*/
 /* OUTPUT HRC AND NON-HRC DATA                                    */
 /*----------------------------------------------------------------*/
 DATA OUTRECS;
   SET MRGRECORDS;
   FILE OUTFILE;
   PUT  @001  BRANCHNO           $7.
        @008  ', '
        @010  REPORTDATE        $10.
        @020  ', '
        @022  BANKNO             Z3.
        @025  ', '
        @027  REPORTNO          $20.
        @047  ', '
        @049  RECTYPE            $5.
        @054  ', '
        @056  ISHRC              $1.
        @057  ', '
        @059  TOTAL               8.
        @067  ', '
        @069  CNTVIEW             8.
        @077  ', '
        @079  PTAGE              8.2;
   RUN;
