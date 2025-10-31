convert program to python with duckdb and pyarrow
duckdb for process input file and output parquet&csv
assumed all the input file ady convert to parquet can directly use it

//CIEXTREV JOB MSGCLASS=X,MSGLEVEL=(1,1),REGION=64M,NOTIFY=&SYSUID      JOB44233
//*--------------------------------------------------------------------
//INITDS   EXEC PGM=IEFBR14
//DEL1     DD DSN=CISREPT.UPDATE.REVIEW,
//            DISP=(MOD,DELETE,DELETE),SPACE=(TRK,(0))
//*--------------------------------------------------------------------
//*--- REPORT PROCESSING START HERE
//*--------------------------------------------------------------------
//SUM#REPT EXEC SAS609,OPTIONS='VERBOSE SORTLIST SORTMSG MSGLEVEL=I'
//* UNLOAD JOB FROM CIULREPT
//REPTFILE DD DISP=SHR,DSN=UNLOAD.CIREPTTT.FB
//CTRLDATE DD DISP=SHR,DSN=SRSCTRL1(0)
//OUTFILE  DD DSN=CISREPT.UPDATE.REVIEW,
//            DISP=(NEW,CATLG,DELETE),SPACE=(CYL,(50,10),RLSE),
//            UNIT=SYSDA,DCB=(LRECL=150,BLKSIZE=0,RECFM=FB)
//SASLIST  DD SYSOUT=X
//SORTWK01 DD UNIT=SYSDA,SPACE=(CYL,(500,200))
//SORTWK02 DD UNIT=SYSDA,SPACE=(CYL,(500,200))
//SORTWK03 DD UNIT=SYSDA,SPACE=(CYL,(500,200))
//SORTWK04 DD UNIT=SYSDA,SPACE=(CYL,(500,200))
//SORTWK05 DD UNIT=SYSDA,SPACE=(CYL,(500,200))
//SORTWK06 DD UNIT=SYSDA,SPACE=(CYL,(500,200))
//SORTWK07 DD UNIT=SYSDA,SPACE=(CYL,(500,200))
//SORTWK08 DD UNIT=SYSDA,SPACE=(CYL,(500,200))
//SORTWK09 DD UNIT=SYSDA,SPACE=(CYL,(500,200))
//SORTWK10 DD UNIT=SYSDA,SPACE=(CYL,(500,200))
//SYSIN    DD *
OPTIONS NOSORTBLKMODE;
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
     CALL SYMPUT('TODAYDATE',TODAYSAS);
 RUN;
 PROC PRINT DATA=SRSDATE(OBS=5);TITLE 'DATE FILE';RUN;
 /*----------------------------------------------------------------*/
 /*    GET REPORT DATA CONFIRMED ACCTS THAT HAS BEEN REVIEWED      */
 /*----------------------------------------------------------------*/
 DATA REPTDATA REVDATA;
  INFILE REPTFILE;
  INPUT  @001  BANKNO            PD2.
         @003  RECTYPE            $5.
         @008  APPLCODE           $5.
         @013  APPLNO            $20.
         @033  NOTENO            $10.
         @043  REPORTDATE        $10.
         @043  RPDATEDD           $2.
         @046  RPDATEMM           $2.
         @049  RPDATEYYYY         $4.
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
         @692  INDORG            $1.
         @693  OCCUPDESC         $40.
         @733  NATUREDESC        $40.
         @773  VIEWOFF            $1.
         @774  REVIEW             $1.;

         IF REVIEW EQ ' ' THEN OUTPUT REPTDATA;
         REPTSAS=MDY(RPDATEMM,RPDATEDD,RPDATEYYYY);
         IF BRCHCHECK NE ' ' AND (&TODAYDATE - REPTSAS ) LT 365
         THEN OUTPUT REVDATA;

 RUN;
 PROC SORT DATA=REPTDATA; BY APPLCODE APPLNO NOTENO;RUN;
 PROC SORT DATA=REVDATA;
      BY APPLCODE APPLNO NOTENO RPDATEYYYY RPDATEMM RPDATEDD; RUN;
 PROC SORT DATA=REVDATA NODUPKEY DUPOUT=DUPREV;
      BY APPLCODE APPLNO NOTENO;RUN;
 /*----------------------------------------------------------------*/
 /* THIS STEP TO KEEP ONLY REQUIRED DATA                           */
 /*----------------------------------------------------------------*/
 DATA ALLREV;
    KEEP REPORTDATE APPLCODE APPLNO NOTENO;
    SET REVDATA;
 RUN;
 PROC SORT DATA=ALLREV; BY APPLCODE APPLNO NOTENO; RUN;
 /*----------------------------------------------------------------*/
 /* MERGED REPORT DATA TO GET RECORDS TO BE UPDATED AS REVIEWED    */
 /* GET REPORT DATA ONLY IF THE REPORT DATE IN ALL REPORTS DATE    */
 /* IS LATER THAN REPORT DATE IN REVIEWED RECORDS                  */
 /*----------------------------------------------------------------*/
 DATA UPDREPT;
    MERGE REPTDATA(IN=A) ALLREV(RENAME=(REPORTDATE=RPDATE)IN=B);
          BY APPLCODE APPLNO NOTENO;
    IF (A AND B) THEN DO;
        IF (SUBSTR(REPORTDATE,7,4)
          ||SUBSTR(REPORTDATE,4,2)
          ||SUBSTR(REPORTDATE,1,2)) GT
           (SUBSTR(RPDATE,7,4)
          ||SUBSTR(RPDATE,4,2)
          ||SUBSTR(RPDATE,1,2))
        THEN OUTPUT;
    END;
RUN;
 /*----------------------------------------------------------------*/
 /* OUTPUT DATA                                                    */
 /*----------------------------------------------------------------*/
 DATA TEMPOUT;
    SET UPDREPT;
    FILE OUTFILE;
      PUT @001  BANKNO       Z3.
          @004  RECTYPE      $5.
          @009  APPLCODE     $5.
          @014  APPLNO       $20.
          @034  NOTENO       $10.
          @044  REPORTDATE   $10.
          @054  REPORTNO     $20.
          @074  BRANCHNO     $7.
          @081  NAME         $60.
          @141  '  '
          @143  'Y' ;
  RETURN;
  RUN;
