convert program to python with duckdb and pyarrow
duckdb for process input file and output parquet&txt
assumed all the input file ady convert to parquet can directly use it

//CIPHNRES JOB MSGCLASS=X,MSGLEVEL=(1,1),REGION=64M,NOTIFY=&SYSUID      JOB60896
//*---------------------------------------------------------------------
//* TO SELECT RECORDS TO RESET PROMPT INDICATOR
//*---------------------------------------------------------------------
//RESET   EXEC SAS609
//IEFRDER   DD DUMMY
//CTRLDATE  DD DISP=SHR,DSN=SRSCTRL1(0)
//CIPHONET  DD DISP=SHR,DSN=UNLOAD.CIPHONET.FB
//OUTFILE   DD DSN=CIPHONET.RESET(+1),
//             DISP=(NEW,CATLG,DELETE),
//             UNIT=SYSDA,SPACE=(CYL,(100,50),RLSE),
//             DCB=(LRECL=158,BLKSIZE=0,RECFM=FB)
//SASLIST   DD SYSOUT=X
//SYSIN     DD *
OPTIONS IMSDEBUG=N YEARCUTOFF=1950 SORTDEV=3390 ERRORS=0;
OPTIONS NODATE NONUMBER NOCENTER;
TITLE;
 /*----------------------------------------------------------------*/
 /*    DATA DECLARATION                                            */
 /*----------------------------------------------------------------*/
    DATA SRSDATE;
       INFILE CTRLDATE;
         INPUT @001  SRSYY    4.
               @005  SRSMM    2.
               @007  SRSDD    2.;

      CURDT  = MDY(SRSMM,SRSDD,SRSYY);
      CALL SYMPUT('CURDT',PUT(CURDT,8.));
      CALL SYMPUT('CURDD',PUT(SRSDD,2.));
      CALL SYMPUT('CURMM',PUT(SRSMM,2.));
      CALL SYMPUT('CURYY',PUT(SRSYY,4.));
    RUN;
 PROC PRINT DATA=SRSDATE(OBS=10);
            TITLE 'DATE FORMAT ** MMDDYYYY **   ';RUN;

 DATA PHONE;
    INFILE CIPHONET;
       FORMAT PROMPTNO  PD1.;
       INPUT @001  BANKNO             $3.
             @004  APPLCODE           $5.
             @009  CUSTNO             $11.
             @029  PHONETYPE          $15.
             @044  PHONEPAC           PD8.
             @052  PHONEPREV          PD8.
             @060  INDORG             $1.
             @061  FIRSTDATE          $10.
             @072  PROMTSOURCE        $5.
             @077  PROMPTDATE         $10.
             @087  PROMPTTIME         $10.
             @095  UPDSOURCE          $5.
             @100  UPDTYY              4.
             @105  UPDTMM              2.
             @108  UPDTDD              2.
             @110  UPDTIME            $8.
             @118  UPDOPER            $8.
             @126  TRXAPPLCODE        $5.
             @131  TRXAPPLNO          $20.
             @151  PHONENEW           PD8.;
             RECDT  = MDY(UPDTMM,UPDTDD,UPDTYY);
             IF &CURDD = UPDTDD AND &CURMM = UPDTMM THEN DO;
                IF &CURYY > UPDTYY THEN OUTPUT;
             END;
        /*   IF RECDT - &CURDT = 365 THEN OUTPUT; */
 PROC PRINT DATA=PHONE(OBS=5);TITLE 'PHONE';RUN;
 PROC SORT  DATA=PHONE; BY CUSTNO ; RUN;
 /*----------------------------------------------------------------*/
 /*   OUTPUT DETAIL REPORT                                         */
 /*----------------------------------------------------------------*/
DATA TEMPOUT;
  SET PHONE;
  FILE OUTFILE;
     PROMPTNO = 0;
     PUT @001  BANKNO             $3.
         @004  APPLCODE           $5.
         @009  CUSTNO             $11.
         @029  PHONETYPE          $15.
         @044  PHONEPAC           PD8.
         @052  PHONEPREV          PD8.
         @060  INDORG             $1.
         @061  FIRSTDATE          $10.
         @071  PROMPTNO           PD1.
         @072  PROMTSOURCE        $5.
         @077  PROMPTDATE         $10.
         @087  PROMPTTIME         $10.
         @095  UPDSOURCE          $5.
         @100  UPDDATE            $10.
         @110  UPDTIME            $10.
         @118  UPDOPER            $8.
         @126  TRXAPPLCODE        $5.
         @131  TRXAPPLNO          $20.
         @151  PHONENEW           PD8.;
  RETURN;
  RUN;
