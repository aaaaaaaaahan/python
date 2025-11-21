convert program to python with duckdb and pyarrow
duckdb for process input file and output parquet&txt
assumed all the input file ady convert to parquet can directly use it

//CIPHB4AF JOB MSGCLASS=X,MSGLEVEL=(1,1),REGION=6M,NOTIFY=&SYSUID       JOB67804
//*---------------------------------------------------------------------
//FILEB4AF EXEC SAS609
//IEFRDER   DD DUMMY
//SORTWK01  DD UNIT=SYSDA,SPACE=(CYL,(1000,500))
//SORTWK02  DD UNIT=SYSDA,SPACE=(CYL,(1000,500))
//SORTWK03  DD UNIT=SYSDA,SPACE=(CYL,(1000,500))
//SORTWK04  DD UNIT=SYSDA,SPACE=(CYL,(1000,500))
//CTRLDATE  DD DISP=SHR,DSN=SRSCTRL1(0)
//CIPHONET  DD DISP=SHR,DSN=UNLOAD.CIPHONET.FB
//CISFILE   DD DISP=SHR,DSN=CIS.CUST.DAILY
//DPTRBALS  DD DISP=SHR,DSN=DPTRBLGS
//OUTFILE   DD DSN=CIPHONET.ATM.CONTACT(+1),
//             DISP=(NEW,CATLG,DELETE),
//             UNIT=SYSDA,SPACE=(CYL,(100,50),RLSE),
//             DCB=(LRECL=200,BLKSIZE=0,RECFM=FB)
//SASLIST   DD SYSOUT=X
//SYSIN     DD *
OPTIONS IMSDEBUG=N YEARCUTOFF=1950 SORTDEV=3390 ERRORS=5;
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

      CURDT = PUT(SRSYY,Z4.) || PUT(SRSMM,Z2.) || PUT(SRSDD,Z2.);
      CALL SYMPUT('CURDT', PUT(CURDT,8.));
      CALL SYMPUT('DAY', PUT(SRSDD,Z2.));
      CALL SYMPUT('MTH', PUT(SRSMM,Z2.));
      CALL SYMPUT('YEAR', PUT(SRSYY,4.));
   RUN;
PROC PRINT DATA=SRSDATE(OBS=10);
           TITLE 'DATE FORMAT ** MMDDYYYY **   ';RUN;

 DATA PHONE;
    INFILE CIPHONET;
            INPUT @001  BANKNO             $3.
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
                  @087  PROMPTTIME         $8.
                  @095  UPDSOURCE          $5.
                  @100  UPDDATE            $10.
                  @100  UPDYY               4.
                  @105  UPDMM               2.
                  @108  UPDDD               2.
                  @110  UPDTIME            $8.
                  @118  UPDOPER            $8.
                  @126  TRXAPPL            $5.
                  @131  TRXACCTNO          $20.
                  @131  TRXACCTDP           11.
                  @151  PHONENEW           PD8.; /* ESMR 2011-3700*/
      IF UPDSOURCE = 'INIT' THEN DELETE;
      RECDT = PUT(UPDYY,Z4.) || PUT(UPDMM,Z2.) || PUT(UPDDD,Z2.);
      IF RECDT = &CURDT THEN OUTPUT;
 RUN;
 PROC PRINT DATA=PHONE(OBS=5);TITLE 'PHONE';RUN;
 PROC SORT  DATA=PHONE; BY CUSTNO ; RUN;

 DATA CIS;
    SET CISFILE.CUSTDLY;
        KEEP CUSTNO CUSTNAME SECPHONE ALIASKEY ALIAS ;
        RUN;
 PROC SORT  DATA=CIS NODUPKEY; BY CUSTNO ; RUN;

DATA MRG1;
   MERGE PHONE(IN=A) CIS(IN=B) ; BY CUSTNO;
   IF A THEN OUTPUT;
   RUN;
PROC PRINT DATA=MRG1(OBS=5);TITLE 'MERGE1';RUN;
PROC SORT  DATA=MRG1; BY TRXACCTDP ;RUN;

  DATA DEPOSIT;
     INFILE DPTRBALS MISSOVER;
     INPUT @24  REPTNO       PD3.
           @27  FMTCODE      PD2. @;

     IF (REPTNO = 1001 AND (FMTCODE IN (1,10,22))) THEN DO;
        INPUT @106   ACCTBRCH    PD4.
              @110   TRXACCTDP   PD6.
              @164   OPENDATE     PD6.
              @716   OPENIND     $1.;   /* VALID VALUE = SPACE,B,C,P,Z*/
               IF TRXACCTDP NE '';
               IF OPENIND EQ '';
     END;
  RUN;
 PROC SORT DATA=DEPOSIT ; BY TRXACCTDP; RUN;
 PROC PRINT DATA=DEPOSIT(OBS=5);TITLE 'DEPOSIT';RUN;

DATA MRG2;
   MERGE MRG1(IN=A) DEPOSIT(IN=B) ; BY TRXACCTDP;
   IF A THEN OUTPUT;
   RUN;
PROC SORT  DATA=MRG2; BY CUSTNO ;RUN;
PROC PRINT DATA=MRG2(OBS=5);TITLE 'MERGE2';RUN;

 /*----------------------------------------------------------------*/
 /*   OUTPUT DETAIL REPORT                                         */
 /*----------------------------------------------------------------*/
DATA TEMPOUT;
  SET MRG2;
  FILE OUTFILE;
     IF UPDSOURCE NOT IN ('ATM','EBK') THEN UPDSOURCE = 'OTC';
     PUT @001  '033'
         @004  TRXAPPL           $5.
         @009  TRXACCTDP         10.
         @029  PHONENEW          Z11.      /* CIPHONET  (AF UPDATE)*/
         @040  PHONEPREV         Z11.      /* ACCT LEVEL(B4 UPDATE)*/
         @051  SECPHONE          Z11.      /* CIS LEVEL (NO CHANGE)*/
         @062  ACCTBRCH          Z7.
         @069  CUSTNO            $20.
         @089  CUSTNAME          $40.
         @129  ALIASKEY          $03.
         @132  ALIAS             $37.
         @169  UPDDD             Z2.
         @171  '/'
         @172  UPDMM             Z2.
         @174  '/'
         @175  UPDYY             Z4.
         @179  UPDSOURCE         $05.;
  RETURN;
  RUN;
