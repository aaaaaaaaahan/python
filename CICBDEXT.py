//CICBDEXT JOB MSGCLASS=X,MSGLEVEL=(1,1),REGION=64M,NOTIFY=&SYSUID      J0117418
//*---------------------------------------------------------------------
//* ESMR2020-1597 - CIS EXTRACT MSIC AND CUSTOMER CODE TO DP/LN/BTRADE
//* ESMR2020-3801 - ADD HEADER AND TRAILER
//* ESMR2021-1125 - CREATE NEW OUTPUT RBP2.B033.BPM.DPFILE
//* ESMR2020-3801 - UPDATE THE CUSTOMER CODE AND MSIC BUSINESS TYPE FROM
//*                 CIS TO DEPOSIT/LOANS/TRADE FINANCE SYSTEM
//*                 (CROSS REF ESMR 2020-1597)
//* ESMR2021-3470 - SEGREGATE TRADE FINANCE RECORDS FROM LOANS RECORDS
//*---------------------------------------------------------------------
//DELETE   EXEC PGM=IEFBR14
//DEL1     DD DSN=RBP2.B033.CDB.DPFILE,
//            DISP=(MOD,DELETE,DELETE),SPACE=(TRK,(0))
//DEL2     DD DSN=RBP2.B033.CDB.LNFILE,
//            DISP=(MOD,DELETE,DELETE),SPACE=(TRK,(0))
//DEL3     DD DSN=RBP2.B033.CDB.BTFILE,
//            DISP=(MOD,DELETE,DELETE),SPACE=(TRK,(0))
//DEL4     DD DSN=RBP2.B033.BPM.DPFILE,
//            DISP=(MOD,DELETE,DELETE),SPACE=(TRK,(0))
//*---------------------------------------------------------------------
//* NODUPS (GET ALL RECORD WITH CHANGES/NEW COMPARE ALL FIELDS)
//* USING IDIC DAILY CHANGES TO GET THE MSIC AND CUSTOMER CODE
//*
//*---------------------------------------------------------------------
//GETCHG   EXEC SAS609
//SORTWK01 DD UNIT=SYSDA,SPACE=(CYL,(1000,500))
//SORTWK02 DD UNIT=SYSDA,SPACE=(CYL,(1000,500))
//SORTWK03 DD UNIT=SYSDA,SPACE=(CYL,(1000,500))
//NEWCHG   DD DISP=SHR,DSN=RBP2.B033.CIS.IDIC.DAILY.INEW
//OLDCHG   DD DISP=SHR,DSN=RBP2.B033.CIS.IDIC.DAILY.IOLD
//NOCHG    DD DISP=SHR,DSN=RBP2.B033.CIS.IDIC.DAILY.NOCHG
//RLEN#CA  DD DISP=SHR,DSN=RBP2.B033.UNLOAD.RLEN#CA
//CTRLDATE DD DISP=SHR,DSN=RBP2.B033.SRSCTRL1(0)
//OUTFILE  DD DSN=RBP2.B033.CDB.DPFILE,
//            DISP=(NEW,CATLG,DELETE),
//            SPACE=(CYL,(50,50),RLSE),UNIT=SYSDA,
//            DCB=(LRECL=100,BLKSIZE=0,RECFM=FB)
//OUTFIL1  DD DSN=RBP2.B033.CDB.LNFILE,
//            DISP=(NEW,CATLG,DELETE),
//            SPACE=(CYL,(50,50),RLSE),UNIT=SYSDA,
//            DCB=(LRECL=100,BLKSIZE=0,RECFM=FB)
//OUTFIL2  DD DSN=RBP2.B033.CDB.BTFILE,
//            DISP=(NEW,CATLG,DELETE),
//            SPACE=(CYL,(50,50),RLSE),UNIT=SYSDA,
//            DCB=(LRECL=100,BLKSIZE=0,RECFM=FB)
//OUTBPM1  DD DSN=RBP2.B033.BPM.DPFILE,
//            DISP=(NEW,CATLG,DELETE),
//            SPACE=(CYL,(50,50),RLSE),UNIT=SYSDA,
//            DCB=(LRECL=100,BLKSIZE=0,RECFM=FB)
//SASLIST  DD SYSOUT=X
//SYSIN    DD *
OPTION NOCENTER;
 /*----------------------------------------------------------------*/
 /*    SET DATES                                                   */
 /*----------------------------------------------------------------*/
    DATA SRSDATE;
          INFILE CTRLDATE;
            INPUT @001  SRSYY    4.
                  @005  SRSMM    2.
                  @007  SRSDD    2.;

          /* DISPLAY TODAY REPORTING DATE*/
          TODAYSAS=MDY(SRSMM,SRSDD,SRSYY);
          CALL SYMPUT('RDATE',PUT(TODAYSAS,YYMMDDN8.));

    RUN;
   PROC PRINT;RUN;
  /*TO CHANGE STATUS*/
DATA NEWCHG;
  INFILE NEWCHG;
   INPUT @21   CUSTNO                 $11.
         @188  CUSTMNTDATE            $08.
         @196  CUSTLASTOPER           $8.
         @316  CUST_CODE              $3.
         @327  MSICCODE               $5.;
PROC SORT  DATA=NEWCHG; BY CUSTNO;
PROC PRINT DATA=NEWCHG(OBS=5);TITLE 'NEW CHG';
RUN;

DATA OLDCHG;
  INFILE OLDCHG;
   INPUT @21   CUSTNO                 $11.
         @188  CUSTMNTDATEX           $08.
         @196  CUSTLASTOPERX          $8.
         @316  CUST_CODEX             $3.
         @327  MSICCODEX              $5.;
PROC SORT  DATA=OLDCHG; BY CUSTNO;
PROC PRINT DATA=OLDCHG(OBS=5);TITLE 'OLD CHG';
RUN;

  /*203801*/
DATA NEWCUST;
  INFILE NOCHG;
   FORMAT UPDMSIC $1. UPDCCDE $1.;
   INPUT @01   RUNTIMESTAMP           $20.
         @21   CUSTNO                 $11.
         @188  CUSTMNTDATEX           $08.
         @196  CUSTLASTOPERX          $8.
         @316  CUST_CODE              $3.
         @327  MSICCODE               $5.;
         DATESTAMP="&YEAR"||"&MONTH"||"&DAY";
         DATEREC = SUBSTR(RUNTIMESTAMP,1,8);
         IF DATESTAMP NE DATEREC THEN DELETE;
         UPDMSIC = 'N';
         UPDCCDE = 'N';
         IF CUST_CODE NOT = ' ' THEN UPDMSIC = 'Y';
         IF MSICCODE  NOT = ' ' THEN UPDCCDE = 'Y';
PROC SORT  DATA=NEWCUST; BY CUSTNO;
PROC PRINT DATA=NEWCUST(OBS=5);TITLE 'NEW CUST';
RUN;

DATA RLEN;
  INFILE RLEN#CA;
  INPUT  @005  ACCTNOC           $20.
         @025  ACCTCODE          $5.
         @046  CUSTNO            $11.
         @066  RLENCODE          PD2.
         @068  PRISEC            PD2.;
         RLENCD = PUT(RLENCODE,Z3.);
         IF ACCTCODE NOT IN ('DP   ','LN   ') THEN DELETE;
 RUN;
PROC SORT  DATA=RLEN; BY CUSTNO;RUN;
PROC PRINT DATA=RLEN(OBS=5);TITLE 'RLEN';

   DATA MERGE_A;
        MERGE NEWCHG(IN=A) OLDCHG(IN=B);
        BY CUSTNO;
        IF A AND B;
   RUN;
   PROC SORT  DATA=MERGE_A;BY CUSTNO;RUN;
   PROC PRINT DATA=MERGE_A(OBS=15);TITLE 'COM CIS';RUN;

   DATA DTCHG;
     FORMAT UPDMSIC $1. UPDCCDE $2.;
     SET MERGE_A;
     UPDMSIC = 'N';
     UPDCCDE = 'N';
     IF MSICCODE NOT = MSICCODEX THEN DO;
        UPDMSIC = 'Y';
     END;
     IF CUST_CODE NOT = CUST_CODEX THEN DO;
        UPDCCDE = 'Y';
     END;
     IF UPDMSIC = 'Y' OR UPDCCDE = 'Y' THEN OUTPUT;
   RUN;
   PROC PRINT DATA=DTCHG(OBS=10);TITLE 'DATA CHANGE';

    /*203801*/
   DATA MIXALL;
        SET DTCHG NEWCUST;
   RUN;
   PROC SORT  DATA=MIXALL;BY CUSTNO;RUN;
   PROC PRINT DATA=MIXALL(OBS=10);TITLE 'MIX ALL';

    /*203801 - START COMMENT OFF
    DATA DPLIST LNLIST BTLIST DPALL;
        KEEP CUSTNO ACCTNOC CUST_CODE MSICCODE
             ACCTCODE UPDMSIC UPDCCDE RLENCD BTRADE;
        MERGE DTCHG(IN=F) RLEN(IN=G);
        BY CUSTNO;
        IF F AND G;
        IF ACCTCODE =  'DP   ' THEN OUTPUT DPALL;

        IF RLENCD = '020';
        IF SUBSTR(ACCTNOC,1,3) = '025' THEN BTRADE = 'Y';
        IF SUBSTR(ACCTNOC,1,4) = '0285' THEN BTRADE = 'Y';
        IF ACCTCODE =  'LN   ' THEN OUTPUT LNLIST;
        IF ACCTCODE =  'DP   ' THEN OUTPUT DPLIST;
        IF  BTRADE = 'Y' THEN OUTPUT BTLIST;
   RUN;
   PROC SORT  DATA=DPLIST;BY CUSTNO;RUN;
   PROC PRINT DATA=DPLIST(OBS=10);TITLE 'DPLIST';RUN;
   PROC SORT  DATA=LNLIST;BY CUSTNO;RUN;
   PROC PRINT DATA=LNLIST(OBS=10);TITLE 'LNLIST';RUN;
   PROC SORT  DATA=BTLIST;BY CUSTNO;RUN;
   PROC PRINT DATA=BTLIST(OBS=10);TITLE 'BTLIST';RUN;
   PROC SORT  DATA=DPALL;BY CUSTNO;RUN;

    203801 - END  */

    /* 203801 - START */
   DATA DPLIST BTLIST LNALL DPALL;
        KEEP CUSTNO ACCTNOC CUST_CODE MSICCODE
             ACCTCODE UPDMSIC UPDCCDE RLENCD BTRADE;
        MERGE MIXALL(IN=F) RLEN(IN=G);
        BY CUSTNO;
        IF F AND G THEN DO;
           IF ACCTCODE =  'DP   ' THEN OUTPUT DPALL;
           IF SUBSTR(ACCTNOC,1,3) = '025'  THEN BTRADE = 'Y';
           IF SUBSTR(ACCTNOC,1,4) = '0285' THEN BTRADE = 'Y';
           IF ACCTCODE =  'LN   ' AND BTRADE = 'Y' THEN OUTPUT BTLIST;
           IF ACCTCODE =  'LN   ' AND BTRADE = ' ' THEN OUTPUT LNALL;
           IF RLENCD = '020' THEN DO;
              IF ACCTCODE =  'DP   ' THEN OUTPUT DPLIST;
           END;
        END;
   RUN;
   PROC SORT  DATA=DPLIST;BY CUSTNO;RUN;
   PROC PRINT DATA=DPLIST(OBS=10);TITLE 'DPLIST';RUN;
   PROC SORT  DATA=BTLIST;BY CUSTNO;RUN;
   PROC PRINT DATA=BTLIST(OBS=10);TITLE 'BTLIST';RUN;
   PROC SORT  DATA=DPALL;BY CUSTNO;RUN;
   PROC SORT  DATA=LNALL;BY CUSTNO;RUN;

    /* 203801 - END   */

   DATA RECORDS;
     FORMAT TTL Z8.;
     SET DPLIST END=EOF;
     RETAIN X;
     FILE OUTFILE;
     IF _N_ = 1 THEN DO;
          PUT @001 'FH '  "&RDATE" ;
     END;
     PUT  @001   CUSTNO          $11.
          @013   ACCTNOC         $20.
          @034   UPDCCDE         $01.
          @036   CUST_CODE       $03.
          @040   UPDMSIC         $01.
          @042   MSICCODE        $05.
      /*  @046   RLENCD          $03. */
          ;
     X+1;
     IF EOF THEN DO;
     TTL = X;
          PUT @001 'T'
              @002  TTL;
     END;
     RUN;

   /*DATA RECORD1;     203801 */
   DATA BPMREC2;     /*203801 */
     FORMAT TTL1 Z8.;
   /*SET LNLIST END=EOF;     203801 */
     SET LNALL END=EOF;    /*203801 */
     RETAIN Y;
     FILE OUTFIL1;
     IF _N_ = 1 THEN DO;
          PUT @001 'FH '  "&RDATE" ;
     END;
     PUT  @001   CUSTNO          $11.
          @013   ACCTNOC         $20.
          @034   UPDCCDE         $01.
          @036   CUST_CODE       $03.
          @040   UPDMSIC         $01.
          @042   MSICCODE        $05.
      /*  @046   RLENCD          $03. */
          ;
     Y+1;
     IF EOF THEN DO;
        TTL1 = Y;
          PUT @001 'T'
              @002 TTL1;
     END;
     RUN;

   DATA RECORD2;
     SET BTLIST END=EOF;
     FILE OUTFIL2;
     IF _N_ = 1 THEN DO;
          PUT @001 'FH '  "&RDATE" ;
     END;
     PUT  @001   CUSTNO          $11.  @12 ';'
          @013   ACCTNOC         $20.  @33 ';'
          @034   UPDCCDE         $01.  @35 ';'
          @036   CUST_CODE       $03.  @39 ';'
          @040   UPDMSIC         $01.  @41 ';'
          @042   MSICCODE        $05.  @47 ';'
      /*  @046   RLENCD          $03. */
          ;
     IF EOF THEN DO;
          PUT @001 'FH' ;
     END;
     RUN;

   DATA BPMREC1;
     FORMAT TTL Z8.;
     SET DPALL END=EOF;
     RETAIN X;
     FILE OUTBPM1;
     IF _N_ = 1 THEN DO;
          PUT @001 'FH '  "&RDATE" ;
     END;
     PUT  @001   CUSTNO          $11.
          @013   ACCTNOC         $20.
          @034   UPDCCDE         $01.
          @036   CUST_CODE       $03.
          @040   UPDMSIC         $01.
          @042   MSICCODE        $05.
          @048   RLENCD          $03.      /*    ADD*/
      /*  @046   RLENCD          $03. */
          ;
     X+1;
     IF EOF THEN DO;
     TTL = X;
          PUT @001 'T'
              @002  TTL;
     END;
     RUN;
