convert program to python with duckdb and pyarrow
duckdb for process input file and output parquet&csv
assumed all the input file ady convert to parquet can directly use it

//CIHRCALL JOB MSGCLASS=X,MSGLEVEL=(1,1),REGION=8M,NOTIFY=&SYSUID       JOB65417
//*---------------------------------------------------------------------
//DELETE   EXEC PGM=IEFBR14
//DELE0    DD DSN=CIS.HRCCUST.DPACCTS,
//            DISP=(MOD,DELETE,DELETE),SPACE=(TRK,(0))
//DELE1    DD DSN=CIS.HRCCUST.LNACCTS,
//            DISP=(MOD,DELETE,DELETE),SPACE=(TRK,(0))
//DELE2    DD DSN=CIS.HRCCUST.OTACCTS,
//            DISP=(MOD,DELETE,DELETE),SPACE=(TRK,(0))
//*---------------------------------------------------------------------
//* PROCESSES STARTS HERE
//*---------------------------------------------------------------------
//ALLACCT  EXEC SAS609,REGION=4M,WORK='50000,50000'
//IEFRDER   DD DUMMY
//*OUTPUT FROM JOB CICUSCD1
//HRCFILE   DD DISP=SHR,DSN=CUSTCODE
//*OUTPUT FROM JOB CCRNMALS
//CISFILE   DD DISP=SHR,DSN=CIS.CUST.DAILY
//OUTDPACC  DD DSN=CIS.HRCCUST.DPACCTS,
//             DISP=(NEW,CATLG,DELETE),
//             UNIT=SYSDA,SPACE=(CYL,(10,10),RLSE),
//             DCB=(LRECL=250,BLKSIZE=0,RECFM=FB)
//OUTLNACC  DD DSN=CIS.HRCCUST.LNACCTS,
//             DISP=(NEW,CATLG,DELETE),
//             UNIT=SYSDA,SPACE=(CYL,(10,10),RLSE),
//             DCB=(LRECL=250,BLKSIZE=0,RECFM=FB)
//OUTOTACC  DD DSN=CIS.HRCCUST.OTACCTS,
//             DISP=(NEW,CATLG,DELETE),
//             UNIT=SYSDA,SPACE=(CYL,(10,10),RLSE),
//             DCB=(LRECL=250,BLKSIZE=0,RECFM=FB)
//SASLIST   DD SYSOUT=X
//SORTWK01  DD UNIT=SYSDA,SPACE=(CYL,800)
//SORTWK02  DD UNIT=SYSDA,SPACE=(CYL,800)
//SORTWK03  DD UNIT=SYSDA,SPACE=(CYL,800)
//SORTWK04  DD UNIT=SYSDA,SPACE=(CYL,800)
//SORTWK05  DD UNIT=SYSDA,SPACE=(CYL,800)
//SORTWK06  DD UNIT=SYSDA,SPACE=(CYL,800)
//SORTWK07  DD UNIT=SYSDA,SPACE=(CYL,800)
//SORTWK08  DD UNIT=SYSDA,SPACE=(CYL,800)
//SYSIN     DD *
OPTIONS IMSDEBUG=N YEARCUTOFF=1950 SORTDEV=3390 ERRORS=0;
OPTIONS NODATE NONUMBER NOCENTER;
TITLE;
 /*----------------------------------------------------------------*/
 /*    HIGH RISK CUSTOMERS FILE DECLARATION                        */
 /*----------------------------------------------------------------*/
  DATA HRCCUST;
      INFILE HRCFILE;
      INPUT @01   CUSTNO             $20.
            @29   HRCCODES           $60.;
  RUN;
  PROC SORT DATA=HRCCUST; BY CUSTNO; RUN;

 /*----------------------------------------------------------------*/
 /*    CIS CUSTOMER FILE DECLARATION                               */
 /*----------------------------------------------------------------*/
  DATA CIS;
      KEEP CUSTBRCH CUSTNO RACE CITIZENSHIP INDORG PRISEC
           ALIASKEY ALIAS ACCTCODE ACCTNO CUSTNAME
           CUSTLASTDATECC CUSTLASTDATEYY CUSTLASTDATEMM CUSTLASTDATEDD;
      SET CISFILE.CUSTDLY;
  RUN;
  PROC SORT DATA=CIS; BY CUSTNO ACCTNO; RUN;

 /*----------------------------------------------------------------*/
 /*    GET ALL HIGH RISK CUSTOMERS THEN ONLY RELATED ACCTS         */
 /*    SPLIT INTO THREE MAIN CATEGORIES DP, LN ,AND OTHER ACCT TYPE*/
 /*    INCLUDES JOINT ACCOUNT CUSTOMERS                            */
 /*----------------------------------------------------------------*/
  DATA MRGCISDP MRGCISLN MRGCISOT;
      MERGE HRCCUST(IN=A) CIS(IN=B); BY CUSTNO;
      IF A AND B THEN DO;
         IF ACCTCODE = 'DP' THEN OUTPUT MRGCISDP;
         IF ACCTCODE = 'LN' THEN OUTPUT MRGCISLN;
         IF ACCTCODE NE 'DP' AND ACCTCODE NE 'LN' THEN OUTPUT MRGCISOT;
      END;
  RUN;
  PROC SORT DATA=MRGCISDP; BY ACCTNO; RUN;
  PROC SORT DATA=MRGCISLN; BY ACCTNO; RUN;
  PROC SORT DATA=MRGCISOT; BY ACCTNO; RUN;
  PROC PRINT DATA=MRGCISDP(OBS=10);TITLE 'HRC WITH DP ACCTS ONLY';RUN;
  PROC PRINT DATA=MRGCISLN(OBS=10);TITLE 'HRC WITH LN ACCTS ONLY';RUN;
  PROC PRINT DATA=MRGCISOT(OBS=10);TITLE 'HRC WITH OT ACCTS ONLY';RUN;

 /*----------------------------------------------------------------*/
 /*   OUTPUT DEPOSIT ACCOUNTS RELATED CUSTOMERS DATASETS           */
 /*----------------------------------------------------------------*/
  DATA TEMPDP;
  SET MRGCISDP;
  FILE OUTDPACC;
     IF PRISEC = 901 THEN PRIMSEC = 'P';
     IF PRISEC = 902 THEN PRIMSEC = 'S';
     PUT @01   '033'
         @04   CUSTBRCH           Z5.
         @09   CUSTNO             $11.
         @20   CUSTNAME           $40.
         @60   RACE               $1.
         @61   CITIZENSHIP        $2.
         @63   INDORG             $1.
         @64   PRIMSEC            $1.
         @65   CUSTLASTDATECC     $2.
         @67   CUSTLASTDATEYY     $2.
         @69   CUSTLASTDATEMM     $2.
         @71   CUSTLASTDATEDD     $2.
         @73   ALIASKEY           $3.
         @76   ALIAS              $20.
         @96   HRCCODES           $60.
         @156  ACCTCODE           $5.
         @161  ACCTNO             20.;
  RETURN;
  RUN;
 /*----------------------------------------------------------------*/
 /*   OUTPUT LOANS ACCOUNTS RELATED CUSTOMERS DATASETS             */
 /*----------------------------------------------------------------*/
  DATA TEMPLN;
  SET MRGCISLN;
  FILE OUTLNACC;
     IF PRISEC = 901 THEN PRIMSEC = 'P';
     IF PRISEC = 902 THEN PRIMSEC = 'S';
     PUT @01   '033'
         @04   CUSTBRCH           Z5.
         @09   CUSTNO             $11.
         @20   CUSTNAME           $40.
         @60   RACE               $1.
         @61   CITIZENSHIP        $2.
         @63   INDORG             $1.
         @64   PRIMSEC            $1.
         @65   CUSTLASTDATECC     $2.
         @67   CUSTLASTDATEYY     $2.
         @69   CUSTLASTDATEMM     $2.
         @71   CUSTLASTDATEDD     $2.
         @73   ALIASKEY           $3.
         @76   ALIAS              $20.
         @96   HRCCODES           $60.
         @156  ACCTCODE           $5.
         @161  ACCTNO             20.;
  RETURN;
  RUN;
 /*----------------------------------------------------------------*/
 /*   OUTPUT OTHERS ACCOUNTS RELATED CUSTOMERS DATASETS            */
 /*----------------------------------------------------------------*/
  DATA TEMPOT;
  SET MRGCISOT;
  FILE OUTOTACC;
     IF PRISEC = 901 THEN PRIMSEC = 'P';
     IF PRISEC = 902 THEN PRIMSEC = 'S';
     PUT @01   '033'
         @04   CUSTBRCH           Z5.
         @09   CUSTNO             $11.
         @20   CUSTNAME           $40.
         @60   RACE               $1.
         @61   CITIZENSHIP        $2.
         @63   INDORG             $1.
         @64   PRIMSEC            $1.
         @65   CUSTLASTDATECC     $2.
         @67   CUSTLASTDATEYY     $2.
         @69   CUSTLASTDATEMM     $2.
         @71   CUSTLASTDATEDD     $2.
         @73   ALIASKEY           $3.
         @76   ALIAS              $20.
         @96   HRCCODES           $60.
         @156  ACCTCODE           $5.
         @161  ACCTNO             20.;
  RETURN;
  RUN;
