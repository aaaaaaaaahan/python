convert program to python with duckdb and pyarrow
duckdb for process input file and output parquet&txt
assumed all the input file ady convert to parquet can directly use it

//CIBTRDEM JOB MSGCLASS=X,MSGLEVEL=(1,1),REGION=64M,NOTIFY=&SYSUID      J0132950
//*--------------------------------------------------------------------
//INITDASD EXEC PGM=IEFBR14
//DEL1     DD DSN=BTRADE.EMAILADD,
//            DISP=(MOD,DELETE,DELETE),UNIT=SYSDA,SPACE=(TRK,(0))
//*--------------------------------------------------------------------
//STATS#01 EXEC SAS609
//SORTWK01 DD UNIT=SYSDA,SPACE=(CYL,(1000,500))
//SORTWK02 DD UNIT=SYSDA,SPACE=(CYL,(1000,500))
//SORTWK03 DD UNIT=SYSDA,SPACE=(CYL,(1000,500))
//CISFILE  DD DISP=SHR,DSN=CIS.CUST.DAILY
//RLEN#CA  DD DISP=SHR,DSN=UNLOAD.RLEN#CA
//EMAILADD DD DISP=SHR,DSN=CCRIS.CISRMRK.EMAIL.FIRST
//EMAILLST DD DSN=BTRADE.EMAILADD,
//            DISP=(NEW,CATLG,DELETE),
//            SPACE=(CYL,(200,200),RLSE),UNIT=SYSDA,
//            DCB=(LRECL=150,BLKSIZE=0,RECFM=FB)
//SASLIST  DD SYSOUT=X
//SYSIN    DD *
OPTION NOCENTER;
DATA CIS;
   SET CISFILE.CUSTDLY;
   KEEP CUSTNO ALIAS ALIASKEY;
   IF ACCTCODE = 'LN';
   IF PRISEC = 901;
RUN;
PROC SORT  DATA=CIS NODUPKEY; BY CUSTNO ;RUN;
PROC PRINT DATA=CIS(OBS=10);TITLE 'CIS';RUN;

 /*----------------------------------------------------------------*/
 /* DEFINE RLEN FILE TO GET ACCT NO                             */
 /*----------------------------------------------------------------*/
 DATA RLEN;
  INFILE RLEN#CA;
  INPUT  @005  ACCTNOC           $20.
         @025  ACCTCODE          $5.
         @046  CUSTNO            $20.
         @066  RLENCODE          PD2.
         @068  PRISEC            PD2.;
         RLENCD = PUT(RLENCODE,Z3.);
         IF ACCTCODE NOT = 'LN' THEN DELETE;
         IF PRISEC = '901';
  RUN;
  PROC SORT  DATA=RLEN; BY CUSTNO;RUN;
  PROC PRINT DATA=RLEN(OBS=10); TITLE 'RLEN' ; RUN;

DATA EMAIL;
   INFILE EMAILADD;
   INPUT @009     CUSTNO                      $20.
         @052     EMAILADD                    $60. ;
   EMAILADD = UPCASE(EMAILADD);
RUN;
PROC SORT  DATA=EMAIL; BY CUSTNO ;RUN;
PROC PRINT DATA=EMAIL(OBS=10);TITLE 'EMAIL';RUN;

 DATA BTLIST;
      KEEP CUSTNO ACCTNOC ACCTCODE BTRADE EMAILADD ALIASKEY ALIAS;
 MERGE RLEN(IN=A) EMAIL (IN=B) CIS(IN=C);
       BY CUSTNO;
 IF A AND B AND C;
 IF SUBSTR(ACCTNOC,1,3) = '025'  THEN BTRADE = 'Y';
 IF SUBSTR(ACCTNOC,1,4) = '0285' THEN BTRADE = 'Y';
 IF ACCTCODE =  'LN   ' AND BTRADE = 'Y' THEN OUTPUT BTLIST;
 RUN;
 PROC SORT  DATA=BTLIST NODUPKEY; BY CUSTNO ACCTNOC;RUN;
 PROC PRINT DATA=BTLIST(OBS=50);TITLE 'BTRADE LIST';RUN;

 DATA DWHACCT;
 FILE EMAILLST;
   SET BTLIST;
       PUT @001    CUSTNO                      $11.
           @013    ALIASKEY                    $5.
           @018    ALIAS                       $20.
           @039    EMAILADD                    $60.
           @099    ACCTNOC                     $20.
           ;
 RUN;
