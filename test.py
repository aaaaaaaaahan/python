convert program to python with duckdb and pyarrow
duckdb for process input file
pyarrow use for output
assumed all the input file ady convert to parquet can directly use it

//CCRSPOST JOB MSGCLASS=X,MSGLEVEL=(1,1),REGION=128M,NOTIFY=&SYSUID
//*--------------------------------------------------------------------
//* EMPTY POSTCODE FILE
//*--------------------------------------------------------------------
//INITDASD EXEC PGM=IEFBR14
//DEL1     DD DSN=ECCRIS.BLANK.ADDR.POSTCODE,
//            DISP=(MOD,DELETE,DELETE),UNIT=SYSDA,SPACE=(TRK,(0))
//*--------------------------------------------------------------------
//STATS#01 EXEC SAS609
//SORTWK01 DD UNIT=SYSDA,SPACE=(CYL,(1000,500))
//SORTWK02 DD UNIT=SYSDA,SPACE=(CYL,(1000,500))
//SORTWK03 DD UNIT=SYSDA,SPACE=(CYL,(1000,500))
//SORTWK04 DD UNIT=SYSDA,SPACE=(CYL,(1000,500))
//SORTWK05 DD UNIT=SYSDA,SPACE=(CYL,(1000,500))
//SORTWK06 DD UNIT=SYSDA,SPACE=(CYL,(1000,500))
//SORTWK07 DD UNIT=SYSDA,SPACE=(CYL,(1000,500))
//SORTWK08 DD UNIT=SYSDA,SPACE=(CYL,(1000,500))
//UNQACCT  DD DISP=SHR,DSN=ACTIVE.ACCOUNT.UNIQUE
//CISFILE  DD DISP=SHR,DSN=CIS.CUST.DAILY
//ADDRFILE DD DISP=SHR,DSN=CCRIS.CISADDR.GDG(0)
//OUTFILE  DD DSN=ECCRIS.BLANK.ADDR.POSTCODE,
//            DISP=(NEW,CATLG,DELETE),
//            SPACE=(CYL,(100,100),RLSE),UNIT=SYSDA,
//            DCB=(LRECL=200,BLKSIZE=0,RECFM=FB)
//SASLIST  DD SYSOUT=X
//SYSIN    DD *

OPTIONS IMSDEBUG=N YEARCUTOFF=1950 SORTDEV=3390 ERRORS=0;
OPTIONS NODATE NONUMBER NOCENTER;
TITLE;
 DATA ACCT;
    INFILE UNQACCT;
    INPUT @1    CUSTNO        $11.
          @12   ACCTCODE      $5.
          @17   ACCTNOC       $20.
          @37   NOTENO        $5.
          @42   ALIASKEY      $3.
          @45   ALIAS         $15.
          @60   CUSTNAME      $40.
          @100  BRANCH        $5.
          @106  OPENDATE      $8.
          @115  OPENIND       $1.;
 PROC SORT  DATA=ACCT; BY CUSTNO ACCTNOC;
 PROC PRINT DATA=ACCT(OBS=5);TITLE 'UNIQUE ACCOUNT';RUN;

DATA CIS;
   SET CISFILE.CUSTDLY;
   IF ACCTCODE = ' ' THEN DELETE;
   IF CUSTNAME = '' THEN DELETE;
   IF ACCTCODE IN ('DP','LN');
   RUN;
PROC PRINT DATA=CIS(OBS=5);TITLE 'CIS';RUN;
PROC SORT  DATA=CIS NODUPKEY; BY ADDREF;

DATA ADDR OUT1;
   INFILE ADDRFILE;
   FORMAT ERRORCODE $3.;
   INPUT @001   ADDREF           11.
         @012   LINE1ADR        $40.
         @052   LINE2ADR        $40.
         @092   LINE3ADR        $40.
         @132   LINE4ADR        $40.
         @172   LINE5ADR        $40.
         @212   ADDR_WEF        $8.    /*DDMMCCYY*/
         @220   ZIPCODE         $5.
         @225   STATE           $2.
         @227   COUNTRY         $10.;

    /* CHECK FOR EMPTY ZIPCODE */
    IF ZIPCODE EQ '' THEN DO;
       FIELDTYPE = 'ZIPCODE';
       FIELDVALUE = 'INVALID POSTCODE';
       REMARKS = 'PLS CHECK POSTCODE';
       ERRORCODE = '100';
       OUTPUT OUT1;
    END;
PROC SORT  DATA=ADDR NODUPKEY; BY ADDREF;

   RUN;
PROC PRINT DATA=ADDR(OBS=5);TITLE 'ADDRESS FILE';RUN;

DATA MERGE1;
     MERGE OUT1(IN=A) CIS(IN=B); BY ADDREF;
     IF A AND B;
RUN;

PROC SORT  DATA=MERGE1; BY CUSTNO ACCTNOC;
DATA MERGE2;
     MERGE ACCT(IN=A) MERGE1(IN=B); BY CUSTNO ACCTNOC;
     IF A AND B;
RUN;

DATA OUT;
  SET MERGE2;
  FILE OUTFILE;
     IF PRISEC = 901 THEN PRIMSEC = 'P';
     IF PRISEC = 902 THEN PRIMSEC = 'S';
     PUT @01   BRANCH            $5.
         @06   ACCTCODE          $5.
         @11   ACCTNOC           $20.
         @31   PRIMSEC           $1.
         @32   CUSTNO            $11.
         @43   ERRORCODE         $03.
         @46   FIELDTYPE         $20.
         @66   FIELDVALUE        $30.
         @96   REMARKS           $40.;
  RETURN;
  RUN;
