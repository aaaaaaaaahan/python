convert program to python with duckdb and pyarrow
duckdb for process input file and output parquet&csv
assumed all the input file ady convert to parquet can directly use it

//CIGTOREX JOB MSGCLASS=X,MSGLEVEL=(1,1),REGION=64M,NOTIFY=&SYSUID
//*--------------------------------------------------------------------
//INITDASD EXEC PGM=IEFBR14
//DEL1     DD DSN=LIABFILE.GUARANTR.CIS,
//            DISP=(MOD,DELETE,DELETE),UNIT=SYSDA,SPACE=(TRK,(0))
//*--------------------------------------------------------------------
//STATS#01 EXEC SAS609
//LIABFILE DD DISP=SHR,DSN=LIABFILE
//CISFILE  DD DISP=SHR,DSN=CIS.CUST.DAILY
//OUTFILE  DD DSN=LIABFILE.GUARANTR.CIS,
//            DISP=(NEW,CATLG,DELETE),
//            SPACE=(CYL,(50,50),RLSE),UNIT=SYSDA,
//            DCB=(LRECL=200,BLKSIZE=0,RECFM=FB)
//SASLIST  DD SYSOUT=X
//SYSIN    DD *

OPTIONS IMSDEBUG=N YEARCUTOFF=1950 SORTDEV=3390 ERRORS=0;
OPTIONS NODATE NONUMBER NOCENTER;
TITLE;
DATA LIAB;
   INFILE LIABFILE;
       INPUT @1    HIRERACCTNO  PD6.
             @7    HIRERNOTE    PD3.
             @10   GTORACCTNO   PD6.;
RUN;
PROC SORT  DATA=LIAB ; BY GTORACCTNO ;RUN;
PROC PRINT DATA=LIAB(OBS=50);TITLE 'LIABILITY FILE';RUN;

DATA CIS;
   SET CISFILE.CUSTDLY;
   KEEP CUSTNO CUSTNAME ALIASKEY ALIAS PRISEC
        GTORACCTNO RLENCODE INDORG;
   GTORACCTNO=ACCTNO;

   IF ACCTCODE = 'LN';
RUN;
PROC SORT  DATA=CIS; BY GTORACCTNO ;RUN;
PROC PRINT DATA=CIS(OBS=50);TITLE 'CIS';RUN;

DATA GTOR;
     MERGE LIAB(IN=P) CIS(IN=Q); BY GTORACCTNO ;
     IF P;
RUN;

DATA OUT;
  SET GTOR;
  FILE OUTFILE;
     PUT @1    HIRERACCTNO       Z11.
         @12   HIRERNOTE         Z5.
         @17   GTORACCTNO        Z11.
         @28   CUSTNO            $11.      /*CIS CUST NUMBER     */
         @39   ALIASKEY          $03.     /* IC/BC/ML/PL/PP..ETC */
         @42   ALIAS             $15. ;   /* CUST IDENTIFICATION */
  RETURN;
  RUN;
