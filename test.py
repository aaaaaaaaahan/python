convert program to python with duckdb and pyarrow
duckdb for process input file and output parquet&txt
assumed all the input file ady convert to parquet can directly use it

//CIMULTIC JOB MSGCLASS=X,MSGLEVEL=(1,1),REGION=8M,NOTIFY=&SYSUID       JOB56145
//*--------------------------------------------------------------------
//INITDASD EXEC PGM=IEFBR14
//DLT      DD DSN=CIS.MULTIPLE.ALIAS.IC,
//            DISP=(MOD,DELETE,DELETE),UNIT=SYSDA,SPACE=(TRK,(0))
//*--------------------------------------------------------------------
//STATS#01 EXEC SAS609
//*UNLOAD ALL ALIAS CUSTOMERS FILE WITHOUT SORT FIRST FROM CIULDALS
//ALSFILE  DD DISP=SHR,DSN=UNLOAD.ALLALIAS.FB
//OUTFILE  DD DSN=CIS.MULTIPLE.ALIAS.IC,
//            DISP=(NEW,CATLG,DELETE),
//            SPACE=(CYL,(2,10),RLSE),UNIT=SYSDA,
//            DCB=(LRECL=100,BLKSIZE=0,RECFM=FB)
//SASLIST  DD SYSOUT=X
//SYSIN    DD *
OPTIONS IMSDEBUG=N YEARCUTOFF=1950 SORTDEV=3390 ERRORS=0;
OPTIONS NODATE NONUMBER NOCENTER;
TITLE;

DATA ALIASDATA;
  INFILE ALSFILE;
  INPUT  @005  CUSTNO             $11.
         @089  ALIASKEY           $3.
         @092  ALIAS              $20.;
         IF ALIASKEY NE 'IC' THEN DELETE;
PROC SORT  DATA=ALIASDATA; BY CUSTNO ALIAS ;
PROC PRINT DATA=ALIASDATA(OBS=5);TITLE 'ALIAS DATA';
RUN;

PROC SQL;
  CREATE TABLE TEMPALS AS
  SELECT CUSTNO
  FROM ALIASDATA
  GROUP BY CUSTNO
  HAVING COUNT(CUSTNO) > 1;
QUIT;
RUN;


DATA OUT;
  SET TEMPALS;
  FILE OUTFILE;
     PUT @01   '033'
         @05   CUSTNO            $11.;
       /*@17   'IC '                  */
       /*@20   ALIAS             $20.;*/
