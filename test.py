convert program to python with duckdb and pyarrow
duckdb for process input file
pyarrow use for output
assumed all the input file ady convert to parquet can directly use it

//CCRSRACE JOB MSGCLASS=X,MSGLEVEL=(1,1),REGION=64M,NOTIFY=&SYSUID
//*--------------------------------------------------------------------
//INITDASD EXEC PGM=IEFBR14
//DEL1     DD DSN=CIS.RACE,
//            DISP=(MOD,DELETE,DELETE),UNIT=SYSDA,SPACE=(TRK,(0))
//*--------------------------------------------------------------------
//STATS#01 EXEC SAS609
//CISFILE  DD DISP=SHR,DSN=CIS.CUST.DAILY
//OUTFILE  DD DSN=CIS.RACE,
//            DISP=(NEW,CATLG,DELETE),
//            SPACE=(CYL,(10,10),RLSE),UNIT=SYSDA,
//            DCB=(LRECL=200,BLKSIZE=0,RECFM=FB)
//SASLIST  DD SYSOUT=X
//SYSIN    DD *
DATA CIS;
   SET CISFILE.CUSTDLY;
   IF CUSTNAME = '' THEN DELETE;
   IF ALIASKEY = 'IC';
   IF INDORG = 'I' ;
   IF CITIZENSHIP NE 'MY' THEN DELETE;
   IF RACE = 'O' THEN OUTPUT;
PROC SORT  DATA=CIS NODUPKEY; BY CUSTNO ;

PROC PRINT DATA=CIS(OBS=5);TITLE 'CIS';RUN;

DATA OUT;
  SET CIS;
  FILE OUTFILE;
     PUT       ALIASKEY          $03. ';'
               ALIAS             $15. ';'
               CUSTNAME          $40. ';'
               CUSTNO            $11. ';'
               CUSTBRCH          Z3.  ';'
     ;
  RETURN;
  RUN;
