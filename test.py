convert program to python with duckdb and pyarrow
duckdb for process input file and output parquet&txt
assumed all the input file ady convert to parquet can directly use it

//CIMSCBLK JOB MSGCLASS=X,MSGLEVEL=(1,1),REGION=128M,NOTIFY=&SYSUID     JOB25741
//*--------------------------------------------------------------------
//INITDASD EXEC PGM=IEFBR14
//DEL1     DD DSN=CIS.BLANK.MASCO,
//            DISP=(MOD,DELETE,DELETE),UNIT=SYSDA,SPACE=(TRK,(0))
//DEL2     DD DSN=CIS.BLANK.MSIC,
//            DISP=(MOD,DELETE,DELETE),UNIT=SYSDA,SPACE=(TRK,(0))
//*--------------------------------------------------------------------
//MERGE#01 EXEC SAS609
//SORTWK01  DD UNIT=SYSDA,SPACE=(CYL,(1000,500))
//SORTWK09  DD UNIT=SYSDA,SPACE=(CYL,(1000,500))
//SORTWK10  DD UNIT=SYSDA,SPACE=(CYL,(1000,500))
//CUSTFILE DD DISP=SHR,DSN=UNLOAD.ALLCUST.FB
//BLKMASCO DD DSN=CIS.BLANK.MASCO,
//            DISP=(NEW,CATLG,DELETE),UNIT=SYSDA,
//            DCB=(LRECL=200,BLKSIZE=0,RECFM=FB),
//            SPACE=(CYL,(100,100),RLSE)
//BLKMSIC  DD DSN=CIS.BLANK.MSIC,
//            DISP=(NEW,CATLG,DELETE),UNIT=SYSDA,
//            DCB=(LRECL=200,BLKSIZE=0,RECFM=FB),
//            SPACE=(CYL,(100,100),RLSE)
//SASLIST  DD SYSOUT=X
//SYSIN    DD *
OPTION NOCENTER;
DATA DATA_INDV DATA_ORG;
  INFILE CUSTFILE;
  DROP GENDER;
  INPUT  @005  CUSTNO           $11.
         @033  GENDER           $1.
         @252  MSICCODE         $5.     /*  MISC-DEMO-9  */
         @262  MASCO2008        $5.  ;  /*  MISC-DEMO-10 */

         IF GENDER = 'O' THEN INDORG = 'O';
         ELSE INDORG = 'I';
         IF INDORG = 'O' AND MASCO2008 NE '' THEN OUTPUT DATA_ORG;
         IF INDORG = 'I' AND MSICCODE  NE '' THEN OUTPUT DATA_INDV;
RUN;
PROC SORT  DATA=DATA_INDV; BY CUSTNO ;RUN;
PROC PRINT DATA=DATA_INDV(OBS=50);TITLE 'INDIVIDUAL CUST';RUN;

PROC SORT  DATA=DATA_ORG ; BY CUSTNO ;RUN;
PROC PRINT DATA=DATA_ORG (OBS=50);TITLE 'ORGANISATION CUST';RUN;

DATA OUT_INDV;
  SET DATA_INDV;
  FILE BLKMSIC ;
  MSICCODE = '' ;
  PUT    @001  CUSTNO           $11.
         @045  MSICCODE         $5.  ;
RUN;

DATA OUT_ORG;
  SET DATA_ORG;
  FILE BLKMASCO;
  MASCO2008 = '' ;
  PUT    @001  CUSTNO           $11.
         @045  MASCO2008        $5.  ;
RUN;
