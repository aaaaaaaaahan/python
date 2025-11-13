convert program to python with duckdb and pyarrow
duckdb for process input file and output parquet&txt
assumed all the input file ady convert to parquet can directly use it

//CIINCHKA JOB MSGCLASS=X,MSGLEVEL=(1,1),REGION=64M,NOTIFY=&SYSUID      JOB30877
//*---------------------------------------------------------------------
//*- BACKUP FAILED RECORDS FOR CHECK AND LOAD PURPOSES
//*---------------------------------------------------------------------
//COPYFIL1 EXEC PGM=ICEGENER
//SYSPRINT DD SYSOUT=X
//SYSUT1   DD DISP=SHR,DSN=CIS.INNAMEKY.FAIL
//SYSUT2   DD DSN=CIS.INNAMEKY.FBKP(+1),
//            DISP=(NEW,CATLG,DELETE),UNIT=SYSDA,
//            DCB=(LRECL=353,BLKSIZE=0,RECFM=FB),
//            SPACE=(CYL,(100,50),RLSE)
//SYSIN    DD DUMMY
//*---------------------------------------------------------------------
//*- PROCESS EMPLOYER INFORMATION (DELTA FILE)
//*---------------------------------------------------------------------
//SOCSOFUL EXEC SAS609
//CIINCKEY DD DISP=SHR,DSN=CIS.INNAMEKY.FAIL
//SASLIST  DD SYSOUT=X
//SYSIN    DD *
OPTION NOCENTER;
 DATA CIINCKEY;
    RETAIN X;
    INFILE CIINCKEY END=LAST EOF=EOFRTN;
      INPUT @01 ALLRECS       $352.;

    EOFRTN:
      IF _N_ NE 1  THEN ABORT 77;

 RUN;
 PROC PRINT DATA=CIINCKEY(OBS=100);TITLE 'FAILED REC';RUN;
