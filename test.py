convert program to python with duckdb and pyarrow
duckdb for process input file and output parquet&txt
assumed all the input file ady convert to parquet can directly use it

//CIDJACCD JOB MSGCLASS=X,MSGLEVEL=(1,1),REGION=128M,NOTIFY=&SYSUID     J0040485
//*--------------------------------------------------------------------
//INITDASD EXEC PGM=IEFBR14
//DEL1     DD DSN=CIS_DJW_DPACCT,
//            DISP=(MOD,DELETE,DELETE),UNIT=SYSDA,SPACE=(TRK,(0))
//*--------------------------------------------------------------------
//MERGE#01 EXEC SAS609
//SORTWK01  DD UNIT=SYSDA,SPACE=(CYL,(1000,500))
//SORTWK02  DD UNIT=SYSDA,SPACE=(CYL,(1000,500))
//CTRLDATE DD DISP=SHR,DSN=SRSCTRL1(0)
//CIDETBRL DD DISP=SHR,DSN=DETICA_CUST_ACCTBRCH
//DJACCTDP DD DSN=CIS_DJW_DPACCT,
//            DISP=(NEW,CATLG,DELETE),
//            DCB=(RECFM=FB,LRECL=100,BLKSIZE=0),
//            SPACE=(CYL,(10,10),RLSE),UNIT=SYSDA
//SASLIST  DD SYSOUT=X
//SYSIN    DD *
OPTIONS NOCENTER;
 /*----------------------------------------------------------------*/
 /*    SET DATES                                                   */
 /*----------------------------------------------------------------*/
 DATA SRSDATE;
       INFILE CTRLDATE;
         INPUT @001  SRSYY    4.
               @005  SRSMM    2.
               @007  SRSDD    2.;

          TODAYSAS=MDY(SRSMM,SRSDD,SRSYY)-180;      /* 6   MONTHS*/
          CALL SYMPUT('DATE3',PUT(TODAYSAS,YYMMDDN8.));
          CALL SYMPUT('YEAR' ,PUT(SRSYY,Z4.));
          CALL SYMPUT('MONTH',PUT(SRSMM,Z2.));
          CALL SYMPUT('DAY'  ,PUT(SRSDD,Z2.));
    RUN;
 PROC PRINT;FORMAT TODAYSAS YYMMDDN8.; RUN;

DATA ACTBRCH;
  INFILE CIDETBRL;
  INPUT  @004   CUSTNO            $11.
         @020   PRIMSEC            $1.
         @030   ACCTCODE           $5.
         @035   ACCTNO             20.
         @060   OPENYY             $4.
         @064   OPENMM             $2.
         @066   OPENDD             $2. ;
         OPENDT=COMPRESS(OPENYY||OPENMM||OPENDD);
         OPENDX=COMPRESS(OPENYY||'-'||OPENMM||'-'||OPENDD);
         ACCTNOX = PUT(ACCTNO,Z11.);
         IF OPENDT > &DATE3 THEN DELETE;
         IF ACCTCODE = 'DP';
RUN;
PROC SORT  DATA=ACTBRCH NODUPKEY; BY CUSTNO;RUN;
PROC PRINT DATA=ACTBRCH(OBS=5);TITLE 'ACTBRCH';RUN;

DATA DPACCT;
  SET ACTBRCH;
  FILE DJACCTDP;
     PUT @001  CUSTNO     $11.
         @021  ACCTCODE   $5.
         @026  ACCTNOX    $20.
         @046  OPENDX     $10.;
  RUN;
