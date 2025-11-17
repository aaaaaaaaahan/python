convert program to python with duckdb and pyarrow
duckdb for process input file and output parquet&txt
assumed all the input file ady convert to parquet can directly use it

//*-------------------------------------------------------------------**
//*- GET LISTING OF ACCOUNT PER STAFF                                 **
//*-------------------------------------------------------------------**
//SASPGRM  EXEC SAS609
//CUSTFILE DD DISP=SHR,DSN=CIS.CUST.DAILY
//STAFFACC DD DISP=SHR,DSN=CICUSCD5.UPDATE.DP.TEMP
//DPFILE   DD DSN=CICUSCD5.UPDATE.DP,
//            DISP=(NEW,CATLG,DELETE),
//            SPACE=(CYL,(100,100),RLSE),UNIT=SYSDA,
//            DCB=(LRECL=250,BLKSIZE=0,RECFM=FB)
//SASLIST  DD SYSOUT=X
//SYSIN    DD *
OPTIONS NOCENTER;
   DATA CUST;
   FORMAT ACCTNOC $11. ;
   KEEP CUSTNO ACCTNOC ACCTCODE JOINTACC;
   SET CUSTFILE.CUSTDLY;
   IF ACCTCODE = 'DP';
   RUN;
   PROC SORT  DATA=CUST ;BY CUSTNO;RUN;
   PROC PRINT DATA=CUST (OBS=05);TITLE 'CUST';RUN;

DATA STAFFACC;
  INFILE STAFFACC;
   KEEP STAFFNO CUSTNO STAFFNAME BRANCHCODE;
     INPUT @01 STAFFNO        $9.
           @10 CUSTNO         $11.
           @30 ACCTCODE       $5.
           @35 ACCTNOC        $11.
           @55 JOINTACC       $1.
           @56 STAFFNAME      $40.
           @96 BRANCHCODE     $03.;
RUN;
PROC SORT  DATA=STAFFACC NODUPKEY; BY CUSTNO; RUN;
PROC PRINT DATA=STAFFACC(OBS=5);TITLE 'STAFF ACCT';RUN;

DATA MERGE;
   MERGE   CUST (IN=S)  STAFFACC(IN=T); BY CUSTNO;
   IF T;
RUN;
PROC SORT  DATA=MERGE ;BY CUSTNO ACCTNOC;RUN;

DATA OUT;
   FILE DPFILE;
   SET MERGE;
        PUT @01 STAFFNO        $9.
            @10 CUSTNO         $20.
            @30 ACCTCODE       $5.
            @35 ACCTNOC        $11.
            @55 JOINTACC       $1.
            @56 STAFFNAME      $40.
            @96 BRANCHCODE     $03.;
RUN;
