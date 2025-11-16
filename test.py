convert program to python with duckdb and pyarrow
duckdb for process input file and output parquet&txt
assumed all the input file ady convert to parquet can directly use it

//CICUSCD4 JOB MSGCLASS=X,MSGLEVEL=(1,1),REGION=64M,NOTIFY=&SYSUID      JOB77614
//*--------- -----------------------------------------------------------
//* ESMR 2014-746 LESLIE ANDREW DE SOUZA
//* PERFORM ONE TIME CIS EXTRACTION AND UPDATE CUSTOMER CODE
//* THIS JOBS PREPARE FILE TO INITIALIZE 002 FROM CUSTOMER CODE
//*---------------------------------------------------------------------
//DELETE   EXEC PGM=IEFBR14
//DEL1     DD DSN=RBP2.B033.CICUSCD4.STAF002.INIT,
//            DISP=(MOD,DELETE,DELETE),SPACE=(TRK,(0))
//*-------------------------------------------------------------------**
//*- GET CUSTOMER WITH STAFF 003                                      **
//*-------------------------------------------------------------------**
//SASPGRM  EXEC SAS609
//CUSTFILE  DD DISP=SHR,DSN=RBP2.B033.CIS.CUST.DAILY
//OUTFILE  DD DSN=RBP2.B033.CICUSCD4.STAF002.INIT,
//            DISP=(NEW,CATLG,DELETE),
//            SPACE=(CYL,(100,100),RLSE),UNIT=SYSDA,
//            DCB=(LRECL=250,BLKSIZE=0,RECFM=FB)
//SASLIST  DD SYSOUT=X
//SYSIN    DD *
DATA CIS;
   SET CUSTFILE.CUSTDLY;
   KEEP CUSTNO   INDORG   CUSTBRCH CUSTNAME CUSTCODEALL FILECODE
        HRC01C   HRC06C   HRC11C    HRC16C
        HRC02C   HRC07C   HRC12C    HRC17C
        HRC03C   HRC08C   HRC13C    HRC18C
        HRC04C   HRC09C   HRC14C    HRC19C
        HRC05C   HRC10C   HRC15C    HRC20C;

         HRC01C= PUT(HRC01,Z3.);
         HRC02C= PUT(HRC02,Z3.);
         HRC03C= PUT(HRC03,Z3.);
         HRC04C= PUT(HRC04,Z3.);
         HRC05C= PUT(HRC05,Z3.);
         HRC06C= PUT(HRC06,Z3.);
         HRC07C= PUT(HRC07,Z3.);
         HRC08C= PUT(HRC08,Z3.);
         HRC09C= PUT(HRC09,Z3.);
         HRC10C= PUT(HRC10,Z3.);
         HRC11C= PUT(HRC11,Z3.);
         HRC12C= PUT(HRC12,Z3.);
         HRC13C= PUT(HRC13,Z3.);
         HRC14C= PUT(HRC14,Z3.);
         HRC15C= PUT(HRC15,Z3.);
         HRC16C= PUT(HRC16,Z3.);
         HRC17C= PUT(HRC17,Z3.);
         HRC18C= PUT(HRC18,Z3.);
         HRC19C= PUT(HRC19,Z3.);
         HRC20C= PUT(HRC20,Z3.);

      /* KEEP RECORDS WITH BANK EMPLOYESS ONLY */
         IF (HRC01C= '002' OR   HRC11C= '002' OR
             HRC02C= '002' OR   HRC12C= '002' OR
             HRC03C= '002' OR   HRC13C= '002' OR
             HRC04C= '002' OR   HRC14C= '002' OR
             HRC05C= '002' OR   HRC15C= '002' OR
             HRC06C= '002' OR   HRC16C= '002' OR
             HRC07C= '002' OR   HRC17C= '002' OR
             HRC08C= '002' OR   HRC18C= '002' OR
             HRC09C= '002' OR   HRC19C= '002' OR
             HRC10C= '002' OR   HRC20C= '002'     ) ;

      /* INITIALISE BANK EMPLOYESS CODE ONLY */
         IF HRC01C= '002' THEN HRC01C= '   ';
         IF HRC02C= '002' THEN HRC02C= '   ';
         IF HRC03C= '002' THEN HRC03C= '   ';
         IF HRC04C= '002' THEN HRC04C= '   ';
         IF HRC05C= '002' THEN HRC05C= '   ';
         IF HRC06C= '002' THEN HRC06C= '   ';
         IF HRC07C= '002' THEN HRC07C= '   ';
         IF HRC08C= '002' THEN HRC08C= '   ';
         IF HRC09C= '002' THEN HRC09C= '   ';
         IF HRC10C= '002' THEN HRC10C= '   ';
         IF HRC11C= '002' THEN HRC11C= '   ';
         IF HRC12C= '002' THEN HRC12C= '   ';
         IF HRC13C= '002' THEN HRC13C= '   ';
         IF HRC14C= '002' THEN HRC14C= '   ';
         IF HRC15C= '002' THEN HRC15C= '   ';
         IF HRC16C= '002' THEN HRC16C= '   ';
         IF HRC17C= '002' THEN HRC17C= '   ';
         IF HRC18C= '002' THEN HRC18C= '   ';
         IF HRC19C= '002' THEN HRC19C= '   ';
         IF HRC20C= '002' THEN HRC20C= '   ';
         CODEFILLER = '000';
         FILECODE   = 'A'; /* FOR SORTING PURPOSES */
    CUSTCODEALL = COMPRESS(HRC01C||HRC02C||HRC03C||HRC04C||HRC05C||
                           HRC06C||HRC07C||HRC08C||HRC09C||HRC10C||
                           HRC11C||HRC12C||HRC13C||HRC14C||HRC15C||
                           HRC16C||HRC17C||HRC18C||HRC19C||HRC20C||
                           CODEFILLER);
RUN;
PROC SORT  DATA=CIS NODUPKEY; BY CUSTNO;RUN;
PROC PRINT DATA=CIS(OBS=5);TITLE 'CIS';RUN;

DATA OUT;
  SET CIS;
  FILE OUTFILE;
     PUT @ 1   CUSTNO             $20.
         @21   INDORG             $1.
         @22   CUSTBRCH           Z7.
         @29   CUSTCODEALL        $60.
         @89   FILECODE           $1.
         @90   STAFFID            $9.
         @99   CUSTNAME           $40.;
  RETURN;
  RUN;
/*
