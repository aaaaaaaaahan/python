convert program to python with duckdb and pyarrow
duckdb for process input file and output parquet&txt
assumed all the input file ady convert to parquet can directly use it

//CIBRABVB  JOB MSGCLASS=X,MSGLEVEL=(1,1),REGION=64M,NOTIFY=&SYSUID     JOB81979
//*********************************************************************
//EBKFILE  EXEC SAS609
//BRANCH   DD DISP=SHR,DSN=EBANK.BRANCH.OFFICER.COMBINE
//* EXCEL FILE FROM HELP DESK
//HELPDESK DD DISP=SHR,DSN=PBB.BRANCH.HELPDESK
//OUTFILE  DD DSN=EBANK.BRANCH.PREFER,
//            DISP=(NEW,CATLG,DELETE),
//            SPACE=(TRK,(10,10),RLSE),UNIT=SYSDA,
//            DCB=(LRECL=200,BLKSIZE=0,RECFM=FB)
//SASLIST  DD SYSOUT=X
//SYSIN    DD *
DATA BRANCH;
   INFILE BRANCH;
   INPUT @1     BANKINDC          $1.
         @2     BRANCHNO          $7.
         @9     BRANCHABRV        $3.
         @12    PB_BRNAME         $20.
         @32    ADDRLINE1         $35.
         @67    ADDRLINE2         $35.
         @102   ADDRLINE3         $35.
         @137   PHONENO           $11.
         @148   STATENO           $3.
         @151   BRANCHABRV2       $4. ;
RUN;
PROC SORT  DATA=BRANCH NODUPKEY; BY BRANCHABRV;RUN;

DATA HELPDESK;
   INFILE HELPDESK;
   INPUT @1  BRANCHABRV  $3.
         @7  HD_BRNAME   $30.;
RUN;
PROC SORT  DATA=HELPDESK NODUPKEY; BY BRANCHABRV;RUN;

DATA ACTIVE;
   MERGE  BRANCH(IN=A) HELPDESK(IN=B);BY BRANCHABRV;
   IF B;
RUN;
PROC SORT  DATA=ACTIVE; BY BRANCHNO  ;RUN;

DATA OUT;
  SET ACTIVE;
  FILE OUTFILE;
     PUT @1     BANKINDC          $1.
         @2     BRANCHNO          $7.
         @9     BRANCHABRV        $3.
         @12    PB_BRNAME         $20.
         @32    ADDRLINE1         $35.
         @67    ADDRLINE2         $35.
         @102   ADDRLINE3         $35.
         @137   PHONENO           $11.
         @148   STATENO           $3.
         @151   BRANCHABRV2       $4. ;
  RETURN;
  RUN;
