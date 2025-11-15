convert program to python with duckdb and pyarrow
duckdb for process input file and output parquet&txt
assumed all the input file ady convert to parquet can directly use it

//CICISALS JOB MSGCLASS=X,MSGLEVEL=(1,1),REGION=64M,NOTIFY=&SYSUID      JOB59669
//*---------------------------------------------------------------------
//DELETE   EXEC PGM=IEFBR14
//DEL1     DD DSN=CIS.ALIAS.CHANGE.RPT,
//            DISP=(MOD,DELETE,DELETE),SPACE=(TRK,(0))
//*---------------------------------------------------------------------
//* LIST OF BNMID AND ALIAS ONLY - CHANGES ONLY
//*---------------------------------------------------------------------
//SET1B1   EXEC SAS609
//SORTWK01 DD UNIT=SYSDA,SPACE=(CYL,(1000,500))
//SORTWK02 DD UNIT=SYSDA,SPACE=(CYL,(1000,500))
//SORTWK03 DD UNIT=SYSDA,SPACE=(CYL,(1000,500))
//EODRPT   DD DISP=SHR,DSN=CIDARPGS
//CCRSBANK DD DISP=SHR,DSN=CIS.CUST.DAILY.ACTVOD
//NAMEFILE DD DISP=SHR,DSN=UNLOAD.PRIMNAME.OUT
//CTRLDATE DD DISP=SHR,DSN=SRSCTRL1(0)
//OUTFILE  DD DSN=CIS.ALIAS.CHANGE.RPT,
//            DISP=(NEW,CATLG,DELETE),
//            SPACE=(CYL,(50,50),RLSE),UNIT=SYSDA,
//            DCB=(LRECL=500,BLKSIZE=0,RECFM=FB)
//SASLIST  DD SYSOUT=X
//SYSIN    DD *
OPTION NOCENTER;

DATA GETDATE;
     FORMAT XX Z6.;
     TM=TIME();
     XX=COMPRESS(PUT (TM,TIME8.),':');
     CALL SYMPUT('TIMEX', PUT(XX,Z6.));
RUN;
PROC PRINT;RUN;
 /*----------------------------------------------------------------*/
 /*    SET DATES                                                   */
 /*----------------------------------------------------------------*/
    DATA SRSDATE;
          INFILE CTRLDATE;
            INPUT @001  SRSYY    4.
                  @005  SRSMM    2.
                  @007  SRSDD    2.;

          /* DISPLAY TODAY REPORTING DATE*/
          TODAYSAS=MDY(SRSMM,SRSDD,SRSYY);
          CALL SYMPUT('DATE1',PUT(TODAYSAS,8.));
          CALL SYMPUT('YEAR' ,PUT(SRSYY,Z4.));
          CALL SYMPUT('MONTH',PUT(SRSMM,Z2.));
          CALL SYMPUT('DAY'  ,PUT(SRSDD,Z2.));
    RUN;
   PROC PRINT;RUN;
DATA EODRPT;
  INFILE EODRPT;
   FORMAT CUSTNO $11. UPDOPER $10. OLDVALUE $150. NEWVALUE $150.
          ALIASKEY $3.;
   INPUT @39   OPERID                 $08.
         @106  INDALS                 PD2.
         @132  INDFUNCT               $01.
         @133  CUSTNOX                PD6.
         @181  ALIAS                  $23.
         ;
         IF OPERID = ' ' THEN DELETE;
         IF INDALS = '230';
         ALIASKEY  = SUBSTR(ALIAS,1,3);
         IF ALIASKEY IN ('CH ','CV ','EN ','NM ','VE ') THEN DELETE;
         IF ALIASKEY IN ('BR ','CI ','PC ','SA ','GB '
                        ,'LP ', 'RE ','AI ','AO ',
                        'IC ','SI ','BI ','PP ','ML ','PL ','BC ');
         IF ALIASKEY IN ('AI ','AO ')
            THEN FIELDS = 'BNM ASSIGNED ID';
         IF ALIASKEY NOT IN ('AI','AO')
            THEN FIELDS = 'ID NUMBER';
         CUSTNO = PUT(CUSTNOX,Z11.) ;
         UPDOPER  = OPERID;
         IF INDFUNCT = 'D' THEN DO;
            OLDVALUE = ALIAS;
            NEWVALUE = ' ';
         END;
         IF INDFUNCT = 'A' THEN DO;
            OLDVALUE = ' ';
            NEWVALUE = ALIAS;
         END;
PROC SORT  DATA=EODRPT NODUPKEY; BY CUSTNO INDFUNCT ALIAS;
PROC PRINT DATA=EODRPT(OBS=20);TITLE 'EODRPT';
RUN;

DATA NAME;
  INFILE NAMEFILE;
  INPUT  @005  CUSTNO           $11.
         @089  CUSTNAME         $40.;
RUN;
PROC SORT  DATA=NAME; BY CUSTNO ;RUN;
PROC PRINT DATA=NAME(OBS=5);TITLE 'NAME';RUN;

DATA ACTIVE;
  INFILE CCRSBANK;
   INPUT  @001   CUSTNO          $11.
          @021   ACCTCODE        $5.
          @026   ACCTNOC         $20.
          @055   DATEOPEN        $10.
          @065   DATECLSE        $10.
          ;
          IF ACCTCODE NOT IN ('DP   ','LN   ') THEN DELETE;
RUN;
PROC SORT  DATA=ACTIVE; BY CUSTNO DESCENDING DATEOPEN;RUN;
PROC PRINT DATA=ACTIVE(OBS=5);TITLE 'ACTIVE';RUN;

DATA LISTACT;
  SET ACTIVE;
  KEEP CUSTNO ACCTCODE ACCTNOC;
  IF DATECLSE NOT IN ('       .','        ','00000000') THEN DELETE;
RUN;
PROC SORT  DATA=LISTACT NODUPKEY; BY CUSTNO;RUN;
PROC PRINT DATA=LISTACT(OBS=5);TITLE 'ACCOUNT';RUN;

   DATA MERGE_A;
        MERGE  EODRPT(IN=A) NAME(IN=B) LISTACT(IN=C);
        BY CUSTNO;
        IF (A AND C) AND B;
   RUN;
   PROC SORT  DATA=MERGE_A NODUPKEY;BY CUSTNO ALIAS;RUN;
   PROC PRINT DATA=MERGE_A(OBS=15);TITLE 'MERGE';RUN;

   DATA RECORDS;
     SET MERGE_A;
     FILE OUTFILE;
     UPDDATX = "&DAY"||"/"||"&MONTH"||"/"||"&YEAR";
     PUT  @001   UPDOPER         $10.
          @021   CUSTNO          $20.
          @041   ACCTNOC         $20.
          @061   CUSTNAME        $40.
          @101   FIELDS          $20.
          @121   OLDVALUE        $150.
          @271   NEWVALUE        $150.
          @424   UPDDATX         $10.
          ;
     RUN;
    LINECNT = 9;
  RETURN;
RUN;
