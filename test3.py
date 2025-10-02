convert program to python with duckdb
duckdb for process input file
output make it to txt file
assumed all the input file ady convert to parquet can directly use it

//CISECOMT JOB MSGCLASS=X,MSGLEVEL=(1,1),REGION=8M,NOTIFY=&SYSUID       JOB91279
//**********************************************************************
//*- GENERATE REPORT WITH RACE CODE OTHERS
//**********************************************************************
//REPORTS   EXEC SAS609
//* CUST OTHERS FROM CCRSRACE
//CUSTOTH   DD DISP=SHR,DSN=CIS.RACE
//OUTOTH    DD DSN=ETHNIC.REPORT.MONTHLY(+1),
//             DISP=(NEW,CATLG,DELETE),UNIT=SYSDA,
//             SPACE=(TRK,(100,50),RLSE),
//             DCB=(LRECL=150,BLKSIZE=0,RECFM=FBA)
//SASLIST   DD SYSOUT=X
//SYSIN     DD *
 /*----------------------------------------------------------------*/
 /*   REPORTING DATA SECTION                                       */
 /*----------------------------------------------------------------*/
 DATA REPTDATE;
   REPTDATE=TODAY();
   CALL SYMPUT('RDATE',PUT(REPTDATE,DDMMYY10.));
 ;
 /*----------------------------------------------------------------*/
 /*   INPUT DATA DECLARATION                                       */
 /*----------------------------------------------------------------*/
 DATA ERRDATA;
   INFILE CUSTOTH;
   INPUT  @001 ALIASKEY           $02.
          @005 ALIAS              $12.
          @021 CUSTNAME           $40.
          @062 CUSTNO             $11.
          @074 CUSTBRCH           $03.;
 PROC SORT DATA=ERRDATA; BY CUSTBRCH;
 RUN;

 /*----------------------------------------------------------------*/
 /* PREPARE FOR ERROR UPDATE RACE CODE FOR OTHERS REPORT           */
 /*----------------------------------------------------------------*/
 DATA ERPT;
  FILE OUTOTH PRINT HEADER=NEWPAGE;
  IF TRN=0 THEN DO;
     PUT @002 ' ';
     PUT @047 '**********************************';
     PUT @047 '*                                *';
     PUT @047 '*         EMPTY REPORT           *';
     PUT @047 '*                                *';
     PUT @047 '**********************************';
     PUT @002 ' ';
  END;
  RETAIN TRN;
   SET ERRDATA NOBS=TRN END=EOF; BY CUSTBRCH;
   FILE OUTOTH NOTITLE;
   IF _N_ = 1 THEN DO;
      PAGECNT = 0;
   END;
   IF  LINECNT >= 52 OR FIRST.CUSTBRCH  THEN DO;
      PUT _PAGE_;
   END;
   LINECNT + 6;
   BRCNT + 1;

   LINECNT + 1;
   PUT @002 CUSTNO       $11.
       @017 ALIAS        $20.
       @040 CUSTNAME     $40.
       @084 CUSTBRCH     $08.;
   BRCUST   + 1;
   GRCUST   + 1;

   IF LAST.CUSTBRCH AND BRCUST > 0 THEN DO;
      PUT  @25 'TOTAL = '
           @35  BRCUST  9.;

           BRCUST  = 0;
           PAGECNT = 0;
   END;
   IF EOF THEN DO;
      PUT /@3    'GRAND TOTAL OF ALL BRANCHES = '
           @35   GRCUST     9.;
   END;
   RETURN;

   NEWPAGE:
      PAGECNT +1;
      LINECNT = 0;
      PUT @001 'REPORT NO : ETHNIC/OTHERS'
          @055 'PUBLIC BANK BERHAD'
          @094 'PAGE        : ' PAGECNT   4.
         /@001 'PROGRAM ID  : CISECOMT'
          @094 'REPORT DATE : ' "&RDATE"
         /@001 'BRANCH      : 00' CUSTBRCH 5.
          @040 'LIST OF MALAYSIAN WITH ETHNIC CODE OTHERS'
         /@040 '=========================================';
      PUT @002 'CIS NUMBER'
          @017 'MYKAD NUMBER'
          @040 'NAME'
          @084 'BRANCH';
      PUT @002 '==========='
          @017 '===================='
          @040 '========================================'
          @084 '========';
      LINECNT +7;
   RETURN;
 RUN;
