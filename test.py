convert program to python with duckdb and pyarrow
duckdb for process input file
pyarrow for output csv
assumed all the input file ady convert to parquet can directly use it

//CIKWSP01 JOB MSGCLASS=X,MSGLEVEL=(1,1),REGION=8M,NOTIFY=&SYSUID       JOB36760
//*---------------------------------------------------------------------
//DELETE   EXEC PGM=IEFBR14
//DEL1     DD DSN=KWSP.EMPLOYER.FILE.LOAD,
//            DISP=(MOD,DELETE,DELETE),SPACE=(TRK,(0))
//*---------------------------------------------------------------------
//*- PROCESS ACCOUNT INFORMATION
//*---------------------------------------------------------------------
//KWSP01   EXEC SAS609
//*WSPFILE DD DISP=SHR,DSN=KWSP.EMPLOYER.FILE.NEW
//*WSPFILE DD DISP=SHR,DSN=KWSP.EMPLOYER.FILE.CUT5
//KWSPFILE DD DISP=SHR,DSN=KWSP.EMPLOYER.FILE
//OUTFILE  DD DSN=KWSP.EMPLOYER.FILE.LOAD,
//            DISP=(NEW,CATLG,DELETE),
//            UNIT=SYSDA,SPACE=(TRK,(10,10),RLSE),
//            DCB=(LRECL=118,BLKSIZE=0,RECFM=FB)
//SASLIST  DD SYSOUT=X
//SYSIN    DD *
 DATA KWSP;
    INFILE KWSPFILE END=LAST;
      INPUT @01 REC_ID         $02.
            @03 EMPLYR_NO       19.
            @03 TOTAL_REC       10.        /* ESMR 2013-1180 */
            @22 EMPLYR_NAME1   $40. ;      /* ESMR 2013-1180 */
    /*      @22 ROB_ROC        $15.           ESMR 2013-1180 */
    /*      @37 EMPLYR_NAME1   $40.           ESMR 2013-1180 */
    /*      @77 EMPLYR_NAME2   $40.;          ESMR 2013-1180 */
            EMPLYR_NAME1 = LEFT(EMPLYR_NAME1);
            IF REC_ID EQ '' THEN ABORT 111;
            IF REC_ID EQ '01' AND (EMPLYR_NO EQ 0 OR EMPLYR_NAME1 EQ '')
               THEN ABORT 222;
            IF LAST AND REC_ID NE '02' THEN ABORT 333;
            IF LAST AND REC_ID = '02' THEN DO;
               IF TOTAL_REC NE X THEN ABORT 444;
               DELETE;
            END;
            IF REC_ID = '01' THEN X+1;     /* ESMR 2013-1180 */
            IND_ORG='O';
 RUN;
 PROC PRINT DATA=KWSP(OBS=15);TITLE 'KWSP FILE';RUN;

 DATA _NULL_;
   SET KWSP;
   FILE OUTFILE;
      ROB_ROC = ' ';            /* ESMR 2013-1180 */
      EMPLYR_NAME2 = ' ' ;      /* ESMR 2013-1180 */
      DELIM = '0D'X;
      EMPLYR_NAME1 = COMPRESS(EMPLYR_NAME1,DELIM);
      EMPLYR_NAME2 = COMPRESS(EMPLYR_NAME2,DELIM);
      PUT @01 REC_ID         $02.
          @03 IND_ORG        $01.
          @04 EMPLYR_NO      Z19.
          @23 ROB_ROC        $15.
          @38 EMPLYR_NAME1   $40.
          @78 EMPLYR_NAME2   $40.;
   RETURN;
   RUN;
