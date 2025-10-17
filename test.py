convert program to python with duckdb and pyarrow
duckdb for process input file
pyarrow for output csv
assumed all the input file ady convert to parquet can directly use it

//CISOCSOF JOB MSGCLASS=X,MSGLEVEL=(1,1),REGION=64M,NOTIFY=&SYSUID      JOB10981
//*---------------------------------------------------------------------
//DELETE   EXEC PGM=IEFBR14
//DEL1     DD DSN=PERKESO.EMPLFILE.FULLLOAD,
//            DISP=(MOD,DELETE,DELETE),SPACE=(TRK,(0))
//DEL2     DD DSN=PERKESO.EMPLFILE.FULLLOAD.UNQ,
//            DISP=(MOD,DELETE,DELETE),SPACE=(TRK,(0))
//*---------------------------------------------------------------------
//*- PROCESS EMPLOYER INFORMATION (VALIDATE FILE ONLY)
//*---------------------------------------------------------------------
//VALIDATE EXEC SAS609
//EMPLFILE DD DISP=SHR,DSN=PERKESO.EMPLFILE.FULL(0)
//SASLIST  DD SYSOUT=X
//SYSIN    DD *
OPTION NOCENTER;
 DATA EMPLFULL;
    RETAIN X Y;
    INFILE EMPLFILE END=LAST EOF=EOFRTN;
      INPUT @01  RECORD_TYPE     $1.     
            @02  NEW_EMPL_CODE   $12.
            @02  TOTAL_REC       $7.
            @14  OLD_EMPL_CODE   $9.
            @23  EMPL_NAME       $100.      
            @123 ACR_RECEIPT_NO  $20.       
            @143 ACR_AMOUNT      $20.;     
      IF RECORD_TYPE = 'H' THEN DELETE;
      IF RECORD_TYPE = 'D' THEN X+1;
      IF RECORD_TYPE = 'F' THEN DO;
         Y+TOTAL_REC;                       /*TOTAL OF FOOTER*/
         TOTAL_REC_NUM = Y * 1;             
         IF TOTAL_REC_NUM NE X THEN ABORT 88;
      END;
      /* ------------------------- */
      /* CHECK FOR INCOMPLETE FILE */
      /* ------------------------- */
      IF RECORD_TYPE = 'F' THEN F+1;
      IF LAST AND F = 0 THEN ABORT 77;
      IF RECORD_TYPE IN (' ','F') THEN DELETE;

      /*-----------------------------------------------*/
      /* CHECK FOR ACR OR ECR CODE                     */
      /*-----------------------------------------------*/
      IF RECORD_TYPE = 'D' AND SUBSTR(ACR_RECEIPT_NO,1,3) NE 'ACR'
      THEN ABORT 55;

    EOFRTN:
      IF _N_ = 1  THEN ABORT 66;

 RUN;

//**********************************************************************
//* PROCESS THE 9 FILES TO CATER FOR CEK TAK LAKU
//**********************************************************************
//CEKXLAKU EXEC SAS609
//NEWFILES DD DISP=SHR,DSN=PERKESO.EMPLFILE.FULL(0)
//OLDFILES DD DISP=SHR,DSN=PERKESO.EMPLFILE.FULL(-1)
//         DD DISP=SHR,DSN=PERKESO.EMPLFILE.FULL(-2)
//         DD DISP=SHR,DSN=PERKESO.EMPLFILE.FULL(-3)
//         DD DISP=SHR,DSN=PERKESO.EMPLFILE.FULL(-4)
//         DD DISP=SHR,DSN=PERKESO.EMPLFILE.FULL(-5)
//         DD DISP=SHR,DSN=PERKESO.EMPLFILE.FULL(-6)
//         DD DISP=SHR,DSN=PERKESO.EMPLFILE.FULL(-7)
//         DD DISP=SHR,DSN=PERKESO.EMPLFILE.FULL(-8)
//         DD DISP=SHR,DSN=PERKESO.EMPLFILE.FULL(-9)
//* FILE SIZE LONGER FOR THE LAST INDICATOR (FOR SORTING PURPOSE)
//ALLFILES DD DSN=PERKESO.EMPLFILE.FULLLOAD,
//            DISP=(NEW,CATLG,DELETE),
//            UNIT=SYSDA,SPACE=(CYL,(200,100),RLSE),
//            DCB=(LRECL=250,BLKSIZE=0,RECFM=FB)
//SASLIST  DD SYSOUT=X
//SYSIN    DD *
OPTION NOCENTER;
 DATA NEWFILES;
    INFILE NEWFILES;
      INPUT @01  RECORD_TYPE     $1.        /* VALUE H , D , F */
            @02  NEW_EMPL_CODE   $12.
            @14  OLD_EMPL_CODE   $9.
            @23  EMPL_NAME       $100.      /* TRUNCATE TO 40 */
            @123 ACR_RECEIPT_NO  $20.       /* ESMR 2017-2133*/
            @143 ACR_AMOUNT      $20.;      /* ESMR 2019-685 */
      IF RECORD_TYPE = 'D';
      LAST_INDICATOR = 'A';
 RUN;
 PROC PRINT DATA=NEWFILES(OBS=10);TITLE 'NEW FILE';RUN;

 DATA OLDFILES;
    INFILE OLDFILES;
      INPUT @01  RECORD_TYPE     $1.        /* VALUE H , D , F */
            @02  NEW_EMPL_CODE   $12.
            @14  OLD_EMPL_CODE   $9.
            @23  EMPL_NAME       $100.      /* TRUNCATE TO 40 */
            @123 ACR_RECEIPT_NO  $20.       /* ESMR 2017-2133*/
            @143 ACR_AMOUNT      $20.;      /* ESMR 2019-685 */
      IF RECORD_TYPE = 'D';
      LAST_INDICATOR = 'B';
 RUN;
 PROC PRINT DATA=OLDFILES(OBS=10);TITLE 'OLD FILE';RUN;

 DATA ALLFILES;
   SET NEWFILES OLDFILES;
 RUN;
 PROC SORT  DATA=ALLFILES ;BY NEW_EMPL_CODE
                              ACR_RECEIPT_NO
                              LAST_INDICATOR ; RUN;
 PROC PRINT DATA=ALLFILES(OBS=100);TITLE 'ALL FILE';RUN;

 DATA _NULL_;
   SET ALLFILES;
   FILE ALLFILES;
      PUT @001 NEW_EMPL_CODE   $12.
          @013 ACR_RECEIPT_NO  $20.
          @033 OLD_EMPL_CODE   $9.
          @042 EMPL_NAME       $100.
          @142 ACR_AMOUNT      $20.
          @200 LAST_INDICATOR  $1.   /* OLDNEW RECORD INDICATOR */
          ;
   RETURN;
   RUN;
//**********************************************************************
//* EJS A2018-5906
//* REMOVE DUPLICATE PRIOR TO LOADING DATABASE
//**********************************************************************
//UNQREC   EXEC PGM=ICETOOL
//TOOLMSG  DD SYSOUT=*
//DFSMSG   DD SYSOUT=*
//INDD01   DD DISP=SHR,DSN=PERKESO.EMPLFILE.FULLLOAD
//OUTDD01  DD DSN=PERKESO.EMPLFILE.FULLLOAD.UNQ,
//            DISP=(NEW,CATLG,DELETE),
//            SPACE=(CYL,(500,300),RLSE),UNIT=SYSDA,
//            DCB=(LRECL=161,BLKSIZE=0,RECFM=FB)
//TOOLIN   DD *
   SELECT FROM(INDD01) TO(OUTDD01) ON(1,32,CH) FIRST
