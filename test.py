convert program to python with duckdb and pyarrow
duckdb for process input file and output parquet&txt
assumed all the input file ady convert to parquet can directly use it

//*--------------------------------------------------------------------
//*- STEP 2 :  RE-SORT CUSTCODE AND REFORMAT TO FIT PROGRAM CIUPDCCD
//*--------------------------------------------------------------------
//STEP#002 EXEC SAS609
//SASLIST  DD SYSOUT=X
//INFILE    DD DISP=SHR,DSN=RBP2.B033.CICUSCD5.UPDATE
//OUTFILE   DD DSN=RBP2.B033.CICUSCD5.UPDATE.SORT,
//             DISP=(NEW,CATLG,DELETE),
//             UNIT=SYSDA,SPACE=(CYL,(100,50),RLSE),
//             DCB=(LRECL=250,BLKSIZE=0,RECFM=FB)
//SYSIN    DD *
OPTIONS NOCENTER;
DATA TEMP1;
   INFILE INFILE;
        INPUT @02 CUSTNO   $11.
              @20 F1        3.  /* CIS CUST CODE */
              @23 RECTYPE   $1.
              @24 BRANCH    $7.
              @31 FILECODE  $1.
              @33 STAFFNO   $9.
              @42 STAFFNAME $40.;
RUN;
PROC SORT DATA=TEMP1 NODUPKEY; BY CUSTNO F1;RUN;
PROC PRINT DATA=TEMP1 (OBS=10);TITLE 'CUST CODE FILE';RUN;

 DATA TEMP2;
   SET TEMP1 END=EOF;                /*A.SET ARRAY WITH 20 OCCURANCE */
   BY CUSTNO;                        /*B.EACH OCCURANCE IS 3 CHAR   */
   ARRAY LINE1 {20}  3  W1-W20;     /*C.FIELD NAME = W1,W2..W20     */

   RETAIN W1-W20;                    /*D.KEEP THE VALUES W1-W20      */

   IF (FIRST.CUSTNO) THEN DO;        /*E.IF FIRST REC, SET OCCUR TO 1*/
      I=1;
   END;

   IF I LT 11 THEN DO;               /*F.IF ARRAY LESS THAN 11,      */
      LINE1{I}=F1;                   /*  PUT F1      TO CURRENT ARRAY*/
      I+1;                           /*  SET OCCUR UP BY 1           */
   END;


   IF (LAST.CUSTNO) THEN DO;         /*G.IF LAST CUSTNO, SET OCCUR=1 */
        OUTPUT;                      /*  INITIALISE W1-W20 TO BLANK  */
        I=1;                         /*H. CONTINUE UNTIL END=EOF     */
        DO J=1 TO 20;
         LINE1{J}=' ';
        END;
   END;
 RUN;
 PROC SORT DATA = TEMP2; BY CUSTNO ;RUN;
 PROC PRINT DATA=TEMP2 (OBS=10);TITLE 'ARRAY TEMP 2 ';RUN;

DATA OUT;
   SET TEMP2;
   FILE OUTFILE;
        IF W1  EQ . THEN W1  =  0;       IF W6  EQ . THEN W6  =  0;
        IF W2  EQ . THEN W2  =  0;       IF W7  EQ . THEN W7  =  0;
        IF W3  EQ . THEN W3  =  0;       IF W8  EQ . THEN W8  =  0;
        IF W4  EQ . THEN W4  =  0;       IF W9  EQ . THEN W9  =  0;
        IF W5  EQ . THEN W5  =  0;       IF W10 EQ . THEN W10 =  0;

        IF W11 EQ . THEN W11 =  0;       IF W16 EQ . THEN W16 =  0;
        IF W12 EQ . THEN W12 =  0;       IF W17 EQ . THEN W17 =  0;
        IF W13 EQ . THEN W13 =  0;       IF W18 EQ . THEN W18 =  0;
        IF W14 EQ . THEN W14 =  0;       IF W19 EQ . THEN W19 =  0;
        IF W15 EQ . THEN W15 =  0;       IF W20 EQ . THEN W20 =  0;
          PUT @01 CUSTNO   $11.
              @21 RECTYPE  $1.
              @22 BRANCH   $7.
              @29  W1      Z3.
              @32  W2      Z3.
              @35  W3      Z3.
              @38  W4      Z3.
              @41  W5      Z3.
              @44  W6      Z3.
              @47  W7      Z3.
              @50  W8      Z3.
              @53  W9      Z3.
              @56  W10     Z3.
              @59  W11     Z3.
              @62  W12     Z3.
              @65  W13     Z3.
              @68  W14     Z3.
              @71  W15     Z3.
              @74  W16     Z3.
              @77  W17     Z3.
              @80  W18     Z3.
              @83  W19     Z3.
              @86  W20     Z3.
              @89 FILECODE  $1.
              @90  STAFFNO   $9.
              @99  STAFFNAME $40.;
   RUN;

//*-------------------------------------------------------------------**
//*- GET LISTING OF ACCOUNT PER STAFF                                 **
//*-------------------------------------------------------------------**
//SASPGRM  EXEC SAS609
//CUSTFILE DD DISP=SHR,DSN=RBP2.B033.CIS.CUST.DAILY
//STAFFACC DD DISP=SHR,DSN=RBP2.B033.CICUSCD5.UPDATE.DP.TEMP
//DPFILE   DD DSN=RBP2.B033.CICUSCD5.UPDATE.DP,
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
