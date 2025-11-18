convert program to python with duckdb and pyarrow
duckdb for process input file and output parquet&txt
assumed all the input file ady convert to parquet can directly use it

//CIBMSPEN JOB MSGCLASS=X,MSGLEVEL=(1,1),REGION=64M,NOTIFY=&SYSUID      JOB86639
//*--------------------------------------------------------------------
//INITDASD EXEC PGM=IEFBR14
//DEL1     DD DSN=CIBMSYST.PENDING,
//            DISP=(MOD,DELETE,DELETE),UNIT=SYSDA,SPACE=(TRK,(0))
//*---------------------------------------------------------------------
//PENDING   EXEC SAS609
//IEFRDER   DD DUMMY
//CTRLDATE  DD DISP=SHR,DSN=SRSCTRL1(0)
//CIBMSYST  DD DISP=SHR,DSN=UNLOAD.CIBMSYST.PENDING
//OUTFILE   DD DSN=CIBMSYST.PENDING,
//             DISP=(NEW,CATLG,DELETE),
//             UNIT=SYSDA,SPACE=(CYL,(50,30),RLSE),
//             DCB=(LRECL=100,BLKSIZE=0,RECFM=FB)
//SASLIST   DD SYSOUT=X
//SYSIN     DD *
OPTION NOCENTER;
  DATA DATE;
     INFILE CTRLDATE;
     INPUT @1   TODAYYY          4.
           @5   TODAYMM          2.
           @7   TODAYDD          2.;
           TODAYDATE=MDY(TODAYMM,TODAYDD,TODAYYY);
           CALL SYMPUT('TODAYDT', PUT(TODAYDATE,YYMMDD10.));
  RUN;
  PROC PRINT;RUN;
  /*---------------------------------------------------------*/
  /* DEFINE INPUT FILE                                       */
  /*---------------------------------------------------------*/
  DATA PENDING;
    INFILE CIBMSYST;
    INPUT @001 LOAD_DATE          $10.   /* BMS_LOAD_DATE       */
          @011 BRANCHNO           $3.    /* BMS_BRANCH_NO       */
          @014 APPL_CODE          $5.    /* BMS_APPL_CODE       */
          @019 ACCTNOC            $20.   /* BMS_ACCT_NO         */
          @039 PBB_NAME           $40.   /* BMS_PBB_NAME        */
          @079 CATEGORY           $1.    /* BMS_CATEGORY        */
          @080 MATCH_TYPE         $4.    /* BMS_MATCH_TYPE      */
          @084 BRANCH_ABBR        $7.    /* BMS_BRANCH_ABBR     */
          @091 ACCT_TYPE          $1.    /* BMS_ACCT_TYPE       */
          @092 JIM_CODE           $3.    /* BMS_JIM_CODE        */
          @095 BANKRUPTCY_NO      $20.   /* BMS_BANKRUPTCY_NO   */
          @115 JIM_REFER_NO       $30.   /* BMS_JIM_REFER_NO    */
          @145 ADJUDGE_DATE       $10.   /* BMS_ADJUDGE_DATE    */
          @155 JIM_NAME           $70.   /* BMS_JIM_NAME        */
          @225 NEWIC              $15.   /* BMS_ALIAS           */
          @240 OLDIC              $10.   /* BMS_OLDIC           */
          @250 LEDGERBAL1         $15.   /* BMS_CURRENT_BALANCE */
          @265 ACCTSTATUS         $25.   /* BMS_ACTION_ORIGINAL */
          @290 ACTION_TAKEN       $25.   /* BMS_ACTION_TAKEN    */
          @315 ACTION_USERID      $10.   /* BMS_ACTION_USERID   */
          @325 ACTION_DATE        $10.   /* BMS_ACTION_DATE     */
          @335 ACTION_TIME        $08.   /* BMS_ACTION_TIME     */
          @343 WAIVE_REMARKS      $40.   /* BMS_WAIVE_REMARKS   */
          @383 BANKINDC           $1.    /* BMS_BANK_INDC       */
          @384 PERIOD_OVERDUEX     3.;   /* BMS_PERIOD_OVERDUE  */
          IF LOAD_DATE = "&TODAYDT" THEN DELETE;
          PERIOD_OVERDUE = PERIOD_OVERDUEX + 1;
        ;
  RUN;
 /*----------------------------------------------------------------*/
 /*    OUTPUT FILE                                                 */
 /*----------------------------------------------------------------*/
 DATA OUT;
    SET PENDING;
    FILE OUTFILE;
    PUT @001 LOAD_DATE          $10.    /* BMS_LOAD_DATE */
        @011 BRANCHNO           $3.     /* BMS_BRANCH_NO */
        @014 APPL_CODE          $5.     /* BMS_APPL_CODE */
        @019 ACCTNOC            $20.    /* BMS_ACCT_NO   */
        @039 PERIOD_OVERDUE     Z3.
        ;
   RETURN;
 RUN;
