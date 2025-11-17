convert program to python with duckdb and pyarrow
duckdb for process input file and output parquet&txt
assumed all the input file ady convert to parquet can directly use it

//CICUSCD5 JOB MSGCLASS=X,MSGLEVEL=(1,1),REGION=64M,NOTIFY=&SYSUID      JOB39551
//*---------------------------------------------------------------------
//DELETE   EXEC PGM=IEFBR14
//DEL1     DD DSN=CICUSCD5.UPDATE,
//            DISP=(MOD,DELETE,DELETE),SPACE=(TRK,(0))
//DEL2     DD DSN=CICUSCD5.UPDATE.DP,
//            DISP=(MOD,DELETE,DELETE),SPACE=(TRK,(0))
//DEL3     DD DSN=CICUSCD5.UPDATE.SORT,
//            DISP=(MOD,DELETE,DELETE),SPACE=(TRK,(0))
//DEL4     DD DSN=CICUSCD5.NOTFND,
//            DISP=(MOD,DELETE,DELETE),SPACE=(TRK,(0))
//DEL5     DD DSN=CICUSCD5.UPDATE.DP.TEMP,
//            DISP=(MOD,DELETE,DELETE),SPACE=(TRK,(0))
//*-------------------------------------------------------------------**
//*- GET CUSTOMER WITH STAFF 002                                      **
//*-------------------------------------------------------------------**
//SASPGRM  EXEC SAS609
//CUSTCODE DD DISP=SHR,DSN=CUSTCODE
//CUSTFILE DD DISP=SHR,DSN=CIS.CUST.DAILY
//HRMSFILE DD DISP=SHR,DSN=HCMS.STAFF.TAG(0)
//OUTFILE  DD DSN=CICUSCD5.UPDATE,
//            DISP=(NEW,CATLG,DELETE),
//            SPACE=(CYL,(100,100),RLSE),UNIT=SYSDA,
//            DCB=(LRECL=250,BLKSIZE=0,RECFM=FB)
//NOTFND   DD DSN=CICUSCD5.NOTFND,
//            DISP=(NEW,CATLG,DELETE),
//            SPACE=(CYL,(100,100),RLSE),UNIT=SYSDA,
//            DCB=(LRECL=250,BLKSIZE=0,RECFM=FB)
//DPFILE   DD DSN=CICUSCD5.UPDATE.DP.TEMP,
//            DISP=(NEW,CATLG,DELETE),
//            SPACE=(CYL,(100,100),RLSE),UNIT=SYSDA,
//            DCB=(LRECL=250,BLKSIZE=0,RECFM=FB)
//SASLIST  DD SYSOUT=X
//SYSIN    DD *
OPTIONS NOCENTER;
   DATA CUST;
   FORMAT ACCTNOC $11. BRANCH $7.;
   KEEP CUSTNO ACCTNOC ACCTCODE CUSTNAME TAXID ALIASKEY ALIAS
        JOINTACC BRANCH;
   SET CUSTFILE.CUSTDLY;
        IF ACCTCODE EQ 'DP';
        IF PRISEC = 901 ;
        BRANCH=PUT(CUSTBRCH,Z7.);
   RUN;
   PROC SORT  DATA=CUST NODUPKEY;BY ACCTNOC;RUN;
   PROC SORT  DATA=CUST ;BY CUSTNO;RUN;
   PROC PRINT DATA=CUST (OBS=05);TITLE 'CUST';RUN;

DATA HRCCODE;
   INFILE CUSTCODE;
   INPUT @ 1   CUSTNO             $20.
         @21   RECTYPE            $1.
         @22   BRANCH             $7.
         @29   C01                 3.
         @32   C02                 3.
         @35   C03                 3.
         @38   C04                 3.
         @41   C05                 3.
         @44   C06                 3.
         @47   C07                 3.
         @50   C08                 3.
         @53   C09                 3.
         @56   C10                 3.
         @59   C11                 3.
         @62   C12                 3.
         @65   C13                 3.
         @68   C14                 3.
         @71   C15                 3.
         @74   C16                 3.
         @77   C17                 3.
         @80   C18                 3.
         @83   C19                 3.
         @86   C20                 3. ;
RUN;
PROC SORT  DATA=HRCCODE; BY CUSTNO;RUN;
PROC PRINT DATA=HRCCODE(OBS=5);TITLE 'HRC CODE';RUN;

DATA CUSTACCT;
    MERGE HRCCODE (IN=X)  CUST (IN=Y);BY CUSTNO;
    IF Y;
RUN;
PROC SORT  DATA=CUSTACCT;BY ACCTNOC;RUN;
PROC PRINT DATA=CUSTACCT(OBS=5);TITLE 'CUST+ACCT';RUN;

  DATA HRMS;
  DROP ACCTNO;
    INFILE HRMSFILE DELIMITER = ';' MISSOVER DSD LRECL=102;
    INFORMAT ORGCODE        $03.;
    INFORMAT STAFFNO        $09.;
    INFORMAT STAFFNAME      $40.;
    INFORMAT ACCTNO          11.;
    INFORMAT NEWIC          $12.;
    INFORMAT OLDIC          $10.;
    INFORMAT BRANCHCODE     $03.;
      FORMAT ORGCODE        $03.;
      FORMAT STAFFNO        $09.;
      FORMAT STAFFNAME      $40.;
      FORMAT ACCTNO         $17.;
      FORMAT NEWIC          $12.;
      FORMAT OLDIC          $10.;
      FORMAT BRANCHCODE     $03.;
      INPUT  ORGCODE        $
             STAFFNO        $
             STAFFNAME      $
             ACCTNO         $
             NEWIC          $
             OLDIC          $
             BRANCHCODE     $   ;
    ACCTNOC=PUT(ACCTNO,Z11.);
         FILECODE   = 'B'; /* FOR SORTING PURPOSES */
RUN;
PROC SORT  DATA=HRMS NODUPKEY; BY ACCTNOC;RUN;
PROC PRINT DATA=HRMS(OBS=5);TITLE 'HRMS';RUN;

DATA MERGEFOUND MERGEXMTCH;
  MERGE HRMS(IN=A) CUSTACCT(IN=B);BY ACCTNOC;
  IF A AND B     THEN  OUTPUT MERGEFOUND;
  IF A AND NOT B THEN  OUTPUT MERGEXMTCH;
RUN;
PROC SORT  DATA=MERGEFOUND; BY ACCTNOC;RUN;
PROC PRINT DATA=MERGEFOUND(OBS=5);TITLE 'HRMS';RUN;

DATA NOTFOUND; /* FOR EXCEPTION - NO MATCHING  */
  SET MERGEXMTCH;
  FILE NOTFND;
      PUT @01 ORGCODE        $03.
          @05 STAFFNO        $09.
          @15 STAFFNAME      $40.
          @55 ACCTNOC        $11.
          @75 NEWIC          $12.
          @87 OLDIC          $10.
          @97 BRANCHCODE     $03.;
RUN;
PROC SORT  DATA=NOTFOUND; BY ORGCODE STAFFNAME ACCTNOC;RUN;
PROC PRINT DATA=NOTFOUND(OBS=5);TITLE 'REC NOT FOUND';RUN;

DATA DPTEAM;   /*FOR DP TEAM  - ACCOUNT LIST PER CUSTOMER */
  SET MERGEFOUND;
  KEEP STAFFNO CUSTNO ACCTCODE ACCTNOC JOINTACC;
  FILE DPFILE;
      PUT @01 STAFFNO        $9.
          @10 CUSTNO         $20.
          @30 ACCTCODE       $5.
          @35 ACCTNOC        $11.
          @55 JOINTACC       $1.
          @56 STAFFNAME      $40.
          @96 BRANCHCODE     $03.;
RUN;
PROC SORT  DATA=DPTEAM; BY STAFFNO CUSTNO ACCTCODE ACCTNOC JOINTACC;RUN;
PROC PRINT DATA=DPTEAM(OBS=5);TITLE 'DP TEAM FILE';RUN;

DATA SORT;   /*  OUTPUT PREPARE FOR SORTING */
  SET MERGEFOUND;
  FILE OUTFILE;
        /*---------------------*/
        /* ADD NEW CODES HERE  */
        /*---------------------*/
     Y=002;     PUT @02 CUSTNO   $11. @20 Y    Z3.
                    @23 RECTYPE   $1.
                    @24 BRANCH    $7.
                    @31 FILECODE  $1.
                    @33 STAFFNO   $9.
                    @42 STAFFNAME $40.;

        /*----------------------*/
        /* EXISTING CODE IN CIS */
        /*----------------------*/
     IF C01 NE 0 AND C01 NE . THEN PUT @02 CUSTNO   $11. @20 C01  Z3.
                                       @23 RECTYPE   $1.
                                       @24 BRANCH    $7.
                                       @31 FILECODE  $1.
                                       @33 STAFFNO   $9.
                                       @42 STAFFNAME $40.;
     IF C02 NE 0 AND C02 NE . THEN PUT @02 CUSTNO   $11. @20 C02  Z3.
                                       @23 RECTYPE   $1.
                                       @24 BRANCH    $7.
                                       @31 FILECODE  $1.
                                       @33 STAFFNO   $9.
                                       @42 STAFFNAME $40.;
     IF C03 NE 0 AND C03 NE . THEN PUT @02 CUSTNO   $11. @20 C03  Z3.
                                       @23 RECTYPE   $1.
                                       @24 BRANCH    $7.
                                       @31 FILECODE  $1.
                                       @33 STAFFNO   $9.
                                       @42 STAFFNAME $40.;
     IF C04 NE 0 AND C04 NE . THEN PUT @02 CUSTNO   $11. @20 C04  Z3.
                                       @23 RECTYPE   $1.
                                       @24 BRANCH    $7.
                                       @31 FILECODE  $1.
                                       @33 STAFFNO   $9.
                                       @42 STAFFNAME $40.;
     IF C05 NE 0 AND C05 NE . THEN PUT @02 CUSTNO   $11. @20 C05  Z3.
                                       @23 RECTYPE   $1.
                                       @24 BRANCH    $7.
                                       @31 FILECODE  $1.
                                       @33 STAFFNO   $9.
                                       @42 STAFFNAME $40.;
     IF C06 NE 0 AND C06 NE . THEN PUT @02 CUSTNO   $11. @20 C06  Z3.
                                       @23 RECTYPE   $1.
                                       @24 BRANCH    $7.
                                       @31 FILECODE  $1.
                                       @33 STAFFNO   $9.
                                       @42 STAFFNAME $40.;
     IF C07 NE 0 AND C07 NE . THEN PUT @02 CUSTNO   $11. @20 C07  Z3.
                                       @23 RECTYPE   $1.
                                       @24 BRANCH    $7.
                                       @31 FILECODE  $1.
                                       @33 STAFFNO   $9.
                                       @42 STAFFNAME $40.;
     IF C08 NE 0 AND C08 NE . THEN PUT @02 CUSTNO   $11. @20 C08  Z3.
                                       @23 RECTYPE   $1.
                                       @24 BRANCH    $7.
                                       @31 FILECODE  $1.
                                       @33 STAFFNO   $9.
                                       @42 STAFFNAME $40.;
     IF C09 NE 0 AND C09 NE . THEN PUT @02 CUSTNO   $11. @20 C09  Z3.
                                       @23 RECTYPE   $1.
                                       @24 BRANCH    $7.
                                       @31 FILECODE  $1.
                                       @33 STAFFNO   $9.
                                       @42 STAFFNAME $40.;
     IF C10 NE 0 AND C10 NE . THEN PUT @02 CUSTNO   $11. @20 C10  Z3.
                                       @23 RECTYPE   $1.
                                       @24 BRANCH    $7.
                                       @31 FILECODE  $1.
                                       @33 STAFFNO   $9.
                                       @42 STAFFNAME $40.;
     IF C11 NE 0 AND C11 NE . THEN PUT @02 CUSTNO   $11. @20 C11  Z3.
                                       @23 RECTYPE   $1.
                                       @24 BRANCH    $7.
                                       @31 FILECODE  $1.
                                       @33 STAFFNO   $9.
                                       @42 STAFFNAME $40.;
     IF C12 NE 0 AND C12 NE . THEN PUT @02 CUSTNO   $11. @20 C12  Z3.
                                       @23 RECTYPE   $1.
                                       @24 BRANCH    $7.
                                       @31 FILECODE  $1.
                                       @33 STAFFNO   $9.
                                       @42 STAFFNAME $40.;
     IF C13 NE 0 AND C13 NE . THEN PUT @02 CUSTNO   $11. @20 C13  Z3.
                                       @23 RECTYPE   $1.
                                       @24 BRANCH    $7.
                                       @31 FILECODE  $1.
                                       @33 STAFFNO   $9.
                                       @42 STAFFNAME $40.;
     IF C14 NE 0 AND C14 NE . THEN PUT @02 CUSTNO   $11. @20 C14  Z3.
                                       @23 RECTYPE   $1.
                                       @24 BRANCH    $7.
                                       @31 FILECODE  $1.
                                       @33 STAFFNO   $9.
                                       @42 STAFFNAME $40.;
     IF C15 NE 0 AND C15 NE . THEN PUT @02 CUSTNO   $11. @20 C15  Z3.
                                       @23 RECTYPE   $1.
                                       @24 BRANCH    $7.
                                       @31 FILECODE  $1.
                                       @33 STAFFNO   $9.
                                       @42 STAFFNAME $40.;
     IF C16 NE 0 AND C16 NE . THEN PUT @02 CUSTNO   $11. @20 C16  Z3.
                                       @23 RECTYPE   $1.
                                       @24 BRANCH    $7.
                                       @31 FILECODE  $1.
                                       @33 STAFFNO   $9.
                                       @42 STAFFNAME $40.;
     IF C17 NE 0 AND C17 NE . THEN PUT @02 CUSTNO   $11. @20 C17  Z3.
                                       @23 RECTYPE   $1.
                                       @24 BRANCH    $7.
                                       @31 FILECODE  $1.
                                       @33 STAFFNO   $9.
                                       @42 STAFFNAME $40.;
     IF C18 NE 0 AND C18 NE . THEN PUT @02 CUSTNO   $11. @20 C18  Z3.
                                       @23 RECTYPE   $1.
                                       @24 BRANCH    $7.
                                       @31 FILECODE  $1.
                                       @33 STAFFNO   $9.
                                       @42 STAFFNAME $40.;
     IF C19 NE 0 AND C19 NE . THEN PUT @02 CUSTNO   $11. @20 C19  Z3.
                                       @23 RECTYPE   $1.
                                       @24 BRANCH    $7.
                                       @31 FILECODE  $1.
                                       @33 STAFFNO   $9.
                                       @42 STAFFNAME $40.;
     IF C20 NE 0 AND C20 NE . THEN PUT @02 CUSTNO   $11. @20 C20  Z3.
                                       @23 RECTYPE   $1.
                                       @24 BRANCH    $7.
                                       @31 FILECODE  $1.
                                       @33 STAFFNO   $9.
                                       @42 STAFFNAME $40.;
   RUN;
