convert program to python with duckdb and pyarrow
duckdb for process input file
pyarrow use for output
assumed all the input file ady convert to parquet can directly use it

//CISVPBCS JOB MSGCLASS=X,MSGLEVEL=(1,1),REGION=64M,NOTIFY=&SYSUID      JOB18660
//*---------------------------------------------------------------------
//* EXTRACTING ALL CARD ACCOUNTS
//*---------------------------------------------------------------------
//ALLCARD  EXEC SAS609
//IEFRDER   DD DUMMY
//SORTWK01  DD UNIT=SYSDA,SPACE=(CYL,(1000,500))
//SORTWK02  DD UNIT=SYSDA,SPACE=(CYL,(1000,500))
//SORTWK03  DD UNIT=SYSDA,SPACE=(CYL,(1000,500))
//SORTWK04  DD UNIT=SYSDA,SPACE=(CYL,(1000,500))
//SORTWK05  DD UNIT=SYSDA,SPACE=(CYL,(1000,500))
//SORTWK06  DD UNIT=SYSDA,SPACE=(CYL,(1000,500))
//MERCHANT  DD DISP=SHR,DSN=UNICARD.MERCHANT
//VISAFILE  DD DISP=SHR,DSN=UNICARD.VISA
//OUTFILE   DD DSN=SNGLVIEW.PBCS,
//             DISP=(NEW,CATLG,DELETE),
//             UNIT=SYSDA,SPACE=(CYL,(500,200),RLSE),
//             DCB=(LRECL=1000,BLKSIZE=0,RECFM=FB)
//SASLIST   DD SYSOUT=X
//SYSIN     DD *
 /*----------------------------------------------------------------*/
 /*    UNICARD MERCHANT DECLARATION                                */
 /*----------------------------------------------------------------*/
   DATA MERCHANT;
     INFILE MERCHANT;
     FORMAT ACCTNO $20. ACCTSTATUS $20. CUSTNAME$40.;
         INPUT @01  ACCTNO             $10.        /*MERCHANT NO    */
               @11  CUSTNAME1          $30.        /*MERCHANT NAME  */
               @41  DATEOPEN         DDMMYY6.      /*OPEN DATE      */
               @47  DATECLSE         DDMMYY6.      /*CLOSED DATE    */
               @53  MERCHTYPE          $4.         /*MERCH CATEGORY */
               @57  ALIAS              $14.;       /*MERCH REGNO    */
               CUSTNAME=CUSTNAME1;
               ACCTCODE='MERCH';
               COLLINDC='N';
               BANKINDC='C';
               PRIMSEC='P';
               OCCUPDESC='MERCHANT CODE : '||MERCHTYPE;
               IF DATECLSE EQ .  THEN ACCTSTATUS = 'ACTIVE';
               IF DATECLSE NE .  THEN ACCTSTATUS = 'CLOSED';
               INDORG='O';
 PROC SORT DATA=MERCHANT; BY ACCTNO; RUN;
 PROC PRINT DATA=MERCHANT(OBS=10); TITLE 'MERCHANT FILE'; RUN;
 /*----------------------------------------------------------------*/
 /*    UNICARD VISA FILE DECLARATION                               */
 /*----------------------------------------------------------------*/
   DATA VISA;
     INFILE VISAFILE;
     FORMAT ACCTNO $20. ACCTSTATUS $20. ;
         INPUT @1   ACCTNO             $16.        /*CARD NUMBER    */
               @17  CARDNOGTOR         $16.        /*CARDNO (GTOR)  */
               @33  CUSTNAME           $40.        /*CUSTNAME       */
               @73  ALIASKEY           $3.         /*DOC TYPE       */
               @76  ALIAS1             $12.        /*CUSTNO (CARD)  */
               @88  ALIAS2             $12.        /*REFNO  (CARD)  */
               @100 EMPLNAME           $30.        /*EMPLOYER NAME  */
               @130 OCCUPDESC          $20.        /*               */
               @150 DATEOPEN           DDMMYY6.    /*               */
               @156 CREDITLIMIT        6.          /*               */
               @162 ACCTTYPE           $2.         /* I/IA/IS       */
               @164 ACCTCLSECODE       $1.         /*               */
               @165 ACCTCLSEDESC       $20.        /*               */
               @185 DATECLSE           DDMMYY6.    /*               */
               @191 RECLASSCODE        $1.         /*               */
               @192 CURRENTBAL         9.2         /*               */
               @201 CURRENTBALSIGN     $1.         /*               */
               @202 AUTHCHARGE         10.2        /*               */
               @212 AUTHCHARGESIGN     $1.         /*               */
               @213 ADDRESSTYPE        $1.         /*HOME/BUS/COLL  */
               @214 ADDRESSLINE1       $30.        /*               */
               @244 ADDRESSLINE2       $30.        /*               */
               @274 ADDRESSLINE3       $30.        /*               */
               @304 ADDRESSLINE4       $30.        /*               */
               @334 ADDRESSLINE5       $30.        /*               */
               @364 POSTCODE           $5.         /*               */
               @369 MONITORCODE        $1.         /*               */
               @478 PRODDESC           $20.        /*PRODUCT DESCRIB*/
               @498 COLLNO             $5.         /*FDR VALUE      */
               @503 CCELCODE           $1.         /*CANCEL CODE    */
               @504 CCELCODEDESC       $20.        /*CCEL CODE DESC */
               @524 CRINDC             $20.        /*CREDIT/DEBIT   */
               @525 DOBDOR             $8.;        /*DOB - INDV     */

         BANKINDC='C';
         IF ALIAS1 NE '' THEN ALIAS=ALIAS1;
         IF ALIAS1 EQ '' AND ALIAS2 NE '' THEN DO;
            ALIAS=ALIAS1;
            ALIASKEY='';
         END;
         IF ALIAS1 EQ '' AND ALIAS2 EQ '' THEN ALIASKEY='';

         IF CRINDC = 'C' THEN ACCTCODE='CREDT';
         IF CRINDC = 'D' THEN ACCTCODE='DEBIT';

         IF ACCTTYPE = 'I ' THEN RELATIONDESC='PRINCIPAL CARD ';
         ELSE IF ACCTTYPE = 'IA' THEN RELATIONDESC='PRINC + SUPP   ';
         ELSE IF ACCTTYPE = 'IS' THEN RELATIONDESC='SUPP SEPARATE  ';
         ELSE IF ACCTTYPE = 'A ' THEN RELATIONDESC='SUPP COMBINE   ';
         ELSE RELATIONDESC='UNKNOWN        ';

         IF CCELCODE EQ '' AND CCELCODEDESC EQ ''
               THEN ACCTSTATUS='ACTIVE               ';
         IF CCELCODE NE '' AND CCELCODEDESC NE ''
               THEN ACCTSTATUS=CCELCODEDESC;
         IF CCELCODE NE ' ' AND CCELCODEDESC EQ ''
               THEN ACCTSTATUS='INACTIVE             ';
         IF DATECLSE NE . THEN ACCTSTATUS='CLOSED    ';
         IF ACCTSTATUS='ACTIVE   ' THEN DATECLSE = .;

         IF COLLNO NE '00000' THEN DO;
            COLLINDC='Y';
            COLLDESC='FIXED DEPOSIT';
         END;
         ELSE DO;
            COLLINDC='N';
            COLLDESC='';
         END;

         IF SUBSTR(ACCTNO,14,1)='1' THEN PRIMSEC='P';
         ELSE PRIMSEC='S';

         IF SUBSTR(ACCTNO,15,1) = '0' THEN PRISEC='P';
         ELSE PRISEC='S';

         INDORG='I';
         BAL1INDC='O/B';                           /*OUTSTANDING BAL  */
         IF CURRENTBALSIGN = '-' THEN CURRENTBAL=CURRENTBAL*(-1);
         BAL1=CURRENTBAL-AUTHCHARGE;
         BAL1=BAL1*(-1);
         AMT1INDC='C/L';                           /*CREDIT LIMIT     */
         AMT1=CREDITLIMIT;                         /*CREDIT LIMIT     */
   RUN;
 PROC SORT DATA=VISA; BY ACCTNO; RUN;
 PROC PRINT DATA=VISA(OBS=20); TITLE 'VISA FILE '; RUN;

 /*----------------------------------------------------------------*/
 /*   APPEND UNICARD ACCT AND CARD FILES                           */
 /*----------------------------------------------------------------*/
   DATA MRGCARD;
       SET MERCHANT VISA;BY ACCTNO;
       ACCTBRABBR='PBCSS';
       JOINTACC='N';
       IF DOBDOR = '00000000' THEN DOBDOR = '';
   RUN;

 PROC SORT DATA=MRGCARD; BY ACCTCODE ; RUN;
 PROC PRINT DATA=MRGCARD(OBS=10);TITLE 'MERCH + CARD FILES'; RUN;

 /*----------------------------------------------------------------*/
 /*   OUTPUT DETAILS                                               */
 /*----------------------------------------------------------------*/
DATA TEMPOUT;
  SET MRGCARD;
  FILE OUTFILE;

     CUSTNAME=TRANWRD(CUSTNAME,'\','\\');    /* 2011-2834 */
     PUT @1    '"'
         @2    '033'               @5     '","' /*BANK NO        */
      /* @8    CUSTNO         $11. */     @19    '","'
         @22   INDORG         $01. @23    '","'
         @26   CUSTNAME       $40. @66    '","'
         @69   ALIASKEY       $03. @72    '","'
         @75   ALIAS          $20. @95    '","'
      /* @98   OCCUPCD1       $05. */     @103   '","'
         @106  OCCUPDESC      $20. @126   '","'
         @129  EMPLNAME       $60. @189   '","'
         @192  ACCTBRABBR     $07. @199   '","'
      /* @202  BRANCHNO        Z5. */     @207   '","'
         @210  ACCTCODE       $05. @215   '","' /*ACCOUNT CODE   */
         @218  ACCTNO         $20. @238   '","' /*ACCOUNT NUM    */
      /* @241  NOTENO         $05. */     @246   '","'
         @249  BANKINDC       $01. @250   '","'
         @253  PRIMSEC        $01. @254   '","' /*PRIM SEC ACCT  */
      /* @257  RLENCODE        Z3. */     @260   '","'
         @263  RELATIONDESC   $15. @278   '","'
         @281  ACCTSTATUS     $25. @306   '","'
         @309  DATEOPEN   YYMMDDN8. @319   '","'
         @322  DATECLSE   YYMMDDN8. @332   '","'
      /* @335  SIGNATORY      $01. */     @336   '","'
         @339  BAL1INDC       $05. @344   '","' /*LEDGER BAL     */
         @350  BAL1           13.2 @365   '","' /*LEDGER BAL AMT */
      /* @368  BAL2INDC       $05. */     @373   '","'
      /* @376  BAL2           13.2 */     @391   '","'
         @394  AMT1INDC       $05. @399   '","' /*OD LIMIT       */
         @402  AMT1           13.2 @417   '","' /*OD LIMIT AMT   */
      /* @420  AMT2INDC       $05. */     @425   '","'
      /* @428  AMT2           13.2 */     @443   '","'
      /* @446  COLLTYPE       $05. */     @451   '","'
      /* @454  COLLCODE       Z05. */     @459   '","'
         @462  COLLDESC       $30. @492   '","'
         @495  COLLINDC        $1. @496   '","' /*COLL INDC  Y/N  */
         @499  COLLNO         $11. @510   '","'
      /* @513  COLLCLASS      $03. */     @516   '","'
      /* @519  AANUMBER       $20. */     @539   '","'
      /* @542  ARREARDAY       Z5. */     @547   '","'
         @550  JOINTACC        $1. @551   '","'
         @554  PRODDESC       $40.        @594   '", '
         @597  '\N'
         @605  DOBDOR         $8.;
  RUN;
//*--------------------------------------------------------------------
//*--> SPLIT FROM ONE FILE INTO A FEW FILES
//*--------------------------------------------------------------------
//STEP0001 EXEC PGM=SORT
//SORTIN   DD DISP=SHR,DSN=SNGLVIEW.PBCS
//OUT01    DD DSN=SNGLVIEW.PBCS01,
//            DISP=(NEW,CATLG,DELETE),
//            UNIT=SYSDA,SPACE=(CYL,(500,200),RLSE),
//            DCB=(LRECL=1000,BLKSIZE=0,RECFM=FB)
//OUT02    DD DSN=SNGLVIEW.PBCS02,
//            DISP=(NEW,CATLG,DELETE),
//            UNIT=SYSDA,SPACE=(CYL,(500,200),RLSE),
//            DCB=(LRECL=1000,BLKSIZE=0,RECFM=FB)
//OUT03    DD DSN=SNGLVIEW.PBCS03,
//            DISP=(NEW,CATLG,DELETE),
//            UNIT=SYSDA,SPACE=(CYL,(500,200),RLSE),
//            DCB=(LRECL=1000,BLKSIZE=0,RECFM=FB)
//OUT04    DD DSN=SNGLVIEW.PBCS04,
//            DISP=(NEW,CATLG,DELETE),
//            UNIT=SYSDA,SPACE=(CYL,(500,200),RLSE),
//            DCB=(LRECL=1000,BLKSIZE=0,RECFM=FB)
//OUT05    DD DSN=SNGLVIEW.PBCS05,
//            DISP=(NEW,CATLG,DELETE),
//            UNIT=SYSDA,SPACE=(CYL,(500,200),RLSE),
//            DCB=(LRECL=1000,BLKSIZE=0,RECFM=FB)
//OUT06    DD DSN=SNGLVIEW.PBCS06,
//            DISP=(NEW,CATLG,DELETE),
//            UNIT=SYSDA,SPACE=(CYL,(500,200),RLSE),
//            DCB=(LRECL=1000,BLKSIZE=0,RECFM=FB)
//OUT07    DD DSN=SNGLVIEW.PBCS07,
//            DISP=(NEW,CATLG,DELETE),
//            UNIT=SYSDA,SPACE=(CYL,(500,200),RLSE),
//            DCB=(LRECL=1000,BLKSIZE=0,RECFM=FB)
//OUT08    DD DSN=SNGLVIEW.PBCS08,
//            DISP=(NEW,CATLG,DELETE),
//            UNIT=SYSDA,SPACE=(CYL,(500,200),RLSE),
//            DCB=(LRECL=1000,BLKSIZE=0,RECFM=FB)
//OUT09    DD DSN=SNGLVIEW.PBCS09,
//            DISP=(NEW,CATLG,DELETE),
//            UNIT=SYSDA,SPACE=(CYL,(500,200),RLSE),
//            DCB=(LRECL=1000,BLKSIZE=0,RECFM=FB)
//OUT10    DD DSN=SNGLVIEW.PBCS10,
//            DISP=(NEW,CATLG,DELETE),
//            UNIT=SYSDA,SPACE=(CYL,(500,200),RLSE),
//            DCB=(LRECL=1000,BLKSIZE=0,RECFM=FB)
//SYSOUT   DD SYSOUT=*
//SYSIN    DD *
  OPTION COPY
  OUTFIL FNAMES=(OUT01,OUT02,OUT03,OUT04,OUT05,
                 OUT06,OUT07,OUT08,OUT09,OUT10),SPLIT
/*
//*
