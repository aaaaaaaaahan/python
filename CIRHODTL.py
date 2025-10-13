//CIRHODTL JOB MSGCLASS=X,MSGLEVEL=(1,1),REGION=64M,NOTIFY=&SYSUID      JOB53840
//*---------------------------------------------------------------------
//*- ESMR 2013-2135  RHOLD LIST FOR PBB(L) LTD
//*- ESMR 2012-1898  RHOLD LIST FOR PIVB-BOS
//*- ESMR 2012-368   ONETIME UPDATE FOR CUNEG       (CIRHUNI1)
//*-                 DAILY ADD/DEL FILE FOR CUNEG   (CIRHUNIA/CIRHUNID)
//*- ESMR 2013-37    RHOLD LIST FOR LABUAN TAS (LABUAN TAB ESMR)
//*- ESMR 2014-978   INCL CLASSCODE 4 NATURE 28 IN DETICA
//*-                  CDD, ODD AND MONITORING LIST
//*- ESMR 2014-1737  EXCL CLASSCODE 4 (NATURE 28 AND 44)
//*- ESMR 2014-1954  RHOLD LIST FOR PBB(L) LTD (SEPARATE FILE FR PIVB)
//*- ESMR 2018-2571  PIVB - INCLUDE  CLS0000004 (NAT 0000044)
//*-                 PB(L)- INCLUDE  CLS0000004 (NAT 0000044)
//*-      (1ST SETUP)PMB  - EXCLUDE  CLS0000004 (NAT 0000028)
//*- ESMR 2018-3412  RHOLD LIST FOR CTOS IDGUARD PROJECT (3 FIELDS ONLY)
//*- ESMR 2018-3716  RHOLD LIST FOR CTOS IDGUARD PROJECT (WITH SPECS)
//*- EJS A2024-25382 TEMP FIX - SPECIAL CHARACTER IN LABUAN OUTPUT FILE
//*---------------------------------------------------------------------
//DELETE   EXEC PGM=IEFBR14
//DLT1     DD DSN=RBP2.B033.RHOLD.FULL.LIST,
//            DISP=(MOD,DELETE,DELETE),SPACE=(TRK,(0))
//DLT2     DD DSN=RBP2.B033.RHOLD.FULL.LIST.PIVB,
//            DISP=(MOD,DELETE,DELETE),SPACE=(TRK,(0))
//DLT3     DD DSN=RBP2.B033.RHOLD.FULL.LIST.LABUAN,
//            DISP=(MOD,DELETE,DELETE),SPACE=(TRK,(0))
//DLT4     DD DSN=RBP2.B033.RHOLD.FULL.LIST.PMB,
//            DISP=(MOD,DELETE,DELETE),SPACE=(TRK,(0))
//DLT5     DD DSN=RBP2.B033.RHOLD.FULL.LIST.CTOS,
//            DISP=(MOD,DELETE,DELETE),SPACE=(TRK,(0))
//*---------------------------------------------------------------------
//* MERGING FULL LISTING FOR RHOLD DATABASE
//*---------------------------------------------------------------------
//RHOLDLST EXEC SAS609
//IEFRDER   DD DUMMY
//SORTWK01  DD UNIT=SYSDA,SPACE=(CYL,(200,150))
//SORTWK02  DD UNIT=SYSDA,SPACE=(CYL,(200,150))
//SORTWK03  DD UNIT=SYSDA,SPACE=(CYL,(200,150))
//SORTWK04  DD UNIT=SYSDA,SPACE=(CYL,(200,150))
//CIRHOBCT  DD DISP=SHR,DSN=RBP2.B033.UNLOAD.CIRHOBCT.FB
//CIRHODCT  DD DISP=SHR,DSN=RBP2.B033.UNLOAD.CIRHODCT.FB
//CIRHOLDT  DD DISP=SHR,DSN=RBP2.B033.UNLOAD.CIRHOLDT.FB
//OUTFILE   DD DSN=RBP2.B033.RHOLD.FULL.LIST,
//             DISP=(NEW,CATLG,DELETE),
//             UNIT=SYSDA,SPACE=(CYL,(300,300),RLSE),
//             DCB=(LRECL=1500,BLKSIZE=0,RECFM=FB)
//PIVBFILE  DD DSN=RBP2.B033.RHOLD.FULL.LIST.PIVB,
//             DISP=(NEW,CATLG,DELETE),
//             UNIT=SYSDA,SPACE=(CYL,(300,300),RLSE),
//             DCB=(LRECL=1500,BLKSIZE=0,RECFM=FB)
//LABUANFL  DD DSN=RBP2.B033.RHOLD.FULL.LIST.LABUAN,
//             DISP=(NEW,CATLG,DELETE),
//             UNIT=SYSDA,SPACE=(CYL,(300,300),RLSE),
//             DCB=(LRECL=1500,BLKSIZE=0,RECFM=FB)
//PMBFILE   DD DSN=RBP2.B033.RHOLD.FULL.LIST.PMB,
//             DISP=(NEW,CATLG,DELETE),
//             UNIT=SYSDA,SPACE=(CYL,(300,300),RLSE),
//             DCB=(LRECL=1500,BLKSIZE=0,RECFM=FB)
//CTOSFILE  DD DSN=RBP2.B033.RHOLD.FULL.LIST.CTOS,
//             DISP=(NEW,CATLG,DELETE),
//             UNIT=SYSDA,SPACE=(CYL,(300,300),RLSE),
//             DCB=(LRECL=1500,BLKSIZE=0,RECFM=FB)
//SASLIST   DD SYSOUT=X
//SYSIN     DD *
 /*----------------------------------------------------------------*/
 /*    DESCRIPTION                                                 */
 /*----------------------------------------------------------------*/
   DATA DESCRIBE CLASS DEPT NATURE;
     INFILE CIRHODCT;
         INPUT @001 KEY_ID                  $10.
               @011 KEY_CODE                $10.
               @021 KEY_DESCRIBE            $150.
               @171 KEY_REMARK_ID1          $10.
               @181 KEY_REMARK_1            $50.
               @231 KEY_REMARK_ID2          $10.
               @241 KEY_REMARK_2            $50.
               @291 KEY_REMARK_ID3          $10.
               @301 KEY_REMARK_3            $50.
               @351 DESC_LASTOPERATOR       $8.
               @359 DESC_LASTMNT_DATE       $10.
               @369 DESC_LASTMNT_TIME       $8.  ;
         IF KEY_ID = 'CLASS ' THEN OUTPUT CLASS ;
         IF KEY_ID = 'DEPT  ' THEN OUTPUT DEPT  ;
         IF KEY_ID = 'NATURE' THEN OUTPUT NATURE;
   RUN;

 DATA CLASS (INDEX=(CLASS_CODE/UNIQUE/NOMISS));
 SET CLASS;
      KEEP CLASS_CODE CLASS_DESC;
      CLASS_CODE=KEY_CODE;
      CLASS_DESC=KEY_DESCRIBE;
 RUN;

 DATA NATURE (INDEX=(NATURE_CODE/UNIQUE/NOMISS));
 SET NATURE;
      KEEP NATURE_CODE NATURE_DESC;
      NATURE_CODE=KEY_CODE;
      NATURE_DESC=KEY_DESCRIBE;
 RUN;

 DATA DEPT   (INDEX=(DEPT_CODE/UNIQUE/NOMISS));
 SET DEPT;
      DEPT_CODE=KEY_CODE;
      DEPT_DESC=KEY_DESCRIBE;
      IF KEY_REMARK_ID1 = 'CONTACT1' AND KEY_REMARK_1 NE ''
         THEN CONTACT1=KEY_REMARK_1;
      IF KEY_REMARK_ID2 = 'CONTACT2' AND KEY_REMARK_2 NE ''
         THEN CONTACT2=KEY_REMARK_2;
      IF KEY_REMARK_ID3 = 'CONTACT3' AND KEY_REMARK_3 NE ''
         THEN CONTACT3=KEY_REMARK_3;
 RUN;
 /*----------------------------------------------------------------*/
 /*    CONTROL LIST                                                */
 /*----------------------------------------------------------------*/
   DATA CONTROL;
     INFILE CIRHOBCT;
         INPUT @01  CLASS_CODE             $10.
               @11  NATURE_CODE            $10.
               @21  DEPT_CODE              $10.
               @31  GUIDE_CODE             $10.
               @41  CLASS_ID               $10.
               @51  CTRL_OPERATOR           $8.
               @59  CTRL_LASTMNT_DATE      $10.
               @69  CTRL_LASTMNT_TIME       $8.  ;
   RUN;
 PROC SORT  DATA=CONTROL; BY CLASS_ID ; RUN;
 PROC PRINT DATA=CONTROL(OBS=10); TITLE 'CONTROL' ; RUN;
 /*----------------------------------------------------------------*/
 /*    DETAIL LISTING                                              */
 /*----------------------------------------------------------------*/
   DATA DETAIL;
     INFILE CIRHOLDT;
         INPUT @001 CLASS_ID              $10.
               @011 INDORG                $01.
               @012 NAME                  $40.
               @052 ID1                   $20.
               @072 ID2                   $20.
               @092 DTL_REMARK1           $40.
               @132 DTL_REMARK2           $40.
               @172 DTL_REMARK3           $40.
               @212 DTL_REMARK4           $40.
               @252 DTL_REMARK5           $40.
               @292 DTL_CRT_DATE          $10.
               @302 DTL_CRT_TIME          $08.
               @310 DTL_LASTOPERATOR      $08.
               @318 DTL_LASTMNT_DATE      $10.
               @328 DTL_LASTMNT_TIME      $08.;
  RUN;
 PROC SORT DATA=DETAIL; BY CLASS_ID; RUN;
 PROC PRINT DATA=DETAIL(OBS=10); TITLE 'DETAIL'; RUN;
 /*------------------------------------------------------------------*/
 /*- MERGE DETAIL AND CONTROL                                       -*/
 /*------------------------------------------------------------------*/
 DATA FIRST;
     MERGE DETAIL(IN=A) CONTROL(IN=B);BY CLASS_ID;
         IF A;
 RUN;

 DATA CLASS_DESC;          /* CLASS DESCRIPTION */
      SET FIRST;
      SET CLASS     KEY=CLASS_CODE;
      IF _IORC_ THEN DO;
         _ERROR_ = 0;
         _IORC_ = 0;
         END;
 RUN;

 DATA NATURE_DESC;          /* NATURE DESCRIPTION */
      SET CLASS_DESC;
      SET NATURE    KEY=NATURE_CODE;
      IF _IORC_ THEN DO;
         _ERROR_ = 0;
         _IORC_ = 0;
         END;
 RUN;

 DATA DEPT_DESC;            /* DEPARTMENT DESCRIPTION */
      SET NATURE_DESC;
      SET DEPT      KEY=DEPT_CODE;
      IF _IORC_ THEN DO;
         _ERROR_ = 0;
         _IORC_ = 0;
         END;
 RUN;
 PROC SORT DATA=DEPT_DESC; BY CLASS_ID INDORG NAME; RUN;

 /*-------------------------------------------------------*/
 /*- FULL FILE DETAILS                                   -*/
 /*-------------------------------------------------------*/
 DATA OUT;
 SET DEPT_DESC;
     FILE OUTFILE;
     PUT @001     CLASS_CODE             $10.
         @011     CLASS_DESC             $150.
         @161     NATURE_CODE            $10.
         @171     NATURE_DESC            $150.
         @321     DEPT_CODE              $10.
         @331     DEPT_DESC              $150.
         @481     GUIDE_CODE             $10.
         @491     CLASS_ID               $10.
         @501     INDORG                 $01.
         @505     NAME                   $40.
         @545     ID1                    $20.
         @565     ID2                    $20.
         @585     DTL_REMARK1            $40.
         @625     DTL_REMARK2            $40.
         @665     DTL_REMARK3            $40.
         @705     DTL_REMARK4            $40.
         @745     DTL_REMARK5            $40.
         @785     DTL_CRT_DATE           $10.
         @795     DTL_CRT_TIME           $08.
         @805     DTL_LASTOPERATOR       $08.
         @815     DTL_LASTMNT_DATE       $10.
         @825     DTL_LASTMNT_TIME       $08.
         @835     CONTACT1               $50.
         @885     CONTACT2               $50.
         @935     CONTACT3               $50.
         @985     ' '  ;
 RUN;

 /*-------------------------------------------------------*/
 /*- FILE TO BE SENT TO PIVB DAILY REFRESH               -*/
 /*-------------------------------------------------------*/
 DATA PIVB;
 SET DEPT_DESC;
     FILE PIVBFILE;
     /* ESMR 2014-1737                                 */
     /* EXCL CLASSCODE 4 (NATURE 28 AND 44)            */
     IF CLASS_CODE = 'CLS0000004' AND NATURE_CODE = 'NAT0000028'
        THEN DELETE;
   /* ESMR 2018-2571                                             */
   /* INCL CLASSCODE 4 (NATURE 44)                               */
   /*IF CLASS_CODE = 'CLS0000004' AND NATURE_CODE = 'NAT0000044' */
   /*   THEN DELETE;                                             */
     PUT @501     INDORG                 $01.
         @505     NAME                   $40.
         @545     ID1                    $20.
         @565     ID2                    $20.
         @835     CONTACT1               $50.
         @885     CONTACT2               $50.
         @935     CONTACT3               $50.
         @985     ' '  ;
 RUN;

 /*-------------------------------------------------------*/
 /*- FILE TO BE SENT TO PB(LABUAN)                       -*/
 /*- ESMR 2014-1954 SEPARATE FILE FR LABUAN               */
 /*-------------------------------------------------------*/
 DATA LABUAN;
 SET DEPT_DESC;
     FILE LABUANFL;
     IF CLASS_CODE = 'CLS0000004' AND NATURE_CODE = 'NAT0000028'
        THEN DELETE;
   /* ESMR 2018-2571                                             */
   /* INCL CLASSCODE 4 (NATURE 44)                               */
   /*IF CLASS_CODE = 'CLS0000004' AND NATURE_CODE = 'NAT0000044' */
   /*   THEN DELETE;                                             */
     NAME = TRANWRD(NAME   ,'05'X,' ');    /*EJS A2024-00025382  */
     PUT @001     CLASS_CODE             $10.
         @501     INDORG                 $01.
         @505     NAME                   $40.
         @545     ID1                    $20.
         @565     ID2                    $20.
         @835     CONTACT1               $50.
         @885     CONTACT2               $50.
         @935     CONTACT3               $50.
         @985     ' '  ;
 RUN;

 /*-------------------------------------------------------*/
 /*- ESMR 2018-2571 : FILE TO BE SENT TO PMB              */
 /*-------------------------------------------------------*/
 DATA PMB;
 SET DEPT_DESC;
     FILE PMBFILE;
     IF CLASS_CODE = 'CLS0000004' AND NATURE_CODE = 'NAT0000028'
        THEN DELETE;
     PUT @501     INDORG                 $01.
         @505     NAME                   $40.
         @545     ID1                    $20.
         @565     ID2                    $20.
         @835     CONTACT1               $50.
         @885     CONTACT2               $50.
         @935     CONTACT3               $50.
         @985     ' '  ;
 RUN;
 /*-------------------------------------------------------------*/
 /*- ESMR 2018-3412 : FILE TO BE SENT TO CTOS IDGUARD PROJECT   */
 /*-                                                            */
 /*- UAT                                                        */
 /*- CLASSID     CLASSCODE     NATURE         DEPT              */
 /*- 0000000005  CLS0000001    NAT0000002     HPCS              */
 /*- 0000000006  CLS0000001    NAT0000006     HPCS              */
 /*- 0000000009  CLS0000001    NAT0000002     HPF               */
 /*- 0000000010  CLS0000001    NAT0000006     HPF               */
 /*- 0000000014  CLS0000001    NAT0000004     HPCS              */
 /*- 0000000015  CLS0000001    NAT0000008     HPCS              */
 /*- 0000000019  CLS0000001    NAT0000004     HPF               */
 /*- 0000000021  CLS0000001    NAT0000008     HPF               */
 /*- 0000000025  CLS0000001    NAT0000005     HPCS              */
 /*- 0000000026  CLS0000001    NAT0000009     HPCS              */
 /*- 0000000031  CLS0000001    NAT0000005     HPF               */
 /*- 0000000032  CLS0000001    NAT0000009     HPF               */
 /*-                                                            */
 /*- PROD                                                       */
 /*- CLASSID     CLASSCODE     NATURE         DEPT              */
 /*- 0000000005  CLS0000001    NAT0000002     HPCS              */
 /*- 0000000006  CLS0000001    NAT0000002     HPF               */
 /*- 0000000009  CLS0000001    NAT0000004     HPCS              */
 /*- 0000000010  CLS0000001    NAT0000004     HPF               */
 /*- 0000000014  CLS0000001    NAT0000005     HPCS              */
 /*- 0000000015  CLS0000001    NAT0000005     HPF               */
 /*- 0000000019  CLS0000001    NAT0000006     HPCS              */
 /*- 0000000021  CLS0000001    NAT0000006     HPF               */
 /*- 0000000025  CLS0000001    NAT0000008     HPCS              */
 /*- 0000000026  CLS0000001    NAT0000008     HPF               */
 /*- 0000000031  CLS0000001    NAT0000009     HPCS              */
 /*- 0000000032  CLS0000001    NAT0000009     HPF               */
 /*-------------------------------------------------------------*/
 DATA CTOS;
 SET DEPT_DESC;
     FILE CTOSFILE;
     IF  CLASS_ID IN ('0000000005','0000000006','0000000009'
                     ,'0000000010','0000000014','0000000015'
                     ,'0000000019','0000000021','0000000025'
                     ,'0000000026','0000000031','0000000032' ) ;
   /*IF '2011-10-01' <  DTL_CRT_DATE < '2012-01-01';     UAT    */
     IF '2015-10-01' <  DTL_CRT_DATE < '2018-08-31';  /* PROD   */
     PUT @001     CLASS_CODE             $10.
         @011     CLASS_DESC             $150.
         @161     NATURE_CODE            $10.
         @171     NATURE_DESC            $150.
         @321     DEPT_CODE              $10.
         @331     DEPT_DESC              $150.
     /*  @481     GUIDE_CODE             $10.    */
         @491     CLASS_ID               $10.
         @501     INDORG                 $01.
         @505     NAME                   $40.
         @545     ID1                    $20.
         @565     ID2                    $20.
     /*  @585     DTL_REMARK1            $40.    */
     /*  @625     DTL_REMARK2            $40.    */
     /*  @665     DTL_REMARK3            $40.    */
     /*  @705     DTL_REMARK4            $40.    */
     /*  @745     DTL_REMARK5            $40.    */
         @785     DTL_CRT_DATE           $10.
     /*  @795     DTL_CRT_TIME           $08.    */
     /*  @805     DTL_LASTOPERATOR       $08.    */
         @815     DTL_LASTMNT_DATE       $10.
         @825     DTL_LASTMNT_TIME       $08.
     /*  @835     CONTACT1               $50.    */
     /*  @885     CONTACT2               $50.    */
     /*  @935     CONTACT3               $50.    */
         @985     ' '  ;
 RUN;
