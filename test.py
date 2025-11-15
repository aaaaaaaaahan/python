convert program to python with duckdb and pyarrow
duckdb for process input file and output parquet&txt
assumed all the input file ady convert to parquet can directly use it

//CICISRPD JOB MSGCLASS=X,MSGLEVEL=(1,1),REGION=64M,NOTIFY=&SYSUID      JOB59501
//*---------------------------------------------------------------------
//DELETE   EXEC PGM=IEFBR14
//DEL1     DD DSN=CIS.IDIC.DAILY.RPT.OUT,
//            DISP=(MOD,DELETE,DELETE),SPACE=(TRK,(0))
//DEL2     DD DSN=CIS.IDIC.DAILY.RPT,
//            DISP=(MOD,DELETE,DELETE),SPACE=(TRK,(0))
//*---------------------------------------------------------------------
//* NODUPS (GET ALL RECORD WITH CHANGES/NEW COMPARE ALL FIELDS)
//*---------------------------------------------------------------------
//GETCHG   EXEC SAS609
//SORTWK01 DD UNIT=SYSDA,SPACE=(CYL,(1000,500))
//SORTWK02 DD UNIT=SYSDA,SPACE=(CYL,(1000,500))
//SORTWK03 DD UNIT=SYSDA,SPACE=(CYL,(1000,500))
//NEWCHG   DD DISP=SHR,DSN=CIS.IDIC.DAILY.INEW
//OLDCHG   DD DISP=SHR,DSN=CIS.IDIC.DAILY.IOLD
//*CRSBANK  DD DISP=SHR,DSN=CIS.CUST.DAILY.ACTIVE
//CCRSBANK  DD DISP=SHR,DSN=CIS.CUST.DAILY.ACTVOD
//CTRLDATE DD DISP=SHR,DSN=SRSCTRL1(0)
//OUTFILE  DD DSN=CIS.IDIC.DAILY.RPT.OUT,
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
  /*TO CHANGE STATUS*/
DATA NEWCHG;
  INFILE NEWCHG;
   INPUT @21   CUSTNO                 $20.
         @41   ADDREF                 $11./*  PD6.*/
         @52   CUSTNAME               $40.
         @92   PRIPHONE               $11./*PD6.*/
         @103  SECPHONE               $11./*PD6.*/
         @114  MOBILEPH               $11./*PD6.*/
         @125  FAX                    $11./*PD6.*/
         @136  ALIASKEY               $3.
         @139  ALIAS                  $20.
         @159  PROCESSTIME            $8.
         @167  CUSTSTAT               $1.
         @168  TAXCODE                $1.
         @169  TAXID                  $9.
         @178  CUSTBRCH               $5.
         @183  COSTCTR                $5. /*PD4.*/
         @188  CUSTMNTDATE            $08.
         @196  CUSTLASTOPER           $8.
         @204  PRIM_OFF               $5./*PD3.*/
         @209  SEC_OFF                $5./*PD3.*/
         @214  PRIM_LN_OFF            $5./*PD3.*/
         @219  SEC_LN_OFF             $5./*PD3.*/
         @224  RACE                   $1.
         @225  RESIDENCY              $3.
         @228  CITIZENSHIP            $2.
         @230  OPENDT                 $08. /*  PD6.*/
         @241  HRCALL                 $60.
         @301  EXPERIENCE             $3.
         @304  HOBBIES                $3.
         @307  RELIGION               $3.
         @310  LANGUAGE               $3.
         @313  INST_SEC               $3.
         @316  CUST_CODE              $3.
         @319  CUSTCONSENT            $3.
         @322  BASICGRPCODE           $3.
         @327  MSICCODE               $5.
         @332  MASCO2008              $5.
         @337  INCOME                 $3.
         @340  EDUCATION              $3.
         @343  OCCUP                  $3.
         @346  MARITALSTAT            $1.
         @347  OWNRENT                $1.
         @348  EMPNAME                $40.
         @388  DOBDOR                 $08.
         @396  SICCODE                $05.
         @401  CORPSTATUS             $3.
         @404  NETWORTH               $3.
         @407  LAST_UPDATE_DATE       $10.
         @417  LAST_UPDATE_TIME       $10.
         @427  LAST_UPDATE_OPER       $10.
         @437  PRCOUNTRY              $02.
         @439  EMPLOYMENT_TYPE        $10.
         @449  EMPLOYMENT_SECTOR      $10.
         @459  EMPLOYMENT_LAST_UPDATE $10.
         @469  BNMID                  $20.
         @489  LONGNAME               $150.
         @639  INDORG                 $1.
         @640  RESDESC                $20.
         @660  SALDESC                $20.
         @680  CTZDESC                $20.;
PROC SORT  DATA=NEWCHG; BY CUSTNO;
PROC PRINT DATA=NEWCHG(OBS=5);TITLE 'NEW CHG';
RUN;

DATA OLDCHG;
  INFILE OLDCHG;
   INPUT @21   CUSTNO                 $20.
         @41   ADDREFX                $11.
         @52   CUSTNAMEX              $40.
         @92   PRIPHONEX              $11.
         @103  SECPHONEX              $11.
         @114  MOBILEPHX              $11.
         @125  FAXX                   $11.
         @136  ALIASKEYX              $3.
         @139  ALIASX                 $20.
         @159  PROCESSTIMEX           $8.
         @167  CUSTSTATX              $1.
         @168  TAXCODEX               $1.
         @169  TAXIDX                 $9.
         @178  CUSTBRCHX              $5.
         @183  COSTCTRX               $5. /*PD4.*/
         @188  CUSTMNTDATEX           $08.
         @196  CUSTLASTOPERX          $8.
         @204  PRIM_OFFX              $5./*PD3.*/
         @209  SEC_OFFX               $5./*PD3.*/
         @214  PRIM_LN_OFFX           $5./*PD3.*/
         @219  SEC_LN_OFFX            $5./*PD3.*/
         @224  RACEX                  $1.
         @225  RESIDENCYX             $3.
         @228  CITIZENSHIPX           $2.
         @230  OPENDTX                $08. /*  PD6.*/
         @241  HRCALLX                $60.
         @301  EXPERIENCEX            $3.
         @304  HOBBIESX               $3.
         @307  RELIGIONX              $3.
         @310  LANGUAGEX              $3.
         @313  INST_SECX              $3.
         @316  CUST_CODEX             $3.
         @319  CUSTCONSENTX           $3.
         @322  BASICGRPCODEX          $3.
         @327  MSICCODEX              $5.
         @332  MASCO2008X             $5.
         @337  INCOMEX                $3.
         @340  EDUCATIONX             $3.
         @343  OCCUPX                 $3.
         @346  MARITALSTATX           $1.
         @347  OWNRENTX               $1.
         @348  EMPNAMEX               $40.
         @388  DOBDORX                $08.
         @396  SICCODEX               $05.
         @401  CORPSTATUSX            $3.
         @404  NETWORTHX              $3.
         @407  LAST_UPDATE_DATEX      $10.
         @417  LAST_UPDATE_TIMEX      $10.
         @427  LAST_UPDATE_OPERX      $10.
         @437  PRCOUNTRYX             $02.
         @439  EMPLOYMENT_TYPEX       $10.
         @449  EMPLOYMENT_SECTORX     $10.
         @459  EMPLOYMENT_LAST_UPDATX $10.
         @469  BNMIDX                 $20.
         @489  LONGNAMEX              $150.
         @639  INDORG                 $1.
         @640  RESDESCX               $20.
         @660  SALDESCX               $20.
         @680  CTZDESCX               $20.;
PROC SORT  DATA=OLDCHG; BY CUSTNO;
PROC PRINT DATA=OLDCHG(OBS=5);TITLE 'OLD CHG';
RUN;

DATA ACTIVE;
  INFILE CCRSBANK;
   INPUT  @001   CUSTNO          $20.
          @021   ACCTCODE        $5.
          @026   ACCTNOC         $20.
          @047   NOTENOC         $5.
          @052   BANKINDC        $1.
          @055   DATEOPEN        $10.
          @065   DATECLSE        $10.
          @075   ACCTSTATUS      $1.;
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

   DATA NEWACT;
        MERGE NEWCHG(IN=F) LISTACT(IN=G);
        BY CUSTNO;
        IF F AND G;
   RUN;
   PROC SORT  DATA=NEWACT;BY CUSTNO;RUN;
   PROC PRINT DATA=NEWACT(OBS=10);TITLE 'NEWACT';RUN;

   DATA MERGE_A;
        MERGE NEWACT(IN=A) OLDCHG(IN=B);
        BY CUSTNO;
        IF A AND B;
   RUN;
   PROC SORT  DATA=MERGE_A;BY CUSTNO;RUN;
   PROC PRINT DATA=MERGE_A(OBS=15);TITLE 'COM CIS';RUN;

   DATA C_PRCTRY C_EMNAME C_EMTYP C_EMSEC C_MASCO
        C_CTZN C_CCODE C_MSIC C_CORP C_BGC C_DOB C_LONG
        C_NAME  C_ADDREF C_DATE C_OPER C_RESD;
     KEEP CUSTNO UPDDATE UPDOPER FIELDS OLDVALUE NEWVALUE ACCTNOC
          DATEUPD DATEOPER CUSTNAME CUSTLASTOPER CUSTMNTDATE;
     FORMAT FIELDS $30. OLDVALUE $150. NEWVALUE $150.;
     SET MERGE_A;
     IF CUSTMNTDATE NOT = CUSTMNTDATEX THEN DO;
        UPDDATE = CUSTMNTDATE;
        OUTPUT C_DATE;
     END;
     IF CUSTLASTOPER NOT = CUSTLASTOPERX THEN DO;
        UPDOPER = CUSTLASTOPER;
        OUTPUT C_OPER;
     END;
     IF ADDREF NOT = ADDREFX THEN DO;
        FIELDS ='ADDREF';
        OLDVALUE = ADDREFX;
        NEWVALUE = ADDREF;
        OUTPUT C_ADDREF;
     END;
     IF CUSTNAME NOT = CUSTNAMEX THEN DO;
        FIELDS ='NAME';
        OLDVALUE = CUSTNAMEX;
        NEWVALUE = CUSTNAME;
        OUTPUT C_NAME;
     END;
     IF LONGNAME NOT = LONGNAMEX THEN DO;
        FIELDS ='CUSTOMER NAME';
        OLDVALUE = LONGNAMEX;
        NEWVALUE = LONGNAME;
        OUTPUT C_LONG;
     END;
     IF DOBDOR  NOT = DOBDORX   THEN DO;
        OLDVALUE = DOBDORX;
        NEWVALUE = DOBDOR;
        IF INDORG = 'I' THEN FIELDS ='DATE OF BIRTH';
        ELSE FIELDS ='DATE OF REGISTRATION';
        OUTPUT C_DOB;
     END;
     IF BASICGRPCODE NOT = BASICGRPCODEX  THEN DO;
        FIELDS ='ENTITY TYPE';
        OLDVALUE = BASICGRPCODEX;
        NEWVALUE = BASICGRPCODE;
        OUTPUT C_BGC;
     END;
     IF CORPSTATUS NOT = CORPSTATUSX  THEN DO;
        FIELDS ='CORPORATE STATUS';
        OLDVALUE = CORPSTATUSX;
        NEWVALUE = CORPSTATUS;
        OUTPUT C_CORP;
     END;
     IF MSICCODE NOT = MSICCODEX  THEN DO;
        FIELDS ='MSIC 2008';
        OLDVALUE = MSICCODEX;
        NEWVALUE = MSICCODE;
        OUTPUT C_MSIC;
     END;
     IF CUST_CODE NOT = CUST_CODEX  THEN DO;
        FIELDS ='CUSTOMER CODE';
        OLDVALUE = CUST_CODEX;
        NEWVALUE = CUST_CODE;
        OUTPUT C_CCODE;
     END;
     IF CITIZENSHIP NOT = CITIZENSHIPX THEN DO;
        FIELDS ='NATIONALITY';
        OLDVALUE = CITIZENSHIPX;
        NEWVALUE = CITIZENSHIP;
        OUTPUT C_CTZN;
     END;
     IF MASCO2008 NOT = MASCO2008X THEN DO;
        FIELDS ='MASCO OCCUPATION';
        OLDVALUE = MASCO2008X;
        NEWVALUE = MASCO2008;
        OUTPUT C_MASCO;
     END;
     IF EMPLOYMENT_SECTOR NOT = EMPLOYMENT_SECTORX THEN DO;
        /*CHK OPER AND DATE*/
        IF EMPLOYMENT_LAST_UPDATE NOT = EMPLOYMENT_LAST_UPDATEX
        THEN DATEUPD  = EMPLOYMENT_LAST_UPDATE;
        IF LAST_UPDATE_OPER NOT = LAST_UPDATE_OPERX
        THEN DATEOPER = LAST_UPDATE_OPER;
        FIELDS ='EMPLOYMENT SECTOR';
        OLDVALUE = EMPLOYMENT_SECTORX;
        NEWVALUE = EMPLOYMENT_SECTOR;
        OUTPUT C_EMSEC;
     END;
     IF EMPLOYMENT_TYPE NOT = EMPLOYMENT_TYPEX THEN DO;
        /*CHK OPER AND DATE*/
        IF EMPLOYMENT_LAST_UPDATE NOT = EMPLOYMENT_LAST_UPDATEX
        THEN DATEUPD = EMPLOYMENT_LAST_UPDATE;
        IF LAST_UPDATE_OPER NOT = LAST_UPDATE_OPERX
        THEN DATEOPER= LAST_UPDATE_OPER;

        FIELDS ='EMPLOYMENT TYPE';
        OLDVALUE = EMPLOYMENT_TYPEX;
        NEWVALUE = EMPLOYMENT_TYPE;
        OUTPUT C_EMTYP;
     END;
     IF EMPNAME NOT = EMPNAMEX THEN DO;
        FIELDS ='EMPLOYER NAME';
        OLDVALUE = EMPNAMEX;
        NEWVALUE = EMPNAME;
        OUTPUT C_EMNAME;
     END;
     IF PRCOUNTRY NOT = PRCOUNTRYX THEN DO;
        FIELDS ='PR COUNTRY';
        OLDVALUE = PRCOUNTRYX;
        NEWVALUE = PRCOUNTRY;
        OUTPUT C_PRCTRY;
     END;
     IF RESIDENCY NOT = RESIDENCYX THEN DO;
        FIELDS ='RESIDENCY';
        OLDVALUE = RESIDENCYX;
        NEWVALUE = RESIDENCY;
        OUTPUT C_RESD;
     END;
   RUN;
   PROC SORT  DATA=C_DATE;BY CUSTNO;RUN;
   PROC SORT  DATA=C_OPER;BY CUSTNO;RUN;
   PROC PRINT DATA=C_LONG(OBS=10);TITLE 'C_LONG';
   PROC PRINT DATA=C_DOB (OBS=10);TITLE 'C_DOB ';
   PROC PRINT DATA=C_BGC (OBS=10);TITLE 'C_BGC ';
   PROC PRINT DATA=C_CORP(OBS=10);TITLE 'C_CORP';
   PROC PRINT DATA=C_MSIC(OBS=10);TITLE 'C_MSIC';
   PROC PRINT DATA=C_CCODE(OBS=10);TITLE 'C_CCODE';
   PROC PRINT DATA=C_CTZN (OBS=10);TITLE 'C_CTZN ';
   PROC PRINT DATA=C_MASCO(OBS=10);TITLE 'C_MASCO';
   PROC PRINT DATA=C_EMNAME(OBS=10);TITLE 'C_EMNAME';
   PROC PRINT DATA=C_PRCTRY(OBS=10);TITLE 'C_PRCTRY';
   PROC PRINT DATA=C_EMSEC (OBS=10);TITLE 'C_EMSEC';

   DATA TEMPALL;
     SET C_LONG C_DOB C_BGC C_CORP C_MSIC C_CCODE C_CTZN C_MASCO
         C_EMNAME C_PRCTRY C_EMSEC C_RESD C_EMTYP;
   RUN;
   PROC SORT  DATA=TEMPALL;BY CUSTNO;RUN;
   PROC PRINT DATA=TEMPALL(OBS=15);TITLE 'ALL TEMP';RUN;

   DATA MRGCIS;
        MERGE C_DATE(IN=D) C_OPER(IN=E) TEMPALL(IN=C);
        BY CUSTNO;
        IF C;
        IF DATEUPD NOT = ' ' THEN UPDDATE = DATEUPD;
        IF OPERUPD NOT = ' ' THEN UPDOPER = OPERUPD;
        IF UPDOPER = ' ' THEN UPDOPER =  CUSTLASTOPER;
        IF UPDDATE = ' ' THEN UPDDATE =  CUSTMNTDATE ;
        IF UPDOPER IN ('ELNBATCH','AMLBATCH','HRCBATCH','CTRBATCH',
           'CIFLPRCE','CISUPDEC','CIUPDMSX','CIUPDMS9','MAPLOANS',
           'CRIS') THEN DELETE;
   RUN;
   PROC SORT  DATA=MRGCIS;BY CUSTNO;RUN;
   PROC PRINT DATA=MRGCIS(OBS=30);TITLE 'RESULT CUSTNO';RUN;

   DATA RECORDS;
     SET MRGCIS;
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
