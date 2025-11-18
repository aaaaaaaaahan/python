//CIHRCDP1 JOB MSGCLASS=X,MSGLEVEL=(1,1),REGION=8M,NOTIFY=&SYSUID       JOB65417
//*---------------------------------------------------------------------
//* ESMR 2010-3600 LIST OF HIGH RISK CUSTOMERS
//* PICK RELATED DP ACCOUNTS(EXCLUDE ZERO BALANCE AND CLOSED STATUS)
//* (1) SA AND CA(MYR/FCY) TO EXCLUDE CLOSED ACCT STATUS ONLY
//* (2) OTHER DP ACCTS TO EXCLUDE ZERO BALANCE AND CLOSED ACCT STATUS
//* SPLIT BY COST CENTRE TO DIFFERENTIATE PBB AND PIBB ACCOUNTS
//*---------------------------------------------------------------------
//DELETE   EXEC PGM=IEFBR14
//DELE1    DD DSN=RBP2.B033.CIS.HRCCUST.DPACCTS.GOOD,
//            DISP=(MOD,DELETE,DELETE),SPACE=(TRK,(0))
//DELE2    DD DSN=RBP2.B033.CIS.HRCCUST.DPACCTS.CLOSED,
//            DISP=(MOD,DELETE,DELETE),SPACE=(TRK,(0))
//DELE3    DD DSN=RBP2.B033.CIS.HRCCUST.DPACCTS.GOOD.PBB,
//            DISP=(MOD,DELETE,DELETE),SPACE=(TRK,(0))
//DELE4    DD DSN=RBP2.B033.CIS.HRCCUST.DPACCTS.GOOD.PIBB,
//            DISP=(MOD,DELETE,DELETE),SPACE=(TRK,(0))
//*---------------------------------------------------------------------
//* PROCESSES STARTS HERE
//*---------------------------------------------------------------------
//ALLACCT  EXEC SAS609,REGION=4M,WORK='50000,50000'
//IEFRDER   DD DUMMY
//*OUTPUT FROM JOB CIHRCALL
//HRCSTDP   DD DISP=SHR,DSN=RBP2.B033.CIS.HRCCUST.DPACCTS
//*OUTPUT FROM JOB DPEX2000/2001
//DPFILE    DD DISP=SHR,DSN=RBP2.B033.DPTRBLGS
//OUTDPGOD  DD DSN=RBP2.B033.CIS.HRCCUST.DPACCTS.GOOD,
//             DISP=(NEW,CATLG,DELETE),
//             UNIT=SYSDA,SPACE=(CYL,(10,10),RLSE),
//             DCB=(LRECL=250,BLKSIZE=0,RECFM=FB)
//OUTDPBAD  DD DSN=RBP2.B033.CIS.HRCCUST.DPACCTS.CLOSED,
//             DISP=(NEW,CATLG,DELETE),
//             UNIT=SYSDA,SPACE=(CYL,(10,10),RLSE),
//             DCB=(LRECL=250,BLKSIZE=0,RECFM=FB)
//SASLIST   DD SYSOUT=X
//SORTWK01  DD UNIT=SYSDA,SPACE=(CYL,800)
//SORTWK02  DD UNIT=SYSDA,SPACE=(CYL,800)
//SORTWK03  DD UNIT=SYSDA,SPACE=(CYL,800)
//SORTWK04  DD UNIT=SYSDA,SPACE=(CYL,800)
//SORTWK05  DD UNIT=SYSDA,SPACE=(CYL,800)
//SORTWK06  DD UNIT=SYSDA,SPACE=(CYL,800)
//SORTWK07  DD UNIT=SYSDA,SPACE=(CYL,800)
//SORTWK08  DD UNIT=SYSDA,SPACE=(CYL,800)
//SYSIN     DD *
OPTIONS IMSDEBUG=N YEARCUTOFF=1950 SORTDEV=3390 ERRORS=0;
OPTIONS NODATE NONUMBER NOCENTER;
TITLE;
 /*----------------------------------------------------------------*/
 /*    HIGH RISK CUSTOMERS WITH DP ACCT DECLARATION                */
 /*----------------------------------------------------------------*/
  DATA CISDP;
      INFILE HRCSTDP;
      INPUT  @01   BANKNUM            $3.
             @04   CUSTBRCH           5.
             @09   CUSTNO             $11.
             @20   CUSTNAME           $40.
             @60   RACE               $1.
             @61   CITIZENSHIP        $2.
             @63   INDORG             $1.
             @64   PRIMSEC            $1.
             @65   CUSTLASTDATECC     $2.
             @67   CUSTLASTDATEYY     $2.
             @69   CUSTLASTDATEMM     $2.
             @71   CUSTLASTDATEDD     $2.
             @73   ALIASKEY           $3.
             @76   ALIAS              $20.
             @96   HRCCODES           $60.
             @156  ACCTCODE           $5.
             @161  ACCTNO             20.;
  RUN;
  PROC SORT DATA=CISDP; BY ACCTNO; RUN;
 /*----------------------------------------------------------------*/
 /*    DEPOSIT TRIAL BALANCE FILE DECLARATION                      */
 /*    GET PBB AND PIBB ACCOUNT THEN ONLY SPLIT BY COST CENTRE     */
 /*    GET ALL ACTIVE ACCOUNTS(DP,SA,CA,VOSTRO,NOSTRO,FCY)         */
 /*    EXCLUDE ZERO BALANCE ACCTS AND PURGED/CLOSED ACCTS          */
 /*----------------------------------------------------------------*/
  DATA DPDATA;
      INFILE DPFILE;
      FORMAT TMPDATE 8. OPDATE 8. CLDATE 8. OYYYY 4. OMM Z2. ODD Z2.
             TMPACCT $10.;
      INPUT @03  BANKNO       PD2.
            @24  REPTNO       PD3.
            @27  FMTCODE      PD2. @;
      IF (REPTNO = 1001 AND
         (FMTCODE IN (1,5,10,11,19,20,21,22))) THEN DO;
         INPUT @106 BRANCH    PD4.
               @110 ACCTNO    PD6.
               @158 CLSDATE   PD6.
               @164 OPENDATE  PD6.
               @319 LEDBAL    PD7.
               @716 ACCSTAT   $1.
               @830 COSTCTR   PD4.;
         TMPACCT = PUT(ACCTNO,Z10.);
         CLDATE  = TRIM(SUBSTR(CLSDATE,1,9));
         IF OPENDATE NE 0 AND BRANCH NE 0 THEN DO;
            OPDATE  = TRIM(SUBSTR(OPENDATE,1,9));
            OMM     = SUBSTR(PUT(OPDATE,Z8.),1,2);
            ODD     = SUBSTR(PUT(OPDATE,Z8.),3,2);
            OYYYY   = SUBSTR(PUT(OPDATE,Z8.),5,4);
            TMPDATE = PUT(OYYYY,Z4.) || PUT(OMM,Z2.) || PUT(ODD,Z2.) ;
            OPDATE  = PUT(TMPDATE,8.);
         END;
         ELSE DO;
            OPDATE  = 0;
         END;
         OUTPUT;
      END;
  RUN;

  /****IF PRODUCTION CAN USE SAS 9.1 ABOVE VERSION, DUPOUT*****/
  /*PROC SORT DATA=DPDATA NODUPKEY DUPOUT=DPDUPS; BY ACCTNO; RUN;*/
  PROC SORT DATA=DPDATA NODUPKEY; BY ACCTNO; RUN;
  PROC PRINT DATA=DPDATA(OBS=10);TITLE 'DEPOSIT ACCOUNT DETAILS ';
  RUN;

  DATA GOODDP BADDP;
      MERGE DPDATA(IN=A) CISDP(IN=B); BY ACCTNO;
      IF A AND B THEN DO;
         IF SUBSTR(TMPACCT,1,1) IN (1,3) THEN DO;
           IF ACCSTAT NE 'C' AND ACCSTAT NE 'B' AND ACCSTAT NE 'P' AND
              ACCSTAT NE 'Z' THEN
               OUTPUT GOODDP;
           ELSE DO;
               OUTPUT BADDP;
           END;
         END;
         ELSE DO;
           IF (ACCSTAT NE 'C' AND ACCSTAT NE 'B' AND ACCSTAT NE 'P' AND
               ACCSTAT NE 'Z' ) OR LEDBAL NE 0  THEN
               OUTPUT GOODDP;
           ELSE DO;
               OUTPUT BADDP;
           END;
         END;
      END;
  RUN;

  PROC SORT DATA=GOODDP; BY CUSTNO ACCTNO; RUN;
  PROC SORT DATA=BADDP NODUPKEY; BY CUSTNO ACCTNO; RUN;

 /*----------------------------------------------------------------*/
 /*   OUTPUT GOOD AND BAD DP ACCTS DATASET FOR REPORTING PURPOSE   */
 /*----------------------------------------------------------------*/
  DATA TEMPOUT;
  SET GOODDP;
  FILE OUTDPGOD;
     PUT @01   BANKNUM            $3.
         @04   CUSTBRCH           Z5.
         @09   CUSTNO             $11.
         @20   CUSTNAME           $40.
         @60   RACE               $1.
         @61   CITIZENSHIP        $2.
         @63   INDORG             $1.
         @64   PRIMSEC            $1.
         @65   CUSTLASTDATECC     $2.
         @67   CUSTLASTDATEYY     $2.
         @69   CUSTLASTDATEMM     $2.
         @71   CUSTLASTDATEDD     $2.
         @73   ALIASKEY           $3.
         @76   ALIAS              $20.
         @96   HRCCODES           $60.
         @156  BRANCH             Z7.
         @163  ACCTCODE           $5.
         @168  ACCTNO             20.
         @188  OPDATE             8.
         @196  LEDBAL             Z13.
         @209  ACCSTAT            $1.
         @210  COSTCTR            Z4.;
  RETURN;
  RUN;
  DATA TEMPOUT1;
  SET BADDP;
  FILE OUTDPBAD;
     PUT @01   BANKNUM            $3.
         @04   CUSTBRCH           Z5.
         @09   CUSTNO             $11.
         @20   CUSTNAME           $40.
         @60   RACE               $1.
         @61   CITIZENSHIP        $2.
         @63   INDORG             $1.
         @64   PRIMSEC            $1.
         @65   CUSTLASTDATECC     $2.
         @67   CUSTLASTDATEYY     $2.
         @69   CUSTLASTDATEMM     $2.
         @71   CUSTLASTDATEDD     $2.
         @73   ALIASKEY           $3.
         @76   ALIAS              $20.
         @96   HRCCODES           $60.
         @156  BRANCH             Z7.
         @163  ACCTCODE           $5.
         @168  ACCTNO             20.
         @188  OPDATE             8.
         @196  LEDBAL             Z13.
         @209  ACCSTAT            $1.
         @210  COSTCTR            Z4.;
  RETURN;
  RUN;
//*--------------------------------------------------------------------
//* SORT FILE TO SEPARATE CONVENTIONAL AND ISLAMIC ACCOUNTS
//* FOR DEPOSIT ACCOUNTS ONLY
//*--------------------------------------------------------------------
//COVISLDP EXEC PGM=SORT                                                00170000
//SYSOUT   DD SYSOUT=*                                                  00170000
//SORTWK01 DD UNIT=SYSDA,SPACE=(CYL,(100,100))                          00170000
//SORTWK02 DD UNIT=SYSDA,SPACE=(CYL,(100,100))                          00170000
//SORTWK03 DD UNIT=SYSDA,SPACE=(CYL,(100,100))                          00170000
//SORTIN   DD DISP=SHR,DSN=RBP2.B033.CIS.HRCCUST.DPACCTS.GOOD
//DPCONV   DD DSN=RBP2.B033.CIS.HRCCUST.DPACCTS.GOOD.PBB,               00170000
//            DISP=(NEW,CATLG,DELETE),UNIT=SYSDA,
//            DCB=(LRECL=250,BLKSIZE=0,RECFM=FB),
//            SPACE=(CYL,(5,10),RLSE)
//DPPIBB   DD DSN=RBP2.B033.CIS.HRCCUST.DPACCTS.GOOD.PIBB,              00170000
//            DISP=(NEW,CATLG,DELETE),UNIT=SYSDA,
//            DCB=(LRECL=250,BLKSIZE=0,RECFM=FB),
//            SPACE=(CYL,(5,10),RLSE)
//SYSIN  DD *
 SORT FIELDS=COPY
 OUTFIL INCLUDE=(210,1,CH,NE,C'3'),FNAMES=DPCONV
 OUTFIL INCLUDE=(210,1,CH,EQ,C'3'),FNAMES=DPPIBB

