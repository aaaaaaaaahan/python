//CIRMKLNS JOB MSGCLASS=X,MSGLEVEL=(1,1),REGION=64M,NOTIFY=&SYSUID      J0125252
//*--------------------------------------------------------------------
//INITDASD EXEC PGM=IEFBR14
//DEL1     DD DSN=RBP2.B033.LOANS.CISRMRK.EMAIL.DUP,
//            DISP=(MOD,DELETE,DELETE),UNIT=SYSDA,SPACE=(TRK,(0))
//DEL2     DD DSN=RBP2.B033.LOANS.CISRMRK.EMAIL,
//            DISP=(MOD,DELETE,DELETE),UNIT=SYSDA,SPACE=(TRK,(0))
//*--------------------------------------------------------------------
//STATS#01 EXEC SAS609
//SORTWK01 DD UNIT=SYSDA,SPACE=(CYL,(1000,500))
//SORTWK02 DD UNIT=SYSDA,SPACE=(CYL,(1000,500))
//RMKFILE  DD DISP=SHR,DSN=CISRMRK.EMAIL.FIRST
//LNSPRIM  DD DISP=SHR,DSN=LOANS.CUST.PRIMARY
//LNSSECD  DD DISP=SHR,DSN=LOANS.CUST.SCNDARY
//OUTFILE  DD DSN=LOANS.CISRMRK.EMAIL.DUP,
//            DISP=(NEW,CATLG,DELETE),
//            SPACE=(CYL,(200,200),RLSE),UNIT=SYSDA,
//            DCB=(LRECL=600,BLKSIZE=0,RECFM=FB)
//SASLIST  DD SYSOUT=X
//SYSIN    DD *

DATA RMK;
   INFILE RMKFILE;
   INPUT @009     CUSTNO                      $11.
         @052     REMARKS                     $60.;
RUN;
PROC SORT  DATA=RMK; BY CUSTNO;RUN;
PROC PRINT DATA=RMK(OBS=10);TITLE 'REMARK DATA';RUN;

DATA PRIM;
   INFILE LNSPRIM;
   INPUT @001     CUSTNO              $11.
         @022     ACCTNOC             $20.
         @052     DOBDOR              $10.
         @063     LONGNAME            $150.
         @220     INDORG              $1.
         @222     PRIMSEC             $1.;
RUN;
PROC SORT  DATA=PRIM; BY ACCTNOC;RUN;
PROC PRINT DATA=PRIM(OBS=10);TITLE 'PRIM DATA';RUN;

DATA SECD;
   INFILE LNSSECD;
   INPUT @001     CUSTNO1             $11.
         @022     ACCTNOC             $20.
         @052     DOBDOR1             $10.
         @063     LONGNAME1           $150.
         @220     INDORG1             $1.
         @222     PRIMSEC1            $1.;
RUN;
PROC SORT  DATA=SECD; BY ACCTNOC;RUN;
PROC PRINT DATA=SECD(OBS=10);TITLE 'SECD DATA';RUN;

 /*-----------------------------------------------------------*/
 /*  MATCH TO GET JOINT (FOR CUSTOMER NAME)/2024-3454         */
 /*-----------------------------------------------------------*/
DATA MATCH1 XMATCH;
FORMAT JOINT $1.;
MERGE PRIM(IN=A) SECD(IN=B);
      BY ACCTNOC;
      IF A AND NOT B THEN DO;
      JOINT = 'N';
      OUTPUT XMATCH;
      END;
      IF A AND B THEN DO;
      JOINT = 'Y';
      LONGNAME=TRIM(LONGNAME)||' & ' ||TRIM(LONGNAME1);
      OUTPUT MATCH1;
      END;
RUN;
PROC SORT  DATA=MATCH1; BY CUSTNO ;RUN;
PROC PRINT DATA=MATCH1(OBS=10) ;TITLE 'JOINT';RUN;
PROC SORT  DATA=XMATCH; BY CUSTNO ;RUN;
PROC PRINT DATA=XMATCH(OBS=10) ;TITLE 'XJOINT';RUN;

 /*-----------------------------------------------------------*/
 /*  MATCH EMAIL DATASET AND CUST DAILY                       */
 /*-----------------------------------------------------------*/
DATA MATCH2;
MERGE RMK(IN=A)  MATCH1(IN=B);
      BY CUSTNO;
      IF B;
RUN;
PROC SORT  DATA=MATCH2; BY ACCTNOC;RUN;
PROC PRINT DATA=MATCH2(OBS=10) ;TITLE 'MATCH 2';RUN;

DATA MATCH3;
MERGE RMK(IN=A)  XMATCH(IN=B);
      BY CUSTNO;
      IF B;
RUN;
PROC SORT  DATA=MATCH3 ;BY ACCTNOC;RUN;
PROC PRINT DATA=MATCH3(OBS=10) ;TITLE 'MATCH 3';RUN;

 DATA OUT1;
 FILE OUTFILE;
   SET MATCH2 MATCH3;
       PUT @001     CUSTNO              $20.
           @022     ACCTNOC             $20.
           @042     REMARKS             $60.
           @143     DOBDOR              $10.
           @160     LONGNAME            $200.
           @400     INDORG              $1.
           @402     JOINT               $1.
           ;
 RUN;
 PROC PRINT DATA=OUT1(OBS=5);TITLE 'FULL OUTPUT ';

//*---------------------------------------------------------------------
//* GET THE LAST ROW ONLY
//*---------------------------------------------------------------------
//SORT01   EXEC PGM=ICETOOL
//INFILE1    DD DSN=LOANS.CISRMRK.EMAIL.DUP,DISP=SHR
//OUTFILE1   DD DSN=LOANS.CISRMRK.EMAIL,
//            DISP=(NEW,CATLG,DELETE),
//            SPACE=(CYL,(10,5),RLSE),UNIT=SYSDA,
//            DCB=(RECFM=FB,LRECL=600,BLKSIZE=0)
//TOOLMSG    DD SYSOUT=*
//DFSMSG     DD SYSOUT=*
//TOOLIN     DD *
  SELECT FROM(INFILE1) TO(OUTFILE1) ON(1,20,CH) LASTDUP
