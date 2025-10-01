//CCRTAX2B JOB MSGCLASS=X,MSGLEVEL=(1,1),REGION=64M,NOTIFY=&SYSUID      JOB76242
//*--------------------------------------------------------------------
//NAMEFILE EXEC SAS609
//SORTWK01  DD UNIT=SYSDA,SPACE=(CYL,(1000,500))
//SORTWK02  DD UNIT=SYSDA,SPACE=(CYL,(1000,500))
//SORTWK03  DD UNIT=SYSDA,SPACE=(CYL,(1000,500))
//SORTWK04  DD UNIT=SYSDA,SPACE=(CYL,(1000,500))
//IDFILE   DD DISP=SHR,DSN=UNLOAD.ALLALIAS.OUT
//CCRISEXT DD DSN=CCRIS.ALIAS.GDG,
//            DISP=(NEW,CATLG,DELETE),
//            SPACE=(CYL,(300,300),RLSE),UNIT=SYSDA,
//            DCB=(LRECL=133,BLKSIZE=0,RECFM=FB)
//SASLIST  DD SYSOUT=X
//SYSIN    DD *
    DATA TAXID;
      INFILE IDFILE;
      INPUT  @005  CUSTNO             $11.
             @029  EFFDATE            PD5.
             @034  EFFTIME            $8.
             @089  ALIASKEY           $3.
             @092  ALIAS              $20.
             @343  MNTDATE            $10.;
       ALIAS10=SUBSTR(ALIAS,1,10);
    RUN;
   PROC SORT DATA=TAXID NODUPKEY; BY CUSTNO;RUN;
   PROC PRINT DATA=TAXID(OBS=10);TITLE 'TAXID';RUN;

DATA CCRIS;
  SET TAXID;
  FILE CCRISEXT;
     PUT @ 1   CUSTNO            $11.
         @12   '1'                        /* DUMMY FIELD */
         @13   ALIASKEY          $3.
         @16   ALIAS             $20.
         @53   ALIASKEY          $15.
         @68   ALIAS10           $10.;
  RETURN;
  RUN;
