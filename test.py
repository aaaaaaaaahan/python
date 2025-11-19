convert program to python with duckdb and pyarrow
duckdb for process input file and output parquet&txt
assumed all the input file ady convert to parquet can directly use it

//CICRDHRC JOB MSGCLASS=X,MSGLEVEL=(1,1),REGION=8M,NOTIFY=&SYSUID       JOB44392
//*--------------------------------------------------------------------
//DELETE   EXEC PGM=IEFBR14
//DEL1     DD DSN=UNICARD.HRC.ALLCUST,
//            DISP=(MOD,DELETE,DELETE),SPACE=(TRK,(0))
//*--------------------------------------------------------------------
//RESCORP   EXEC SAS609
//UNICARD   DD DISP=SHR,DSN=UNICARD.HRC.CIS33
//          DD DISP=SHR,DSN=UNICARD.HRC.CIS34
//          DD DISP=SHR,DSN=UNICARD.HRC.CIS54
//          DD DISP=SHR,DSN=UNICARD.HRC.CIS55
//CIS       DD DISP=SHR,DSN=CIS.CUST.DAILY
//OUTHRC    DD DSN=UNICARD.HRC.ALLCUST,
//             DISP=(NEW,CATLG,DELETE),UNIT=SYSDA,
//             SPACE=(CYL,(10,10),RLSE),
//             DCB=(LRECL=582,BLKSIZE=0,RECFM=FB)
//SASLIST   DD SYSOUT=X
//SYSIN     DD *
DATA HRCUNICRD;
  FORMAT ALIAS $37.;
  INFILE UNICARD;
  INPUT  @001  ALIAS               $12.
         @013  BRCHCODE             $7.
         @020  ACCTTYPE             $5.
         @025  APPROVALSTATUS       $2.
         @027  ACCTNO               16.
         @043  CUSTTYPE             $1.
         @044  CUSTNAME           $150.
         @194  CUSTGENDER           $3.
         @197  CUSTDOBDOR           $8.
         @205  CUSTEMPLOYER        $30.
         @235  CUSTADDR1           $30.
         @265  CUSTADDR2           $30.
         @295  CUSTADDR3           $30.
         @325  CUSTADDR4           $30.
         @355  CUSTADDR5           $30.
         @385  CUSTPHONE           $12.
         @397  DTCORGUNIT           $5.
         @402  DTCNATION            $3.
         @405  DTCOCCUP             $5.
         @410  DTCACCTTYPE         $10.
         @420  DTCWEIGHTAGE         $1.
         @421  DTCTOTAL             4.1
         @425  DTCSCORE1             4.
         @429  DTCSCORE2             4.
         @433  DTCSCORE3             4.
         @437  DTCSCORE4             4.
         @441  DTCSCORE5             4.
         @445  DTCSCORE6             4.
         @449  FATCA                $1.
         @450  HOVERIFYREMARKS     $40.;
RUN;
PROC SORT  DATA=HRCUNICRD;BY ALIAS ACCTNO;RUN;
PROC PRINT DATA=HRCUNICRD;TITLE 'HRC UNICARD';RUN;

PROC SORT DATA=CIS.CUSTDLY(KEEP=ALIASKEY ALIAS CUSTNO ACCTNO)
          OUT=CUST;BY ALIAS ACCTNO;
   WHERE ALIAS NE '';
RUN;
PROC PRINT DATA=CUST (OBS=05);TITLE 'CUST';RUN;

DATA MRGCUST;
   MERGE HRCUNICRD(IN=A) CUST(IN=B);
   BY ALIAS ACCTNO;
   IF A AND B;
RUN;
PROC SORT  DATA=MRGCUST;BY CUSTNO;RUN;
PROC PRINT DATA=MRGCUST(OBS=05);TITLE 'MERGEDCUST';RUN;
 /*----------------------------------------------------------------*/
 /*   OUTPUT DETAIL REPORT                                         */
 /*----------------------------------------------------------------*/
DATA OUTPUT;
  SET MRGCUST;
  FILE OUTHRC;
        PUT @001  ALIASKEY           $3.
            @004  ALIAS             $12.
            @016  BRCHCODE           $7.
            @023  ACCTTYPE           $5.
            @028  APPROVALSTATUS     $2.
            @030  ACCTNO             16.
            @046  CUSTNO            $20.
            @066  CUSTTYPE           $1.
            @067  CUSTNAME         $120.
            @187  CUSTGENDER        $10.
            @197  CUSTDOBDOR        $10.
            @207  CUSTEMPLOYER     $120.
            @327  CUSTADDR1         $30.
            @357  CUSTADDR2         $30.
            @387  CUSTADDR3         $30.
            @417  CUSTADDR4         $30.
            @447  CUSTADDR5         $30.
            @477  CUSTPHONE         $12.
            @489  DTCORGUNIT         $5.
            @494  DTCNATION          $3.
            @497  DTCOCCUP           $5.
            @502  DTCACCTTYPE       $10.
            @512  DTCWEIGHTAGE       $1.
            @513  DTCTOTAL
            @518  DTCSCORE1
            @522  DTCSCORE2
            @526  DTCSCORE3
            @530  DTCSCORE4
            @534  DTCSCORE5
            @538  DTCSCORE6
            @542  HOVERIFYREMARKS   $40.
            @582  FATCA              $1.;
RUN;
PROC PRINT; RUN;
