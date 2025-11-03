convert program to python with duckdb and pyarrow
duckdb for process input file and output parquet&csv
assumed all the input file ady convert to parquet can directly use it

//CIHCMDWJ JOB MSGCLASS=X,MSGLEVEL=(1,1),REGION=8M,NOTIFY=&SYSUID       JOB69275
//*********************************************************************
//COPYFIL1 EXEC PGM=ICEGENER
//SYSPRINT DD SYSOUT=X
//SYSUT1   DD DISP=SHR,DSN=HCM.STAFF.LIST
//SYSUT2   DD DSN=HCM.STAFF.LIST.BKP(+1),
//            DISP=(NEW,CATLG,DELETE),UNIT=SYSDA,
//            DCB=(LRECL=500,BLKSIZE=0,RECFM=FB),
//            SPACE=(CYL,(50,10),RLSE)
//SYSIN    DD DUMMY
//*********************************************************************
//* MATCH STAFF IC AND NAME AGAINST CIS RECORDS ICIRHHC1
//* (1) IC MATCH, (2) NAME AND IC MATCH, (3) NAME AND (4) NAME AND DOB
//*********************************************************************
//MATCH#1  EXEC SAS609
//HCMFILE  DD DISP=SHR,DSN=HCM.STAFF.LIST
//DOWJONES  DD DISP=SHR,DSN=UNLOAD.CIDOWJ1T.FB
//OUTPUT   DD DSN=HCM.DOWJONES.MATCH(+1),
//            DISP=(NEW,CATLG,DELETE),
//            SPACE=(CYL,(50,10),RLSE),UNIT=SYSDA,
//            DCB=(LRECL=500,BLKSIZE=0,RECFM=FB)
//SASLIST  DD SYSOUT=X
//SORTWK01 DD UNIT=SYSDA,SPACE=(CYL,(100,100))
//SORTWK02 DD UNIT=SYSDA,SPACE=(CYL,(100,100))
//SORTWK03 DD UNIT=SYSDA,SPACE=(CYL,(100,100))
//SORTWK04 DD UNIT=SYSDA,SPACE=(CYL,(100,100))
//SYSIN    DD *
 /*----------------------------------------------------------------*/   00190000
 /*    DOWJONES FILE                                               */   00200000
 /*----------------------------------------------------------------*/   00210000
    DATA DNAME DIC DID NDOB NNEW NOLD;                                  00220000
      DROP DOBYY DOBMM DOBDD;                                           00230000
      INFILE DOWJONES;                                                  00240000
       INPUT  @001   CUSTNAME          $40.                             00250000
              @041   ALIAS             $20.                             00260000
              @092   DESC1             $4.                              00270000
              @096   DOBYY             $4.                              00280000
              @101   DOBMM             $2.                              00290000
              @104   DOBDD             $2.;                             00300000
       CHCKLEN = LENGTH(ALIAS);                                         00310000
       IF CHCKLEN = 12 THEN NEWIC=ALIAS;                                00320000
       ELSE OTHID=ALIAS;                                                00330000
       DOBDOR = DOBYY||DOBMM||DOBDD;                                    00340000
       NAME = CUSTNAME;                                                 00350000
       IF CUSTNAME NE ' ' THEN OUTPUT DNAME;                            00360000
       IF NEWIC NE ' ' THEN OUTPUT DIC;                                 00370000
       IF OTHID NE ' ' THEN OUTPUT DID;                                 00380000
       IF CUSTNAME NE ' ' AND DOBDOR NE ' ' THEN OUTPUT NDOB;           00390000
       IF CUSTNAME NE ' ' AND NEWIC NE ' ' THEN OUTPUT NNEW;            00400000
       IF CUSTNAME NE ' ' AND OTHID NE ' ' THEN OUTPUT NOLD;            00410000
                                                                        00420000
    RUN;                                                                00430000
    PROC SORT  DATA=DNAME NODUPKEY;BY NAME;RUN;                         00440000
    PROC PRINT DATA=DNAME (OBS=10);TITLE 'DJW NAME'; RUN;               00450000
    PROC SORT  DATA=DIC NODUPKEY;BY NEWIC;RUN;                          00460000
    PROC PRINT DATA=DIC (OBS=10);TITLE 'DJW IC ONLY'; RUN;              00470000
    PROC SORT  DATA=DID NODUPKEY;BY OTHID;RUN;                          00480000
    PROC PRINT DATA=DID (OBS=10);TITLE 'DJW ID ONLY'; RUN;              00490000
    PROC SORT  DATA=NDOB NODUPKEY;BY NAME DOBDOR ;RUN;                  00500000
    PROC PRINT DATA=NDOB(OBS=10);TITLE 'DJW NAME DOB '; RUN;            00510000
    PROC SORT  DATA=NNEW NODUPKEY;BY NAME NEWIC;RUN;                    00520000
    PROC PRINT DATA=NNEW(OBS=10);TITLE 'DJW NAME NEWIC'; RUN;           00530000
    PROC SORT  DATA=NOLD NODUPKEY;BY NAME OTHID;RUN;                    00540000
    PROC PRINT DATA=NOLD(OBS=10);TITLE 'DJW NAME OLDID'; RUN;           00550000
                                                                        00560000
DATA HCMOLD HCMNEW HCMALL HCMNDOB HCMNNEW HCMNOLD;                      00570000
   INFILE HCMFILE DELIMITER = ';' MISSOVER DSD;                         00580000
        FORMAT OTHID $20. DOBDT $10. DOBDOR $8. DOBDD $2.               00590000
        DOBMM $2. DOBYYYY $4.;                                          00600000
        INFORMAT STAFFID        $05.;               /*  1  */           00610000
        INFORMAT HCMNAME        $40.;               /*  2  */           00620000
        INFORMAT OLDID          $15.;               /*  3  */           00630000
        INFORMAT IC             $15.;               /*  4  */           00640000
        INFORMAT DOB            $10.;               /*  5  */           00650000
        INFORMAT BASE           $20.;               /*  6  */           00660000
        INFORMAT COMPCODE       $05.;               /*  7  */           00670000
        INFORMAT DESIGNATION    $30.;               /*  8  */           00680000
        INFORMAT STATUS         $01.;               /*  9  */           00690000
                                                                        00700000
          FORMAT STAFFID        $05.;               /*  1  */           00710000
          FORMAT HCMNAME        $40.;               /*  2  */           00720000
          FORMAT OLDID          $15.;               /*  3  */           00730000
          FORMAT IC             $15.;               /*  4  */           00740000
          FORMAT DOB            $10.;               /*  5  */           00750000
          FORMAT BASE           $20.;               /*  6  */           00760000
          FORMAT COMPCODE       $05.;               /*  7  */           00770000
          FORMAT DESIGNATION    $30.;               /*  8  */           00780000
          FORMAT STATUS         $01.;               /*  9  */           00790000
                                                                        00800000
        INPUT  STAFFID        $                   /*  1  */             00810000
               HCMNAME        $                   /*  2  */             00820000
               OLDID          $                   /*  3  */             00830000
               IC             $                   /*  4  */             00840000
               DOB            $                   /*  5  */             00850000
               BASE           $                   /*  6  */             00860000
               COMPCODE       $                   /*  7  */             00870000
               DESIGNATION    $                   /*  8  */             00880000
               STATUS         $ ;                 /*  9  */             00890000
              OTHID = OLDID;                                            00900000
              NAME = HCMNAME;                                           00910000
              NEWIC= IC;                                                00920000
              DOBDD = SUBSTR(DOB,1,2);                                  00930000
              DOBMM = SUBSTR(DOB,4,2);                                  00940000
              DOBYYYY = SUBSTR(DOB,7,4);                                00950000
              DOBDT = DOBDD||'-'||DOBMM||'-'||DOBYYYY;                  00960000
              DOBDOR=DOBYYYY||DOBMM||DOBDD;                             00970000
              IF OTHID NE ' ' THEN OUTPUT HCMOLD;                       00980000
              IF NEWIC NE ' ' THEN OUTPUT HCMNEW;                       00990000
              IF NAME NE ' ' AND OTHID NE ' ' THEN OUTPUT HCMNOLD;      01000000
              IF NAME NE ' ' AND NEWIC NE ' ' THEN OUTPUT HCMNNEW;      01010000
              IF NAME  NE ' ' THEN OUTPUT HCMALL;                       01020000
              IF NAME NE ' ' AND DOBDOR NE ' ' THEN OUTPUT HCMNDOB;     01030000
RUN;                                                                    01040000
PROC SORT DATA=HCMOLD NODUPKEY; BY OTHID STAFFID ;RUN;                  01050000
PROC PRINT DATA=HCMOLD(OBS=15);TITLE 'HCM OLD';                         01060000
PROC SORT DATA=HCMNEW NODUPKEY; BY NEWIC STAFFID ;RUN;                  01070000
PROC PRINT DATA=HCMNEW(OBS=15);TITLE 'HCM NEW';                         01080000
PROC SORT DATA=HCMNOLD NODUPKEY; BY NAME OTHID STAFFID;RUN;             01090000
PROC PRINT DATA=HCMNOLD(OBS=15);TITLE 'HCM NAME OLD';                   01100000
PROC SORT DATA=HCMNNEW NODUPKEY; BY NAME NEWIC STAFFID;RUN;             01110000
PROC PRINT DATA=HCMNNEW(OBS=15);TITLE 'HCM NAME NEW';                   01120000
PROC SORT DATA=HCMALL NODUPKEY; BY NAME STAFFID;RUN;                    01130000
PROC PRINT DATA=HCMALL(OBS=15);TITLE 'HCM NAME';                        01140000
PROC SORT DATA=HCMNDOB NODUPKEY; BY NAME DOBDOR STAFFID;RUN;            01150000
PROC PRINT DATA=HCMNDOB(OBS=15);TITLE 'HCM NAME DOB';                   01160000
                                                                        01170000
DATA MRGNAME;                                                           01180000
   FORMAT MATCH_DESC $25. REASON $30.;                                  01190000
   MERGE DNAME(IN=A) HCMALL(IN=B); BY NAME ;                            01200000
   IF A AND B;                                                          01210000
   M_NAME = 'Y';                                                        01220000
   MATCH_IND = '6';                                                     01230000
   REASON = 'DOWJONES NAME MATCH';                                      01240000
RUN;                                                                    01250000
PROC SORT DATA=MRGNAME; BY NAME NEWIC OTHID DOBDOR; RUN;                01260000
PROC PRINT DATA=MRGNAME(OBS=5);TITLE 'NAME MATCH';                      01270000
                                                                        01280000
DATA MRGID;                                                             01290000
   FORMAT MATCH_DESC $25. REASON $30.;                                  01300000
   MERGE DID(IN=C) HCMOLD(IN=D); BY OTHID ;                             01310000
   IF C AND D;                                                          01320000
   MATCH_IND = '4';                                                     01330000
   M_ID = 'Y';                                                          01340000
   REASON = 'DOWJONES ID MATCH';                                        01350000
RUN;                                                                    01360000
PROC SORT DATA=MRGID; BY NAME NEWIC OTHID DOBDOR; RUN;                  01370000
PROC PRINT DATA=MRGID(OBS=5);TITLE 'ID MATCH';                          01380000
                                                                        01390000
DATA MRGIC;                                                             01400000
   FORMAT MATCH_IND $01. REASON $30.;                                   01410000
   MERGE DIC(IN=E) HCMNEW(IN=F); BY NEWIC ;                             01420000
   IF E AND F;                                                          01430000
   MATCH_IND = '3';                                                     01440000
   M_IC = 'Y';                                                          01450000
   REASON = 'DOWJONES IC MATCH';                                        01460000
RUN;                                                                    01470000
PROC SORT DATA=MRGIC; BY NAME NEWIC OTHID DOBDOR; RUN;                  01480000
PROC PRINT DATA=MRGIC(OBS=5);TITLE 'IC MATCH';                          01490000
                                                                        01500000
DATA MRGNDOB;                                                           01510000
   FORMAT MATCH_IND $01. REASON $30.;                                   01520000
   MERGE NDOB(IN=G) HCMNDOB(IN=H); BY NAME DOBDOR ;                     01530000
   IF G AND H;                                                          01540000
   MATCH_IND = '5';                                                     01550000
   M_DOB = 'Y';                                                         01560000
   REASON = 'DOWJONES NAME AND DOB MATCH';                              01570000
RUN;                                                                    01580000
PROC SORT DATA=MRGNDOB; BY NAME NEWIC OTHID DOBDOR; RUN;                01590000
PROC PRINT DATA=MRGNDOB(OBS=5);TITLE 'DOB MATCH';                       01600000
                                                                        01610000
DATA MRGNID;                                                            01620000
   FORMAT MATCH_IND $01. REASON $30.;                                   01630000
   MERGE NOLD(IN=I) HCMNOLD(IN=J); BY NAME OTHID ;                      01640000
   IF I AND J;                                                          01650000
   MATCH_IND = '2';                                                     01660000
   M_NID = 'Y';                                                         01670000
   REASON = 'DOWJONES NAME AND ID MATCH';                               01680000
RUN;                                                                    01690000
PROC SORT DATA=MRGNID; BY NAME NEWIC OTHID DOBDOR; RUN;                 01700000
PROC PRINT DATA=MRGNID(OBS=5);TITLE 'NAME ID MATCH';                    01710000
                                                                        01720000
DATA MRGNIC;                                                            01730000
   FORMAT MATCH_IND $01. REASON $30.;                                   01740000
   MERGE NNEW(IN=K) HCMNNEW(IN=L); BY NAME NEWIC ;                      01750000
   IF K AND L;                                                          01760000
   MATCH_IND = '1';                                                     01770000
   M_NIC = 'Y';                                                         01780000
   REASON = 'DOWJONES NAME AND IC MATCH';                               01790000
RUN;                                                                    01800000
PROC SORT DATA=MRGNIC; BY NAME NEWIC OTHID DOBDOR; RUN;                 01810000
PROC PRINT DATA=MRGNIC(OBS=5);TITLE 'NAME IC MATCH';                    01820000
                                                                        01830000
                                                                        01840000
DATA ALLMATCH;                                                          01850000
   FORMAT REMARKS $150.;                                                01860000
   MERGE MRGNAME(IN=M) MRGNDOB(IN=N) MRGID(IN=O) MRGIC(IN=P)            01870000
         MRGNID(IN=Q) MRGNIC(IN=R);                                     01880000
   BY NAME NEWIC OTHID DOBDOR;                                          01890000
   IF N OR O OR P OR Q OR R;                                            01900000
   DEPT= 'AML/CFT';                                                     01910000
   CONTACT1= 'MS NG MEE WUN 03-21767651';                               01920000
   CONTACT2= 'MS WONG LAI SAN 03-21763005';                             01930000
   REMARKS = TRIM(DEPT)||''||TRIM(CONTACT1)||''||TRIM(CONTACT2);        01940000
RUN;                                                                    01950000
PROC SORT DATA=ALLMATCH NODUPKEY; BY HCMNAME IC OLDID STAFFID; RUN;     01960000
PROC PRINT DATA=ALLMATCH(OBS=15);TITLE 'ALL MATCH';                     01970000
                                                                        01980000
                                                                        01990000
  DATA OUTPUT;                                                          02000000
   SET ALLMATCH;                                                        02010000
       IF M_NAME EQ ' ' THEN M_NAME = 'N';                              02020000
       IF M_NIC  EQ ' ' THEN M_NIC  = 'N';                              02030000
       IF M_NID  EQ ' ' THEN M_NID  = 'N';                              02040000
       IF M_IC   EQ ' ' THEN M_IC   = 'N';                              02050000
       IF M_ID   EQ ' ' THEN M_ID   = 'N';                              02060000
       IF M_DOB  EQ ' ' THEN M_DOB  = 'N';                              02070000
   FILE OUTPUT;                                                         02080000
        PUT @001  HCMNAME      $40.                                     02090000
            @042  OLDID        $15.                                     02100000
            @058  IC           $12.                                     02110000
            @070  MATCH_IND    $01.                                     02120000
            @072  DOBDT        $10.                                     02130000
            @085  BASE         $20.                                     02140000
            @110  DESIGNATION  $30.                                     02150000
            @145  REASON       $30.                                     02160000
            @179  M_NAME       $01.                                     02170000
            @180  M_NIC        $01.                                     02180000
            @181  M_NID        $01.                                     02190000
            @182  M_IC         $01.                                     02200000
            @183  M_ID         $01.                                     02210000
            @184  M_DOB        $01.                                     02220000
            @185  COMPCODE     $05.                                     02230000
            @190  STAFFID      $05.                                     02240000
            @196  REMARKS      $150.                                    02250000
            @347  DEPT         $150.;                                   02260000
  RUN;                                                                  02270000
  PROC PRINT DATA=OUTPUT(OBS=5);TITLE 'OUTPUT';                         02280000
