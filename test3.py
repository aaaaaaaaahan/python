DATA ALLMATCH;                                                     
   FORMAT REMARKS $150.;                                           
   MERGE MRGNAME(IN=M) MRGNDOB(IN=N) MRGID(IN=O) MRGIC(IN=P)       
         MRGNID(IN=Q) MRGNIC(IN=R);                                
   BY NAME NEWIC OTHID DOBDOR;                                     
   IF N OR O OR P OR Q OR R;                                       
   DEPT= 'AML/CFT';                                                
   CONTACT1= 'MS NG MEE WUN 03-21767651';                          
   CONTACT2= 'MS WONG LAI SAN 03-21763005';                        
   REMARKS = TRIM(DEPT)||''||TRIM(CONTACT1)||''||TRIM(CONTACT2);   
RUN;                                                               
PROC SORT DATA=ALLMATCH NODUPKEY; BY HCMNAME IC OLDID STAFFID; RUN;
PROC PRINT DATA=ALLMATCH(OBS=15);TITLE 'ALL MATCH';                
