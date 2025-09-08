  DATA TAXID_NEWIC;                                                    
      MERGE TAXID(IN=A) RHOLD(IN=B RENAME=(ALIAS=NEWIC1)); BY NEWIC1;  
      IF A;                                                            
      IF A AND B THEN C = 1;                                           
  RUN;                                                                 
PROC SORT DATA=TAXID_NEWIC; BY OLDIC; RUN;                             
