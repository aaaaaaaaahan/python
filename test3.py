( "U_CIS_HOLD_CO_NO"                   
 POSITION(  00001:00002) DECIMAL       
, "U_CIS_BANK_NO"                      
 POSITION(  00003:00004) DECIMAL       
, "C_CIS_APPL_CODE"                    
 POSITION(  00005:00009) CHAR(00005)   
, "U_CIS_APPL_NO"                      
 POSITION(  00010:00029) CHAR(00020)   
, "H_CIS_PROCESS_TIME"                 
 POSITION(  00030:00037) TIME EXTERNAL 
, "C_CIS_STATUS"                       
 POSITION(  00038:00038) CHAR(00001)   
, "U_CIS_BRANCH"                       
 POSITION(  00039:00042) DECIMAL       
, "U_CIS_COST_CNTR"                    
 POSITION(  00043:00046) DECIMAL       
, "D_CIS_LST_MNT_DATE"                 
 POSITION(  00047:00056) DATE EXTERNAL 
, "C_CIS_LST_MNT_OPER"                 
 POSITION(  00057:00064) CHAR(00008) 
, "C_CIS_TAX_ID"                     
 POSITION(  00065:00065) CHAR(00001) 
, "U_CIS_TAX_NO"                     
 POSITION(  00066:00074) CHAR(00009) 
, "S_CIS_PAS_IND"                    
 POSITION(  00075:00075) CHAR(00001) 
, "S_CIS_WITHHOLDING"                
 POSITION(  00076:00076) CHAR(00001) 
, "C_CIS_ACCT_TYPE"                  
 POSITION(  00077:00081) CHAR(00005) 
, "U_CIS_PRIM_OFF"                   
 POSITION(  00082:00084) DECIMAL     
, "U_CIS_SEC_OFF"                    
 POSITION(  00085:00087) DECIMAL     
, "U_CIS_PRIM_LN_OFF"                
 POSITION(  00088:00090) DECIMAL     
, "U_CIS_SEC_LN_OFF"                 
 POSITION(  00091:00093) DECIMAL     
 , "C_CIS_CYC_CODE"                          
 POSITION(  00094:00096) CHAR(00003)        
, "C_CIS_MAIL_CODE"                         
 POSITION(  00097:00099) CHAR(00003)        
, "C_CIS_RESTRICT_CD"                       
 POSITION(  00100:00102) CHAR(00003)        
, "U_CIS_CENSUS_TRACT"                      
 POSITION(  00103:00107) DECIMAL            
, "C_CIS_CR_RATING"                         
 POSITION(  00108:00112) CHAR(00005)        
, "C_CIS_SOURCE"                            
 POSITION(  00113:00115) CHAR(00003)        
, "D_CIS_ACCT_OPEN"                         
 POSITION(  00117:00126) DATE EXTERNAL      
                         NULLIF(00116)=X'FF'
, "D_CIS_ACCT_CLOSED"                       
 POSITION(  00128:00137) DATE EXTERNAL      
                         NULLIF(00127)=X'FF'
, "C_CIS_MISC_DEMO_1"                       
 POSITION(  00138:00147) CHAR(00010) 
, "C_CIS_MISC_DEMO_2"                
 POSITION(  00148:00157) CHAR(00010) 
, "C_CIS_MISC_DEMO_3"                
 POSITION(  00158:00167) CHAR(00010) 
, "C_CIS_MISC_DEMO_4"                
 POSITION(  00168:00177) CHAR(00010) 
, "C_CIS_MISC_DEMO_5"                
 POSITION(  00178:00187) CHAR(00010) 
, "C_CIS_MISC_DEMO_6"                
 POSITION(  00188:00197) CHAR(00010) 
, "C_CIS_MISC_DEMO_7"                
 POSITION(  00198:00207) CHAR(00010) 
, "C_CIS_MISC_DEMO_8"                
 POSITION(  00208:00217) CHAR(00010) 
, "C_CIS_MISC_DEMO_9"                
 POSITION(  00218:00227) CHAR(00010) 
, "C_CIS_MISC_DEMO_10"               
 POSITION(  00228:00237) CHAR(00010) 
 , "LAST_MAINT_TIME"                          
 POSITION(  00238:00245) TIME EXTERNAL       
, "PREV_MAINT_DATE"                          
 POSITION(  00247:00256) DATE EXTERNAL       
                         NULLIF(00246)=X'FF' 
, "CURR_CDE"                                 
 POSITION(  00257:00259) CHAR(00003)         
)                                            
