NUMRECS               129394          
( "U_IBS_HOLD_CO_NO"                  
 POSITION(  00001:00002) DECIMAL      
, "U_IBS_BANK_NO"                     
 POSITION(  00003:00004) DECIMAL      
, "U_IBS_APPL_NO"                     
 POSITION(  00005:00024) CHAR(00020)  
, "C_IBS_APPL_CODE"                   
 POSITION(  00025:00029) CHAR(00005)  
, "C_IBS_OWN_TYPE"                    
 POSITION(  00030:00031) CHAR(00002)  
, "D_IBS_EFF_DATE"                    
 POSITION(  00032:00036) DECIMAL      
, "U_IBS_R_HOLD_CO_NO"                
 POSITION(  00037:00038) DECIMAL      
, "U_IBS_R_BANK_NO"                   
 POSITION(  00039:00040) DECIMAL      
, "C_IBS_R_APPL_CODE"                 
 POSITION(  00041:00045) CHAR(00005)  
, "U_IBS_R_APPL_NO"                         
 POSITION(  00046:00065) CHAR(00020)        
, "C_IBS_E1_TO_E2"                          
 POSITION(  00066:00067) DECIMAL            
, "C_IBS_E2_TO_E1"                          
 POSITION(  00068:00069) DECIMAL            
, "H_IBS_PROCESS_TIME"                      
 POSITION(  00070:00077) TIME EXTERNAL      
, "C_IBS_RELATION_TYP"                      
 POSITION(  00078:00079) CHAR(00002)        
, "D_IBS_EXPIRE_DATE"                       
 POSITION(  00081:00090) DATE EXTERNAL      
                         NULLIF(00080)=X'FF'
, "DEF_TYPE"                                
 POSITION(  00091:00091) CHAR(00001)        
, "DEF_NBR"                                 
 POSITION(  00092:00093) DECIMAL            
, "SUB_ACCT_NBR"                            
 POSITION(  00094:00099) DECIMAL            
)
