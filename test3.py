INPUT @001  BANKNO             $3.
             @004  APPLCODE           $5.
             @009  CUSTNO             $11.
             @029  PHONETYPE          $15.
             @044  PHONEPAC           PD8.
             @052  PHONEPREV          PD8.
             @060  INDORG             $1.
             @061  FIRSTDATE          $10.
             @072  PROMTSOURCE        $5.
             @077  PROMPTDATE         $10.
             @077  PROMPTYY            4.
             @082  PROMPTMM            2.
             @085  PROMPTDD            2.
             @087  PROMPTTIME         $8.
             @095  UPDSOURCE          $5.
             @100  UPDTYY              4.
             @105  UPDTMM              2.
             @108  UPDTDD              2.
             @110  UPDTIME            $8.
             @118  UPDOPER            $8.;

( "CI_BANK_NO"                         
 POSITION(  00001:00003) CHAR(00003)   
, "CI_APPL_CODE"                       
 POSITION(  00004:00008) CHAR(00005)   
, "CI_APPL_NO"                         
 POSITION(  00009:00028) CHAR(00020)   
, "CI_PHONE_FIELD"                     
 POSITION(  00029:00043) CHAR(00015)   
, "CI_PAC_PHONE"                       
 POSITION(  00044:00051) DECIMAL       
, "CI_PREV_PHONE"                      
 POSITION(  00052:00059) DECIMAL       
, "CI_CUST_TYPE"                       
 POSITION(  00060:00060) CHAR(00001)   
, "CI_FIRST_DATE"                      
 POSITION(  00061:00070) DATE EXTERNAL 
, "CI_NO_OF_PROMPT"                    
 POSITION(  00071:00071) DECIMAL       
, "CI_PROMPT_SOURCE"                   
 POSITION(  00072:00076) CHAR(00005)   
, "CI_PROMPT_DATE"                     
 POSITION(  00077:00086) DATE EXTERNAL 
, "CI_PROMPT_TIME"                     
 POSITION(  00087:00094) TIME EXTERNAL 
, "CI_UPDATE_SOURCE"                   
 POSITION(  00095:00099) CHAR(00005)   
, "CI_UPDATE_DATE"                     
 POSITION(  00100:00109) DATE EXTERNAL 
, "CI_UPDATE_TIME"                     
 POSITION(  00110:00117) TIME EXTERNAL 
, "CI_UPDATE_OPERATOR"                 
 POSITION(  00118:00125) CHAR(00008)   
, "CI_TRX_APPL_CODE"                   
 POSITION(  00126:00130) CHAR(00005)   
, "CI_TRX_APPL_NO"                     
 POSITION(  00131:00150) CHAR(00020)   
, "CI_NEW_PHONE"                
 POSITION(  00151:00158) DECIMAL
)                               
