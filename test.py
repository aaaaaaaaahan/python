DATA OUTPUT;
  SET MERGEDP MERGELN MERGESDB MERGEUNI MERGECOM;
  /*IF ACCTNOC = ' ' THEN DELETE; */
  FILE OUTPUT;
    PUT @01   CUSTNO            $11.
        @14   ACCTNOC           $20.
        @36   OCCUP             $3.
        @42   MASCO2008         $5.
        @50   ALIASKEY          $5.
        @56   ALIAS             $20.
        @78   CUSTNAME          $40.
        @120  DATEOPEN          $8.
        @130  DATECLSE          $8.
        @140  LEDGERBAL         13.2
        @152  BANKINDC          $1.
        @154  CITIZENSHIP       $2.
        @158  APPL_CODE         $5.
        @164  PRODTY            $3.
        @169  DEMODESC          $60.
        @231  MASCODESC         $27.
        @260  JOINTACC          $1.
        @262  MSICCODE          $5.
        @267  ACCTBRCH          $3.
        @275  BRANCH_ABBR       $5.
        @280  ACCTSTATUS        $24.
        @304  SICCODE           $5.;
 RUN;
