     IF _N_ = 1 THEN DO;
      PUT @01   'LIST OF CUSTOMERS INFORMATION';
      PUT @01   'NO'                        @6    ';'
          @07   'ID TYPE'                   @15   ';'
          @16   'ID NUMBER'                 @37   ';'
          @38   'CUST NAME'                 @79   ';'
          @80   'CIS NUMBER'                @92   ';'
          @93   'OCCUPATION'                @154  ';'
          @155  'MASCO'                     @183  ';'
          @184  'SIC CODE'                  @193  ';'
          @194  'MSIC BIS TYPE'             @222  ';'
          @223  'ACCT NUMBER'               @244  ';'
          @245  'ACCT BRANCH'               @257  ';'
          @258  'ACCT STATUS'               @283  ';'
          @284  'DATE ACCT OPEN  '          @301  ';'
          @302  'DATE ACCT CLOSED'          @320  ';'   /*235004*/
          @321  'SDB(YES/NO)'               @333  ';'   /*235004*/
          @334  'BR SDB MAINTAN'            @348  ';'   /*235004*/
          @349  'CURRENT BALANCE'           @364  ';'   /*235004*/
          @365  'CURR CYC DR'               @376  ';'   /*235004*/
          @377  'CURR AMT DR'               @394  ';'   /*235004*/
          @395  'CURR CYC CR'               @406  ';'   /*235004*/
          @407  'CURR AMT CR'               @424  ';'   /*235004*/
          @425  'PREV CYC DR'               @442  ';'   /*235004*/
          @443  'PREV AMT DR'               @460  ';'   /*235004*/
          @461  'PREV CYC CR'               @478  ';'   /*235004*/
          @479  'PREV AMT CR'               @496  ';'   /*235004*/
          @497  'POST INDICATOR'            @511  ';'   /*235004*/
          @512  'POST INDICATOR REASON'     @533  ';'   /*235004*/
          @534  'TOTAL OF HOLD'             @547  ';'   /*235004*/
          @548  'SEQ OF HOLD(1)'            @562  ';'   /*235004*/
          @563  'AMT OF HOLD(1)'            @579  ';'   /*235004*/
          @580  'DESCRIP OF HOLD(1)'        @600  ';'   /*235004*/
          @601  'SOURCE(1)'                 @621  ';'   /*235004*/
          @622  'SEQ OF HOLD(2)'            @636  ';'   /*235004*/
          @637  'AMT OF HOLD(2)'            @653  ';'   /*235004*/
          @654  'DESCRIP OF HOLD(2)'        @674  ';'   /*235004*/
          @675  'SOURCE(2)'                 @695  ';'   /*235004*/
          @696  'SEQ OF HOLD(3)'            @700  ';'   /*235004*/
          @701  'AMT OF HOLD(3)'            @717  ';'   /*235004*/
          @718  'DESCRIP OF HOLD(3)'        @738  ';'   /*235004*/
          @739  'SOURCE(3)'                 @759  ';'   /*235004*/
           ;                                            /*235004*/
     END;
      PUT @01   _N_               5.        @6    ';'
          @07   ALIASKEY          $5.       @15   ';'
          @16   ALIAS             $20.      @37   ';'
          @38   CUSTNAME          $40.      @79   ';'
          @80   CUSTNO            $11.      @92   ';'
          @93   DEMODESC          $60.      @154  ';'
          @155  MASCODESC         $27.      @183  ';'
          @184  SICCODE           $5.       @193  ';'
          @194  MSICDESC          $27.      @222  ';'
          @223  ACCTNOC           $20.      @244  ';'
          @245  BRANCH_ABBR       $5.       @257  ';'
          @258  ACCTSTATUS        $24.      @283  ';'
          @284  DATEOPEN          $8.       @301  ';'
          @302  DATECLSE          $8.       @320  ';'   /*235004*/
          @321  SDBIND               $03.   @333  ';'   /*235004*/
          @334  SDBBRH               $03.   @348  ';'   /*235004*/
          @349  TEMP_CURBAL        14.2     @364  ';'   /*235004*/
          @365  TEMP_CURR_CYC_DR     10.    @376  ';'   /*235004*/
          @377  TEMP_CURR_AMT_DR     16.2   @394  ';'   /*235004*/
          @395  TEMP_CURR_CYC_CR     10.    @406  ';'   /*235004*/
          @407  TEMP_CURR_AMT_CR     16.2   @424  ';'   /*235004*/
          @425  TEMP_PREV_CYC_DR     10.    @442  ';'   /*235004*/
          @443  TEMP_PREV_AMT_DR     16.2   @460  ';'   /*235004*/
          @461  TEMP_PREV_CYC_CR     10.    @478  ';'   /*235004*/
          @479  TEMP_PREV_AMT_CR     16.2   @496  ';'   /*235004*/
          @497  ACCT_PST_IND         $1.    @511  ';'   /*235004*/
          @512  ACCT_PST_REASON      $20.   @533  ';'   /*235004*/
          @534  TOT_HOLD             $02.   @547  ';'   /*235004*/
          @548  SEQID_1              $02.   @562  ';'   /*235004*/
          @563  TEMP_AMT_1           14.2   @579  ';'   /*235004*/
          @580  DESC_1               $20.   @600  ';'   /*235004*/
          @601  SOURCE_1             $20.   @621  ';'   /*235004*/
          @622  SEQID_2              $02.   @636  ';'   /*235004*/
          @637  TEMP_AMT_2           14.2   @653  ';'   /*235004*/
          @654  DESC_2               $20.   @674  ';'   /*235004*/
          @675  SOURCE_2             $20.   @695  ';'   /*235004*/
          @696  SEQID_3              $02.   @700  ';'   /*235004*/
          @701  TEMP_AMT_3           14.2   @717  ';'   /*235004*/
          @718  DESC_3               $20.   @738  ';'   /*235004*/
          @739  SOURCE_3             $20.   @759  ';'   /*235004*/
           ;
  RUN;
