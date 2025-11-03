          lrecl = 500

INPUT @01 RAWDATA        $EBCDIC500.  
      ;


          lrecl = 177

INPUT @01  DJ_NAME             $EBCDIC40.
      @41  DJ_ID_NO            $EBCDIC40.
      @81  DJ_PERSON_ID        $EBCDIC10.
      @91  DJ_IND_ORG          $EBCDIC01.
      @92  DJ_DESC1            $EBCDIC04.
      @96  DJ_DOB_DOR          $EBCDIC10.
      @106 DJ_NAME_TYPE        $EBCDIC10.
      @116 DJ_ID_TYPE          $EBCDIC50.
      @166 DJ_DATE_TYPE        $EBCDIC04.
      @170 DJ_GENDER           $EBCDIC01.
      @171 DJ_SANCTION_INDC    $EBCDIC01.
      @172 DJ_OCCUP_INDC       $EBCDIC01.
      @173 DJ_RLENSHIP_INDC    $EBCDIC01.
      @174 DJ_OTHER_LIST_INDC  $EBCDIC01.
      @175 DJ_ACTIVE_STATUS    $EBCDIC01.
      @176 DJ_CITIZENSHIP      $EBCDIC02.
      ;
