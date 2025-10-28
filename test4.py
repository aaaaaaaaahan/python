INPUT @001  BANKNO             $3.
      @004  APPLCODE           $5.
      @009  CUSTNO             $20.       /* was $11., changed to match POSITION */
      @029  PHONETYPE          $15.
      @044  PHONEPAC           PD8.
      @052  PHONEPREV          PD8.
      @060  INDORG             $1.
      @061  FIRSTDATE          $10.       /* or use DATE10. if applicable */
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
      @118  UPDOPER            $8.
      @126  TRXAPPLCODE        $5.        /* added */
      @131  TRXAPPLNO          $20.       /* added */
      @151  NEWPHONE           PD8.;      /* added */
