=IF(COUNTIF('[PartData.xlsx]Sheet1'!$A:$A,B2)>0,"YES",C2)
=IF(COUNTIF('[PartData.xlsx]Sheet1'!$A:$A, TRIM(B2))>0,"YES",C2)
