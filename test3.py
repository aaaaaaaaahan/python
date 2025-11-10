=IF(C2="YES","YES",IF(COUNTIF('[PartData.xlsx]Sheet1'!$A:$A,B2)>0,"YES",C2))
