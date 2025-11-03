# ======================================================
# Combine all match results with aligned columns
# (Ensure all flags exist before UNION)
# ======================================================
con.execute("""
    CREATE TABLE ALLMATCH AS
    SELECT * FROM (
        SELECT HCMNAME, OLDID, IC, DOB, BASE, DESIGNATION, COMPCODE, STAFFID,
               MATCH_IND, REASON,
               'Y' AS M_NAME, NULL AS M_NIC, NULL AS M_NID, NULL AS M_IC, NULL AS M_ID, NULL AS M_DOB
        FROM MRGNAME
        UNION ALL
        SELECT HCMNAME, OLDID, IC, DOB, BASE, DESIGNATION, COMPCODE, STAFFID,
               MATCH_IND, REASON,
               NULL AS M_NAME, NULL AS M_NIC, NULL AS M_NID, NULL AS M_IC, 'Y' AS M_ID, NULL AS M_DOB
        FROM MRGID
        UNION ALL
        SELECT HCMNAME, OLDID, IC, DOB, BASE, DESIGNATION, COMPCODE, STAFFID,
               MATCH_IND, REASON,
               NULL AS M_NAME, NULL AS M_NIC, NULL AS M_NID, 'Y' AS M_IC, NULL AS M_ID, NULL AS M_DOB
        FROM MRGIC
        UNION ALL
        SELECT HCMNAME, OLDID, IC, DOB, BASE, DESIGNATION, COMPCODE, STAFFID,
               MATCH_IND, REASON,
               NULL AS M_NAME, NULL AS M_NIC, NULL AS M_NID, NULL AS M_IC, NULL AS M_ID, 'Y' AS M_DOB
        FROM MRGNDOB
        UNION ALL
        SELECT HCMNAME, OLDID, IC, DOB, BASE, DESIGNATION, COMPCODE, STAFFID,
               MATCH_IND, REASON,
               NULL AS M_NAME, NULL AS M_NIC, 'Y' AS M_NID, NULL AS M_IC, NULL AS M_ID, NULL AS M_DOB
        FROM MRGNID
        UNION ALL
        SELECT HCMNAME, OLDID, IC, DOB, BASE, DESIGNATION, COMPCODE, STAFFID,
               MATCH_IND, REASON,
               NULL AS M_NAME, 'Y' AS M_NIC, NULL AS M_NID, NULL AS M_IC, NULL AS M_ID, NULL AS M_DOB
        FROM MRGNIC
    )
""")

# ======================================================
# Final Output (COALESCE all flags)
# ======================================================
con.execute("""
    CREATE TABLE OUTPUT AS
    SELECT
        HCMNAME,
        OLDID,
        IC,
        MATCH_IND,
        DOB,
        BASE,
        DESIGNATION,
        COALESCE(REASON, '') AS REASON,
        COALESCE(M_NAME, 'N') AS M_NAME,
        COALESCE(M_NIC, 'N') AS M_NIC,
        COALESCE(M_NID, 'N') AS M_NID,
        COALESCE(M_IC, 'N') AS M_IC,
        COALESCE(M_ID, 'N') AS M_ID,
        COALESCE(M_DOB, 'N') AS M_DOB,
        COMPCODE,
        STAFFID,
        'AML/CFT' AS DEPT,
        'MS NG MEE WUN 03-21767651; MS WONG LAI SAN 03-21763005' AS CONTACT
    FROM ALLMATCH
""")
