# 1️⃣ Latest and latest previous
get_gdg_generations("CIS.SDB.MATCH.FULL", generations=2)

# 2️⃣ Latest + latest previous + 2nd previous
get_gdg_generations("CIS.SDB.MATCH.FULL", generations=3)

# 3️⃣ All generations (unlimited)
get_gdg_generations("CIS.SDB.MATCH.FULL", generations='all')

# 4️⃣ Only previous + 2nd previous (skip latest)
get_gdg_generations("CIS.SDB.MATCH.FULL", generations=[-1, -2])

# 5️⃣ Latest + 8 previous (total 9)
get_gdg_generations("CIS.SDB.MATCH.FULL", generations=9)

# 6️⃣ Custom pattern
get_gdg_generations("CIS.SDB.MATCH.FULL", generations=[0, -3, -5])
